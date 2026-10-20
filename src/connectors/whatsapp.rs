use crate::{
    config::{boolean, string},
    runtime::{Bridge, Event, Sink},
};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use whatsapp_rust::bot::{Bot, MessageContext};
use whatsapp_rust_sqlite_storage::SqliteStore;

pub fn write_pairing_qr(directory: &std::path::Path, code: &str) -> Result<()> {
    let image = qrcode::QrCode::new(code.as_bytes())?
        .render::<qrcode::render::svg::Color>()
        .min_dimensions(320, 320)
        .build();
    crate::store::write_private(&directory.join("pairing-qr.svg"), image.as_bytes())
}

pub struct WhatsAppSink(pub MessageContext);
#[async_trait]
impl Sink for WhatsAppSink {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(Self(self.0.clone())))
    }
    async fn upload(&self, name: &str, mime: &str, bytes: Vec<u8>) -> Result<()> {
        use whatsapp_rust::{download::MediaType, upload::UploadOptions};
        let kind = if mime.starts_with("image/") {
            MediaType::Image
        } else {
            MediaType::Document
        };
        let uploaded = self
            .0
            .client
            .upload(bytes, kind, UploadOptions::new())
            .await?;
        self.0
            .send_message(media_message(name, mime, uploaded))
            .await?;
        Ok(())
    }
    async fn emit(&self, event: Value) -> Result<()> {
        if matches!(
            event["type"].as_str(),
            Some("response" | "error" | "permission_denied" | "activity")
        ) && let Some(text) = event["text"]
            .as_str()
            .or(event["message"].as_str())
            .filter(|text| !text.is_empty())
        {
            self.0.reply(text).await?;
        }
        Ok(())
    }
}

pub fn media_message(
    name: &str,
    mime: &str,
    uploaded: whatsapp_rust::upload::UploadResponse,
) -> whatsapp_rust::waproto::whatsapp::Message {
    use whatsapp_rust::waproto::whatsapp as wa;
    if mime.starts_with("image/") {
        wa::Message {
            image_message: wa::message::ImageMessage {
                url: Some(uploaded.url),
                mimetype: Some(mime.into()),
                file_sha256: Some(uploaded.file_sha256.to_vec()),
                file_length: Some(uploaded.file_length),
                media_key: Some(uploaded.media_key.to_vec()),
                file_enc_sha256: Some(uploaded.file_enc_sha256.to_vec()),
                direct_path: Some(uploaded.direct_path),
                media_key_timestamp: Some(uploaded.media_key_timestamp),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    } else {
        wa::Message {
            document_message: wa::message::DocumentMessage {
                url: Some(uploaded.url),
                mimetype: Some(mime.into()),
                file_sha256: Some(uploaded.file_sha256.to_vec()),
                file_length: Some(uploaded.file_length),
                media_key: Some(uploaded.media_key.to_vec()),
                file_enc_sha256: Some(uploaded.file_enc_sha256.to_vec()),
                direct_path: Some(uploaded.direct_path),
                media_key_timestamp: Some(uploaded.media_key_timestamp),
                file_name: Some(name.into()),
                title: Some(name.into()),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }
}

pub async fn build(config: &Value, bridge: Arc<Bridge>) -> Result<Bot> {
    build_inner(
        config,
        bridge,
        whatsapp_rust::transport::TokioWebSocketTransportFactory::new(),
    )
    .await
}
pub async fn build_with_transport<Factory: whatsapp_rust::transport::TransportFactory + 'static>(
    config: &Value,
    bridge: Arc<Bridge>,
    transport: Factory,
) -> Result<Bot> {
    build_inner(config, bridge, transport).await
}
async fn build_inner<Factory: whatsapp_rust::transport::TransportFactory + 'static>(
    config: &Value,
    bridge: Arc<Bridge>,
    transport: Factory,
) -> Result<Bot> {
    let directory = std::path::Path::new(string(config, "authFolder"));
    std::fs::create_dir_all(directory)?;
    let database = directory.join("whatsapp.db");
    let pairing_directory = directory.to_path_buf();
    let store = SqliteStore::new(&database.to_string_lossy()).await?;
    let permits = Arc::new(tokio::sync::Semaphore::new(32));
    let respond_to_others = boolean(config, "respondToOthers");
    let builder = Bot::builder()
        .with_backend(store)
        .with_transport_factory(transport);
    let bot = builder
        .on_qr_code(move |code, _timeout| {
            let directory = pairing_directory.clone();
            async move {
                if write_pairing_qr(&directory, &code).is_ok() {
                    eprintln!("WhatsApp pairing QR is available in the configured authFolder.");
                } else {
                    eprintln!("Unable to write WhatsApp pairing QR.");
                }
            }
        })
        .on_message(move |context: MessageContext| {
            let bridge = bridge.clone();
            let permits = permits.clone();
            async move {
                let Ok(_permit) = permits.try_acquire_owned() else {
                    return;
                };
                let source = &context.info.source;
                let self_chat =
                    source.is_from_me && !source.is_group && source.chat.user == source.sender.user;
                if source.is_from_me && !self_chat || !source.is_from_me && !respond_to_others {
                    return;
                }
                let text = context.message.conversation.as_deref().or_else(|| {
                    context
                        .message
                        .extended_text_message
                        .as_option()
                        .and_then(|message| message.text.as_deref())
                });
                let Some(text) = text else {
                    return;
                };
                let event = Event {
                    platform: "whatsapp".into(),
                    room: source.chat.to_string(),
                    thread: String::new(),
                    id: context.info.id.clone(),
                    user: source.sender.user.to_string(),
                    text: text.into(),
                    direct: self_chat,
                    mentioned: text.trim().starts_with('/'),
                    bot: false,
                    images: vec![],
                    reply_to: None,
                };
                let sink = WhatsAppSink(context);
                let _ = bridge.handle(event, &sink).await;
            }
        })
        .build()
        .await?;
    Ok(bot)
}

pub async fn run(bot: Bot, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
    let client = bot.client();
    let running = bot.run();
    tokio::pin!(running);
    tokio::select! {_=shutdown.changed()=>client.disconnect().await,()=&mut running=>{}}
    Ok(())
}
