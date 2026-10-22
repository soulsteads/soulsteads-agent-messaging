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
        if let Some(text) = reply_text(&event) {
            self.0.reply(text).await?;
        }
        Ok(())
    }
}

fn reply_text(event: &Value) -> Option<&str> {
    if !matches!(
        event["type"].as_str(),
        Some("response" | "error" | "permission_denied" | "activity")
    ) {
        return None;
    }
    event["text"]
        .as_str()
        .or(event["message"].as_str())
        .filter(|text| !text.is_empty())
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

/// Normalizes an SDK message and its parsed metadata into the bridge event.
///
/// This deliberately accepts the SDK's message and info types rather than a
/// `MessageContext`: creating a context requires an SDK client, which the
/// normalization itself does not use.
pub(crate) fn event_from_message(
    message: &whatsapp_rust::waproto::whatsapp::Message,
    info: &whatsapp_rust::wacore::types::message::MessageInfo,
    respond_to_others: bool,
) -> Option<Event> {
    let source = &info.source;
    let self_chat = source.is_from_me && !source.is_group && source.chat.user == source.sender.user;
    if source.is_from_me && !self_chat || !source.is_from_me && !respond_to_others {
        return None;
    }
    let text = message.conversation.as_deref().or_else(|| {
        message
            .extended_text_message
            .as_option()
            .and_then(|message| message.text.as_deref())
    })?;

    Some(Event {
        platform: "whatsapp".into(),
        room: source.chat.to_string(),
        thread: String::new(),
        id: info.id.clone(),
        user: source.sender.user.to_string(),
        text: text.into(),
        direct: self_chat,
        mentioned: text.trim().starts_with('/'),
        bot: false,
        images: vec![],
        reply_to: None,
    })
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
                let Some(event) =
                    event_from_message(&context.message, &context.info, respond_to_others)
                else {
                    return;
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

#[cfg(test)]
mod tests {
    use super::{event_from_message, reply_text};
    use serde_json::json;
    use whatsapp_rust::{
        wacore::types::message::{MessageInfo, MessageSource},
        waproto::whatsapp as wa,
    };

    fn info(is_from_me: bool, is_group: bool, chat: &str, sender: &str) -> MessageInfo {
        MessageInfo {
            source: MessageSource {
                chat: chat.parse().expect("valid SDK chat JID"),
                sender: sender.parse().expect("valid SDK sender JID"),
                is_from_me,
                is_group,
                ..Default::default()
            },
            id: "message-id".into(),
            ..Default::default()
        }
    }

    fn conversation(text: &str) -> wa::Message {
        wa::Message {
            conversation: Some(text.into()),
            ..Default::default()
        }
    }

    #[test]
    fn reply_text_maps_supported_outbound_event_types() {
        for kind in ["response", "error", "permission_denied", "activity"] {
            let event = json!({"type": kind, "text": "reply"});

            assert_eq!(reply_text(&event), Some("reply"), "type={kind}");
        }
    }

    #[test]
    fn reply_text_ignores_non_reply_outbound_event_types() {
        for kind in ["chunk", "tool", "reasoning", "done"] {
            let event = json!({"type": kind, "text": "not a reply"});

            assert_eq!(reply_text(&event), None, "type={kind}");
        }
    }

    #[test]
    fn reply_text_uses_message_only_when_text_is_not_a_string() {
        assert_eq!(
            reply_text(&json!({"type": "response", "message": "fallback"})),
            Some("fallback")
        );
        assert_eq!(
            reply_text(&json!({"type": "response", "text": "preferred", "message": "fallback"})),
            Some("preferred")
        );
        assert_eq!(
            reply_text(&json!({"type": "response", "text": "", "message": "fallback"})),
            None,
            "an empty text string remains selected and is filtered rather than falling back"
        );
    }

    #[test]
    fn normalizes_incoming_conversation_text_when_others_are_enabled() {
        let event = event_from_message(
            &conversation("hello"),
            &info(
                false,
                false,
                "15550000001@s.whatsapp.net",
                "15550000001@s.whatsapp.net",
            ),
            true,
        )
        .expect("allowed incoming text maps to an event");

        assert_eq!(event.platform, "whatsapp");
        assert_eq!(event.room, "15550000001@s.whatsapp.net");
        assert_eq!(event.thread, "");
        assert_eq!(event.id, "message-id");
        assert_eq!(event.user, "15550000001");
        assert_eq!(event.text, "hello");
        assert!(!event.direct);
        assert!(!event.mentioned);
        assert!(!event.bot);
        assert!(event.images.is_empty());
        assert_eq!(event.reply_to, None);
    }

    #[test]
    fn normalizes_extended_text_when_conversation_is_absent() {
        let message = wa::Message {
            extended_text_message: wa::message::ExtendedTextMessage {
                text: Some("expanded".into()),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };

        let event = event_from_message(
            &message,
            &info(
                false,
                false,
                "15550000001@s.whatsapp.net",
                "15550000001@s.whatsapp.net",
            ),
            true,
        )
        .expect("extended text maps to an event");

        assert_eq!(event.text, "expanded");
    }

    #[test]
    fn marks_slash_command_text_as_mentioned() {
        let event = event_from_message(
            &conversation("  /help"),
            &info(
                false,
                false,
                "15550000001@s.whatsapp.net",
                "15550000001@s.whatsapp.net",
            ),
            true,
        )
        .expect("allowed command maps to an event");

        assert!(event.mentioned);
    }

    #[test]
    fn maps_outgoing_self_chat_as_direct() {
        let event = event_from_message(
            &conversation("note to self"),
            &info(
                true,
                false,
                "15550000001@s.whatsapp.net",
                "15550000001@s.whatsapp.net",
            ),
            false,
        )
        .expect("self chat is accepted regardless of respondToOthers");

        assert!(event.direct);
        assert_eq!(event.text, "note to self");
    }

    #[test]
    fn rejects_incoming_text_when_others_are_disabled() {
        assert!(
            event_from_message(
                &conversation("hello"),
                &info(
                    false,
                    false,
                    "15550000001@s.whatsapp.net",
                    "15550000001@s.whatsapp.net"
                ),
                false,
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_outgoing_other_chat() {
        assert!(
            event_from_message(
                &conversation("outbound"),
                &info(
                    true,
                    false,
                    "15550000002@s.whatsapp.net",
                    "15550000001@s.whatsapp.net"
                ),
                true,
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_outgoing_group_even_when_sender_matches_chat_user() {
        assert!(
            event_from_message(
                &conversation("outbound group"),
                &info(
                    true,
                    true,
                    "120363021033254949@g.us",
                    "120363021033254949@g.us"
                ),
                true,
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_non_text_messages() {
        assert!(
            event_from_message(
                &wa::Message::default(),
                &info(
                    false,
                    false,
                    "15550000001@s.whatsapp.net",
                    "15550000001@s.whatsapp.net"
                ),
                true,
            )
            .is_none()
        );
    }

    #[test]
    fn preserves_empty_text_as_an_event() {
        let event = event_from_message(
            &conversation(""),
            &info(
                false,
                false,
                "15550000001@s.whatsapp.net",
                "15550000001@s.whatsapp.net",
            ),
            true,
        )
        .expect("existing mapping accepts empty conversation text");

        assert_eq!(event.text, "");
    }
}
