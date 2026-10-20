use async_trait::async_trait;
use soulsteads_messaging::{
    config, media,
    runtime::{Bridge, Event, Sink, session_dir},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::sync::Mutex;
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<Value>>,
    uploads: Mutex<Vec<(String, String, Vec<u8>)>>,
}
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.events.lock().await.push(event);
        Ok(())
    }
    async fn upload(&self, name: &str, mime: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.uploads
            .lock()
            .await
            .push((name.into(), mime.into(), bytes));
        Ok(())
    }
}
#[test]
fn media_reader_rejects_escape_symlink_and_oversize() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("session");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("inside.txt"), "fixture").unwrap();
    std::fs::write(directory.path().join("outside.txt"), "outside fixture").unwrap();
    assert_eq!(
        media::read(&root, "inside.txt", 1024).unwrap().bytes,
        b"fixture"
    );
    assert!(media::read(&root, "../outside.txt", 1024).is_err());
    assert!(media::read(&root, "inside.txt", 2).is_err());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(directory.path().join("outside.txt"), root.join("link.txt"))
            .unwrap();
        assert!(media::read(&root, "link.txt", 1024).is_err());
    }
}
#[tokio::test]
async fn marker_media_from_real_acp_uploads_once_and_cleans_response() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("sessions");
    let root = session_dir(&workspace, "web", "client");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("fixture.png");
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let bytes = bytes.into_inner();
    std::fs::write(&path, &bytes).unwrap();
    let configuration=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MEDIA_PATH":path}}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Bridge::new(configuration, BTreeMap::new(), workspace).unwrap();
    let recorder = Recorder::default();
    bridge
        .handle(Event::web("client", "media"), &recorder)
        .await
        .unwrap();
    let uploads = recorder.uploads.lock().await;
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].1, "image/png");
    assert_eq!(uploads[0].2, bytes);
    assert!(
        recorder
            .events
            .lock()
            .await
            .iter()
            .any(|event| event["type"] == "response" && event["text"] == "Here is the image.")
    );
    bridge.shutdown().await.unwrap();
}

#[test]
fn doclibrary_cache_is_an_owned_supported_root_not_an_arbitrary_file_escape() {
    let directory = tempfile::tempdir().unwrap();
    let session = directory.path().join("session");
    let cache = directory.path().join("doclibrary_cache");
    std::fs::create_dir(&session).unwrap();
    std::fs::create_dir(&cache).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(cache.join("image.png"), "fixture").unwrap();
    std::fs::write(directory.path().join("secret.txt"), "not media").unwrap();
    assert_eq!(
        media::read_supported(
            &session,
            &cache,
            cache.join("image.png").to_str().unwrap(),
            1024
        )
        .unwrap()
        .bytes,
        b"fixture"
    );
    assert!(
        media::read_supported(
            &session,
            &cache,
            directory.path().join("secret.txt").to_str().unwrap(),
            1024
        )
        .is_err()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(
            media::read_supported(
                &session,
                &cache,
                cache.join("image.png").to_str().unwrap(),
                1024
            )
            .is_err()
        );
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::os::unix::fs::symlink(directory.path().join("secret.txt"), cache.join("link.png"))
            .unwrap();
        assert!(
            media::read_supported(
                &session,
                &cache,
                cache.join("link.png").to_str().unwrap(),
                1024
            )
            .is_err()
        );
    }
}

#[test]
fn inline_decoder_rejects_private_directory_symlink_mime_and_size_boundaries() {
    use base64::Engine;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    let image = json!({"data":base64::engine::general_purpose::STANDARD.encode(png.into_inner()),"mimeType":"image/png"});
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(outside.path(), root.path().join(".bridge-images")).unwrap();
        assert!(media::save_inline(root.path(), &image).is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    }
    let root = tempfile::tempdir().unwrap();
    for content in [
        json!({"data":"not base64"}),
        json!({"data":""}),
        json!({"data":image["data"],"mimeType":"image/jpeg"}),
        json!({"data":"A".repeat(2*1024*1024+1)}),
    ] {
        assert!(media::save_inline(root.path(), &content).is_err());
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
