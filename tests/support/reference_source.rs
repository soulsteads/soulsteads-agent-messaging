//! Pinned, actual TypeScript source for the port's conformance tests.
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::OnceLock};

pub fn root() -> PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/reference/opencode-chat-bridge")
            .canonicalize()
            .expect("Initialize the reference with scripts/bootstrap-test-reference.sh");
        let manifest: serde_json::Value =
            serde_json::from_str(include_str!("../reference/typescript-manifest.json")).unwrap();
        for (name, expected) in manifest["files"].as_object().unwrap() {
            let bytes = std::fs::read(root.join(name)).unwrap_or_else(|_| {
                panic!("Reference {name} is missing; run scripts/bootstrap-test-reference.sh")
            });
            assert_eq!(
                format!("{:x}", Sha256::digest(bytes)),
                expected.as_str().unwrap(),
                "Pinned TypeScript reference changed: {name}"
            );
        }
        assert!(
            root.join("node_modules/@agentclientprotocol/sdk").is_dir(),
            "Install reference dependencies with scripts/bootstrap-test-reference.sh"
        );
        root
    })
    .clone()
}
