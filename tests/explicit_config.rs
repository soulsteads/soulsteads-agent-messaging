use std::process::Command;

#[test]
fn explicit_invalid_config_fails_closed_without_disclosing_content() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("required.json");
    for content in [None, Some("{\"private_fixture_marker\":")] {
        if let Some(content) = content {
            std::fs::write(&path, content).unwrap();
        }
        let output = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .current_dir(root.path())
            .args(["--check-config", "--config"])
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "explicit invalid config was accepted"
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("private_fixture_marker"));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("loaded successfully"));
    }
    std::fs::write(&path, "{}").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .args(["--check-config", "--config"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success(), "valid explicit config refused");
}

#[test]
fn implicit_missing_config_retains_default_startup_contract() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(output.status.success());
}
