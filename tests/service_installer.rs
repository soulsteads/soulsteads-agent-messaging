use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

fn temporary_directory(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new().prefix(label).tempdir().unwrap()
}

fn messaging(command: &[&str], cwd: &Path, home: &Path, xdg_config_home: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .args(command)
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", xdg_config_home)
        .output()
        .unwrap()
}

fn output_text(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn unit_path(xdg_config_home: &Path) -> PathBuf {
    xdg_config_home
        .join("systemd")
        .join("user")
        .join("soulsteads-messaging.service")
}

#[test]
fn user_dry_run_previews_explicit_gateway_unit_without_writing() {
    let temporary = temporary_directory("service-preview-");
    let work = temporary.path().join("working directory");
    let home = temporary.path().join("home");
    let xdg_config_home = temporary.path().join("xdg-config");
    fs::create_dir_all(&work).unwrap();
    let output = messaging(
        &[
            "service",
            "install",
            "--user",
            "--dry-run",
            "--config",
            "bridge config.json",
        ],
        &work,
        &home,
        &xdg_config_home,
    );

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(!xdg_config_home.exists());
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(
        rendered.contains(&format!("WorkingDirectory={}\n", work.display())),
        "{rendered}"
    );
    assert!(rendered.contains("ExecStart=\""), "{rendered}");
    assert!(rendered.contains("--serve"), "{rendered}");
    assert!(rendered.contains("--config"), "{rendered}");
    assert!(rendered.contains("bridge config.json"), "{rendered}");
    assert!(!rendered.contains("EnvironmentFile="), "{rendered}");
    assert!(!rendered.contains("enable"), "{rendered}");
}

#[test]
fn user_install_creates_private_unit_once_and_refuses_conflicts() {
    let temporary = temporary_directory("service-user-install-");
    let work = temporary.path().join("work");
    let home = temporary.path().join("home");
    let xdg_config_home = temporary.path().join("xdg-config");
    fs::create_dir_all(&work).unwrap();
    let first = messaging(
        &["service", "install", "--user", "--config", "bridge.json"],
        &work,
        &home,
        &xdg_config_home,
    );

    assert!(first.status.success(), "{}", output_text(&first));
    let unit = unit_path(&xdg_config_home);
    let installed = fs::read_to_string(&unit).unwrap();
    assert!(installed.contains("WorkingDirectory="), "{installed}");
    assert!(installed.contains("ExecStart="), "{installed}");
    assert!(installed.contains("--serve"), "{installed}");
    assert!(!installed.contains("EnvironmentFile="), "{installed}");
    assert_eq!(fs::metadata(&unit).unwrap().permissions().mode() & 0o077, 0);

    let second = messaging(
        &["service", "install", "--user", "--config", "bridge.json"],
        &work,
        &home,
        &xdg_config_home,
    );
    assert!(!second.status.success());
    assert_eq!(fs::read_to_string(&unit).unwrap(), installed);
}

#[test]
fn system_preview_requires_an_explicit_non_root_identity_without_writing() {
    let temporary = temporary_directory("service-system-preview-");
    let work = temporary.path().join("work");
    let home = temporary.path().join("home");
    let xdg_config_home = temporary.path().join("xdg-config");
    fs::create_dir_all(&work).unwrap();

    let missing_identity = messaging(
        &[
            "service",
            "install",
            "--system",
            "--dry-run",
            "--config",
            "bridge.json",
        ],
        &work,
        &home,
        &xdg_config_home,
    );
    assert!(!missing_identity.status.success());
    assert!(!xdg_config_home.exists());

    let root_identity = messaging(
        &[
            "service",
            "install",
            "--system",
            "--run-as",
            "root",
            "--dry-run",
            "--config",
            "bridge.json",
        ],
        &work,
        &home,
        &xdg_config_home,
    );
    assert!(!root_identity.status.success());
    assert!(!xdg_config_home.exists());

    let preview = messaging(
        &[
            "service",
            "install",
            "--system",
            "--run-as",
            "service-user",
            "--dry-run",
            "--config",
            "bridge.json",
        ],
        &work,
        &home,
        &xdg_config_home,
    );
    assert!(preview.status.success(), "{}", output_text(&preview));
    let rendered = String::from_utf8(preview.stdout).unwrap();
    assert!(rendered.contains("User=service-user"), "{rendered}");
    assert!(!xdg_config_home.exists());
}
