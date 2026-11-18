use std::{ffi::OsStr, path::PathBuf};

pub fn acp_fixture() -> PathBuf {
    // Cargo only exports CARGO_BIN_EXE_acp-fixture to integration tests.
    // Unit test runners live in <profile>/deps/ or, with Cargo's newer
    // layout, <profile>/build/<package>/<unit>/out/.
    let executable = std::env::current_exe().expect("locate Cargo unit test executable");
    let test_dir = executable.parent().expect("unit test executable directory");
    let profile = match test_dir.file_name().and_then(OsStr::to_str) {
        Some("deps") => test_dir.parent(),
        Some("out") => test_dir
            .ancestors()
            .nth(3)
            .filter(|dir| dir.file_name() == Some(OsStr::new("build")))
            .and_then(std::path::Path::parent),
        _ => None,
    }
    .expect("expected a Cargo unit test under deps/ or build/<package>/<unit>/out/");
    let fixture = profile.join(format!("acp-fixture{}", std::env::consts::EXE_SUFFIX));
    assert!(
        fixture.is_file(),
        "build the offline acp-fixture binary before library tests: {}",
        fixture.display()
    );
    fixture
}
