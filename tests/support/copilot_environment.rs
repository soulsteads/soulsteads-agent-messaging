use serde_json::{Value, json};
use std::path::Path;

pub fn environment(native_home: &Path, token: bool) -> Value {
    let mut environment = json!({"COPILOT_HOME": native_home});
    if token {
        environment["COPILOT_GITHUB_TOKEN"] = "{env:COPILOT_GITHUB_TOKEN}".into();
    }
    environment
}
