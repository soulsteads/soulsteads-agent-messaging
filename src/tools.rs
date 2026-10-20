use crate::config::string;
use serde_json::{Value, json};

pub struct OutputPresenter {
    options: Value,
    platform: String,
    seen: std::collections::HashMap<String, usize>,
    sent: std::collections::HashSet<String>,
}
impl OutputPresenter {
    pub fn new(options: Value, platform: &str) -> Self {
        Self {
            options,
            platform: platform.into(),
            seen: Default::default(),
            sent: Default::default(),
        }
    }
    pub fn update(&mut self, update: &Value) -> Option<Value> {
        if self.platform == "cli" || update["sessionUpdate"] != "tool_call_update" {
            return None;
        }
        let tool = update["title"]
            .as_str()
            .filter(|name| !name.is_empty())
            .or_else(|| update["name"].as_str().filter(|name| !name.is_empty()))
            .unwrap_or("unknown");
        let identity = update["toolCallId"]
            .as_str()
            .filter(|id| !id.is_empty())
            .unwrap_or(tool);
        let status = update["status"].as_str().unwrap_or("");
        let (kind, text) = match status {
            "in_progress" => {
                let output = update["rawOutput"]["output"].as_str()?;
                let length = output.encode_utf16().count();
                let seen = self.seen.get(identity).copied().unwrap_or(0);
                if length <= seen || (self.seen.len() >= 1024 && !self.seen.contains_key(identity))
                {
                    return None;
                }
                self.seen.insert(identity.into(), length);
                let units: Vec<_> = output.encode_utf16().skip(seen).collect();
                ("tool_output", String::from_utf16_lossy(&units))
            }
            "completed" | "failed" => {
                self.seen.remove(identity);
                let text = if status == "failed" {
                    let error = update["content"]
                        .as_array()
                        .and_then(|items| {
                            items.iter().rev().find_map(|item| {
                                (item["content"]["type"] == "text")
                                    .then(|| item["content"]["text"].as_str())
                                    .flatten()
                            })
                        })
                        .or_else(|| update["rawOutput"]["error"].as_str())
                        .unwrap_or("Tool execution failed");
                    format!("[Error] {error}")
                } else if let Some(items) = update["content"].as_array() {
                    items
                        .iter()
                        .filter(|item| item["content"]["type"] == "text")
                        .filter_map(|item| item["content"]["text"].as_str())
                        .collect::<String>()
                } else {
                    update["rawOutput"]["output"]
                        .as_str()
                        .unwrap_or("")
                        .to_owned()
                };
                let units: Vec<_> = text.encode_utf16().take(2001).collect();
                let text = if units.len() > 2000 {
                    format!(
                        "{}\n... (truncated)",
                        String::from_utf16_lossy(&units[..2000])
                    )
                } else {
                    text
                };
                ("tool_result", text)
            }
            _ => return None,
        };
        if !self.options["showOutputFor"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .any(|selector| {
                let selector = selector.trim();
                selector == "*" || (!selector.is_empty() && tool.contains(selector))
            })
        {
            return None;
        }
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        if self.platform != "web" {
            let key = String::from_utf16_lossy(&text.encode_utf16().take(100).collect::<Vec<_>>());
            if self.sent.len() >= 1024 || !self.sent.insert(key) {
                return None;
            }
            Some(json!({"type":"response","text":text}))
        } else {
            Some(json!({"type":kind,"tool":tool,"text":text}))
        }
    }
}

/// Per-turn presentation. The optional Matrix split uses one terminal presenter
/// per call so an aggregate trace never moves unrelated tools into its thread.
pub struct TurnPresenter {
    calls: Presenter,
    output: OutputPresenter,
    options: Value,
    policy: Option<Value>,
    terminals: std::collections::HashMap<String, (String, Presenter)>,
}
impl TurnPresenter {
    pub fn new(options: Value, platform: &str, policy: Value) -> Self {
        let enabled =
            platform == "matrix" && matches!(policy["mode"].as_str(), Some("auto" | "always"));
        let mut calls = Presenter::new(options.clone());
        calls.matrix_file_paths = (platform == "matrix").then_some(false);
        Self {
            calls,
            output: OutputPresenter::new(options.clone(), platform),
            options,
            policy: enabled.then_some(policy),
            terminals: Default::default(),
        }
    }
    /// Matrix-only display preference; must be configured before the first update.
    pub fn with_matrix_full_paths(mut self, enabled: bool) -> Self {
        if self.calls.matrix_file_paths.is_some() {
            self.calls.matrix_file_paths = Some(enabled);
        }
        self
    }
    pub fn update(&mut self, update: &Value) -> Vec<Value> {
        let Some(policy) = self.policy.clone() else {
            let mut events = self.calls.update(update);
            events.extend(self.output.update(update));
            return events;
        };
        let call = update["toolCallId"].as_str().filter(|id| !id.is_empty());
        let name = update["title"]
            .as_str()
            .filter(|s| !s.is_empty())
            .or_else(|| update["name"].as_str())
            .unwrap_or("");
        let arguments = update["rawInput"]
            .as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .unwrap_or_else(|| update["rawInput"].clone());
        let terminal = matches!(
            name.to_ascii_lowercase().as_str(),
            "bash" | "terminal" | "shell" | "exec"
        ) || (update["kind"] == "execute" && arguments["command"].is_string())
            || (self.calls.matrix_file_paths.is_some()
                && (update["kind"] == "execute"
                    || matches!(
                        name.to_ascii_lowercase().as_str(),
                        "execute" | "exec_command"
                    )));
        let mut events = vec![];
        if let Some(id) = call
            && terminal
            && !self.terminals.contains_key(id)
            && self.terminals.len() < 1024
        {
            let mut presenter = Presenter::new(self.options.clone());
            presenter.matrix_file_paths = self.calls.matrix_file_paths;
            self.terminals
                .insert(id.to_owned(), (name.to_owned(), presenter));
            events.push(json!({"type":"terminal_call","toolCallId":id}));
        }
        if let Some(id) = call
            && let Some((name, presenter)) = self.terminals.get_mut(id)
        {
            let mut update = update.clone();
            if update["title"].as_str().is_none_or(str::is_empty) && !name.is_empty() {
                update["title"] = name.clone().into();
            }
            for mut event in presenter.update(&update) {
                let key = event["activityId"].as_str().unwrap_or("activity");
                event["activityId"] = format!("terminal:{id}:{key}").into();
                event["terminalToolCallId"] = id.into();
                event["terminalThread"] = policy.clone();
                events.push(event);
            }
            if let Some(mut event) = self.output.update(&update) {
                if let Some(text) = event["text"].as_str() {
                    let units: Vec<_> = text.encode_utf16().take(2001).collect();
                    if units.len() > 2000 {
                        event["text"] = format!(
                            "{}\n... (truncated)",
                            String::from_utf16_lossy(&units[..2000])
                        )
                        .into();
                    }
                }
                event["terminalToolCallId"] = id.into();
                event["terminalThread"] = policy;
                events.push(event);
            }
            if update["status"] == "failed" {
                events.push(json!({"type":"terminal_failure","terminalToolCallId":id}));
            }
        } else {
            events.extend(self.calls.update(update));
            events.extend(self.output.update(update));
        }
        events
    }
}

pub struct Presenter {
    options: Value,
    matrix_file_paths: Option<bool>,
    entries: Vec<Value>,
    pages: std::collections::HashMap<usize, String>,
    started: std::collections::HashSet<String>,
    last_event: String,
    fallback_sequence: usize,
    last_fallback: Option<String>,
}
impl Presenter {
    pub fn new(options: Value) -> Self {
        Self {
            options,
            matrix_file_paths: None,
            entries: vec![],
            pages: Default::default(),
            started: Default::default(),
            last_event: String::new(),
            fallback_sequence: 0,
            last_fallback: None,
        }
    }
    pub fn update(&mut self, update: &Value) -> Vec<Value> {
        let mode = if self.options["showCalls"] == false {
            "off"
        } else {
            self.options["mode"].as_str().unwrap_or("events")
        };
        if mode == "off" {
            return vec![];
        }
        let id = if let Some(id) = update["toolCallId"].as_str().filter(|id| !id.is_empty()) {
            id.to_owned()
        } else {
            let name = update["title"]
                .as_str()
                .filter(|name| !name.is_empty())
                .or_else(|| update["name"].as_str().filter(|name| !name.is_empty()));
            let id = if update["sessionUpdate"] != "tool_call" {
                name.filter(|name| self.entries.iter().any(|entry| entry["id"] == *name))
                    .map(str::to_owned)
                    .or_else(|| self.last_fallback.clone())
            } else {
                name.map(str::to_owned)
            };
            let id = id.unwrap_or_else(|| {
                loop {
                    let id = format!("tool:{}", self.fallback_sequence);
                    self.fallback_sequence += 1;
                    if !self.entries.iter().any(|entry| entry["id"] == id) {
                        break id;
                    }
                }
            });
            self.last_fallback = Some(id.clone());
            id
        };
        let position = self.entries.iter().position(|entry| entry["id"] == id);
        let index = if let Some(index) = position {
            index
        } else {
            if self.entries.len() >= 1024 {
                return vec![];
            }
            self.entries.push(json!({"id":id,"tool":update["title"].as_str().or(update["name"].as_str()).unwrap_or("unknown"),"description":"","status":"pending","details":{}}));
            self.entries.len() - 1
        };
        let entry = &mut self.entries[index];
        if entry["tool"] == "unknown"
            && let Some(name) = update["title"].as_str().or(update["name"].as_str())
        {
            entry["tool"] = name.into();
        }
        if self.matrix_file_paths.is_some() {
            if string(entry, "tool").eq_ignore_ascii_case("subagent") {
                entry["matrix_subagent"] = true.into();
            }
            if update["kind"] == "execute"
                || matches!(
                    string(entry, "tool").to_ascii_lowercase().as_str(),
                    "execute" | "executing" | "exec_command"
                )
            {
                entry["tool"] = "exec".into();
            }
            if let Some(kind) = update["kind"]
                .as_str()
                .filter(|k| matches!(*k, "read" | "edit"))
            {
                entry["file_kind"] = kind.into();
            }
            if !file_tool(string(entry, "tool"))
                && let Some(kind) = entry["file_kind"].as_str()
            {
                entry["tool"] = kind.to_owned().into();
            }
        }
        let mut arguments = if let Some(text) = update["rawInput"].as_str() {
            serde_json::from_str(text).unwrap_or_else(|_| json!({"raw":text}))
        } else {
            update["rawInput"].clone()
        };
        if self.matrix_file_paths.is_some()
            && entry["tool"] == "exec"
            && let Some(args) = arguments.as_object_mut()
            && !args.get("command").is_some_and(Value::is_string)
            && let Some(code) = args.get("code").and_then(Value::as_str)
        {
            args.insert("command".into(), code.to_owned().into());
            args.remove("code");
        }
        if self.matrix_file_paths.is_some()
            && file_tool(string(entry, "tool"))
            && let Some(args) = arguments.as_object_mut()
        {
            let patch_paths = matrix_patch_paths(string(entry, "tool"), args);
            for key in FILE_ARGUMENTS {
                if let Some(path) = args.get(key).and_then(Value::as_str) {
                    if self.matrix_file_paths == Some(false) {
                        args.insert(key.into(), filename(path).into());
                    }
                } else {
                    args.remove(key);
                }
            }
            if !patch_paths.is_empty() {
                let labels = patch_paths
                    .iter()
                    .map(|path| {
                        if self.matrix_file_paths == Some(false) {
                            filename(path)
                        } else {
                            path.clone()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                args.insert("filePath".into(), labels.into());
            }
        }
        if arguments.as_object().is_some_and(|args| !args.is_empty()) {
            entry["description"] = if self.matrix_file_paths.is_some()
                && file_tool(string(entry, "tool"))
                && self.options["showArguments"] != true
            {
                let files = arguments
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(key, _)| FILE_ARGUMENTS.contains(&key.as_str()))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<serde_json::Map<_, _>>();
                argument_description(&Value::Object(files)).into()
            } else {
                argument_description(&arguments).into()
            };
            if self.matrix_file_paths.is_some() && read_tool(string(entry, "tool")) {
                entry["read_range"] = requested_read_range(&arguments).into();
                if let Some(range) = entry["read_range"].as_str().filter(|s| !s.is_empty()) {
                    entry["description"] =
                        format!("{} ({range})", string(entry, "description")).into();
                }
            }
            entry["details"] = arguments;
        }
        entry["status"] = match update["status"].as_str() {
            Some("in_progress" | "running") => "running",
            Some("completed") => "completed",
            Some("failed") => "failed",
            _ => entry["status"].as_str().unwrap_or("pending"),
        }
        .into();
        if mode == "events" {
            if self.started.contains(&id)
                || (entry["description"] == ""
                    && !matches!(entry["status"].as_str(), Some("completed" | "failed")))
            {
                return vec![];
            }
            self.started.insert(id.clone());
            let message = if (self.options["showArguments"] == true
                || (self.matrix_file_paths.is_some() && file_tool(string(entry, "tool"))))
                && entry["description"] != ""
            {
                format!(
                    "{} [{}]",
                    string(entry, "description"),
                    string(entry, "tool")
                )
            } else {
                format!("[{}]", string(entry, "tool"))
            };
            if self.last_event == message {
                return vec![];
            }
            self.last_event = message.clone();
            return vec![json!({"type":"activity","activityId":id,"message":message})];
        }
        let maximum = self.options["maxTraceEntries"]
            .as_u64()
            .unwrap_or(20)
            .clamp(1, 1024) as usize;
        let page_count = if mode == "status" {
            1
        } else {
            self.entries.len().div_ceil(maximum)
        };
        let completed = self
            .entries
            .iter()
            .filter(|entry| matches!(entry["status"].as_str(), Some("completed" | "failed")))
            .count();
        let active = self
            .entries
            .iter()
            .rev()
            .find(|entry| matches!(entry["status"].as_str(), Some("pending" | "running")));
        let plural = if completed == 1 { "" } else { "s" };
        let mut output = vec![];
        for page in 0..page_count {
            let entries =
                &self.entries[page * maximum..((page + 1) * maximum).min(self.entries.len())];
            let message = if mode == "status" {
                match active {
                    Some(entry) => format!(
                        "Working...\nCurrent: {}\nCompleted: {completed} tool{plural}",
                        format_entry(
                            entry,
                            self.options["showArguments"] == true
                                || (self.matrix_file_paths.is_some()
                                    && file_tool(string(entry, "tool")))
                        )
                    ),
                    None => format!("Completed {completed} tool{plural}."),
                }
            } else if mode == "hermes" {
                let mut groups: Vec<(String, usize)> = vec![];
                for entry in entries {
                    let message = hermes(
                        entry,
                        self.options["showArguments"] == true
                            || (self.matrix_file_paths.is_some()
                                && file_tool(string(entry, "tool"))),
                    );
                    if let Some((previous, count)) = groups.last_mut()
                        && previous == &message
                        && !message.contains("```")
                    {
                        *count += 1;
                    } else {
                        groups.push((message, 1));
                    }
                }
                groups
                    .into_iter()
                    .map(|(text, count)| {
                        if count > 1 {
                            format!("{text} (×{count})")
                        } else {
                            text
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                let phase = if entries
                    .iter()
                    .any(|entry| matches!(entry["status"].as_str(), Some("pending" | "running")))
                {
                    "working"
                } else if page == page_count - 1 && active.is_none() {
                    "completed"
                } else {
                    "continued"
                };
                let part = if page_count > 1 {
                    format!("part {}/{page_count}, ", page + 1)
                } else {
                    String::new()
                };
                format!(
                    "Tool trace ({part}{phase})\n\n{}",
                    entries
                        .iter()
                        .map(|entry| format_entry(
                            entry,
                            self.options["showArguments"] == true
                                || (self.matrix_file_paths.is_some()
                                    && file_tool(string(entry, "tool")))
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            if self.pages.get(&page) != Some(&message) {
                self.pages.insert(page, message.clone());
                output.push(json!({"type":"activity","activityId":format!("trace:{page}"),"message":message}));
            }
        }
        output
    }
}
const FILE_ARGUMENTS: [&str; 4] = ["filePath", "file_path", "path", "target_file"];
/// Extract only file-operation headers, never changed lines or the patch body.
/// This is Matrix presentation data; provider inputs and other platforms are unchanged.
fn matrix_patch_paths(tool: &str, args: &serde_json::Map<String, Value>) -> Vec<String> {
    if !matches!(
        tool.to_ascii_lowercase().as_str(),
        "patch" | "apply_patch" | "edit"
    ) || FILE_ARGUMENTS.iter().any(|key| {
        args.get(*key)
            .and_then(Value::as_str)
            .is_some_and(|path| !path.trim().is_empty())
    }) {
        return vec![];
    }
    let Some(patch) = ["patch", "patchText", "patch_text"]
        .iter()
        .find_map(|key| args.get(*key).and_then(Value::as_str))
        .filter(|patch| patch.len() <= 1024 * 1024)
    else {
        return vec![];
    };
    let mut lines = patch.trim().lines();
    if lines.next() != Some("*** Begin Patch")
        || patch.trim().lines().last() != Some("*** End Patch")
    {
        return vec![];
    }
    let mut paths = vec![];
    let mut move_allowed = false;
    for line in lines {
        if line == "*** End Patch" {
            break;
        }
        let is_update = line.starts_with("*** Update File: ");
        let path = ["*** Update File: ", "*** Add File: ", "*** Delete File: "]
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix))
            .or_else(|| {
                (tool.eq_ignore_ascii_case("apply_patch") && move_allowed)
                    .then(|| line.strip_prefix("*** Move to: "))
                    .flatten()
            })
            .map(str::trim)
            .filter(|path| !path.is_empty());
        if let Some(path) = path
            && !paths.iter().any(|existing| existing == path)
        {
            if paths.len() == 32 {
                paths.push("…".into());
                break;
            }
            paths.push(path.to_owned());
        }
        move_allowed = is_update;
    }
    paths
}
fn read_tool(tool: &str) -> bool {
    matches!(
        tool.to_ascii_lowercase().as_str(),
        "read" | "read_file" | "readfile"
    )
}
fn requested_read_range(arguments: &Value) -> String {
    let start = match arguments.get("offset") {
        None if arguments.get("limit").is_some() => 1,
        Some(value) => match value.as_u64().filter(|n| *n > 0) {
            Some(start) => start,
            None => return String::new(),
        },
        None => return String::new(),
    };
    let count = match arguments.get("limit") {
        None => return format!("requested from line {start}"),
        Some(value) => match value.as_u64().filter(|n| *n > 0) {
            Some(count) => count,
            None => return String::new(),
        },
    };
    start
        .checked_add(count - 1)
        .map(|end| format!("requested lines {start}–{end}"))
        .unwrap_or_default()
}
fn file_tool(tool: &str) -> bool {
    matches!(
        tool.to_ascii_lowercase().as_str(),
        "read"
            | "read_file"
            | "readfile"
            | "edit"
            | "write"
            | "write_file"
            | "apply_patch"
            | "patch"
            | "multiedit"
    )
}
fn filename(path: &str) -> String {
    path.trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(path)
        .to_owned()
}
fn format_entry(entry: &Value, arguments: bool) -> String {
    let detail = if arguments && entry["description"] != "" {
        format!(
            "{} [{}]",
            string(entry, "description"),
            string(entry, "tool")
        )
    } else {
        format!("[{}]", string(entry, "tool"))
    };
    format!("[{}] {detail}", string(entry, "status"))
}
pub fn argument_description(arguments: &Value) -> String {
    arguments
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, value)| {
            !key.eq_ignore_ascii_case("description") && !value.is_null() && **value != ""
        })
        .take(3)
        .map(|(key, value)| {
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
            let units: Vec<_> = compact.encode_utf16().collect();
            let value = if units.len() <= 80 {
                compact
            } else if [
                "path",
                "filepath",
                "file_path",
                "cwd",
                "workdir",
                "directory",
                "dir",
                "folder",
                "url",
                "uri",
            ]
            .contains(&key.to_ascii_lowercase().as_str())
            {
                format!(
                    "...{}",
                    String::from_utf16_lossy(&units[units.len() - 77..])
                )
            } else {
                format!("{}...", String::from_utf16_lossy(&units[..77]))
            };
            format!("{key}={value}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}
pub fn compact(text: &str, path: bool) -> String {
    let text = regex::Regex::new(r"\s+")
        .unwrap()
        .replace_all(&text.replace('`', "'"), " ")
        .trim()
        .to_owned();
    let units: Vec<_> = text.encode_utf16().collect();
    if units.len() <= 180 {
        text
    } else if path {
        format!("…{}", String::from_utf16_lossy(&units[units.len() - 179..]))
    } else {
        format!("{}…", String::from_utf16_lossy(&units[..179]))
    }
}
pub fn hermes(entry: &Value, arguments: bool) -> String {
    let first = |keys: &[&str]| {
        keys.iter().find_map(|key| {
            if arguments {
                entry["details"][key]
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
            } else {
                None
            }
        })
    };
    let file = first(&FILE_ARGUMENTS);
    let tool = string(entry, "tool").to_lowercase();
    let mut result = match tool.as_str() {
        "bash" | "terminal" | "shell" | "exec" => format!(
            "💻 terminal{}",
            first(&["command", "cmd"])
                .map(|command| format!("\n```\n{}\n```", compact(command, false)))
                .unwrap_or_default()
        ),
        "read" | "read_file" | "readfile" => format!(
            "📖 Reading{}{}",
            file.map(|file| format!(" {}", compact(file, true)))
                .unwrap_or_default(),
            entry["read_range"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(|range| format!(" ({range})"))
                .unwrap_or_default()
        ),
        "edit" | "write" | "write_file" | "apply_patch" | "patch" | "multiedit" => format!(
            "🔧 Editing{}",
            file.map(|file| format!(" {}", compact(file, true)))
                .unwrap_or_default()
        ),
        "grep" | "glob" | "search_files" => format!(
            "🔎 Searching files{}",
            first(&["pattern", "query"])
                .map(|query| format!(" for {}", compact(query, false)))
                .unwrap_or_default()
        ),
        "websearch" | "web_search" | "webfetch" | "web_fetch" => format!(
            "🌐 Searching the web{}",
            first(&["query", "url"])
                .map(|query| format!(" — {}", compact(query, false)))
                .unwrap_or_default()
        ),
        "skill" | "skill_view" => format!(
            "📚 Loading skill{}",
            first(&["name", "id"])
                .map(|name| format!(" {}", compact(name, false)))
                .unwrap_or_default()
        ),
        "subagent" if entry["matrix_subagent"] == true => {
            let role = first(&["subagent_type", "agent"]).unwrap_or_default();
            let task = first(&["description"]).unwrap_or_default();
            let summary = match (role.is_empty(), task.is_empty()) {
                (false, false) => format!("{role} — {task}"),
                (false, true) => role.to_owned(),
                (true, false) => task.to_owned(),
                (true, true) => String::new(),
            };
            format!(
                "🤖 Subagent{}",
                if summary.is_empty() {
                    String::new()
                } else {
                    format!(" {}", compact(&summary, false))
                }
            )
        }
        "todowrite" | "todo" => "📝 Updating task list".into(),
        _ => format!("⚙️ {}", compact(string(entry, "tool"), false)),
    };
    if entry["status"] == "failed" {
        result.push_str("\n⚠️ failed");
    }
    result
}
pub fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
pub fn matrix_notice(text: &str, rich: bool) -> Value {
    if !rich {
        return json!({"msgtype":"m.notice","body":format!("> {text}")});
    }
    let fences = regex::Regex::new(r"```\n[\s\S]*?\n```").unwrap();
    let mut html = String::new();
    let mut previous = 0;
    for fence in fences.find_iter(text) {
        html.push_str(&escape(&text[previous..fence.start()]).replace('\n', "<br>"));
        html.push_str(&format!(
            "<pre><code>{}</code></pre>",
            escape(&text[fence.start() + 4..fence.end() - 4])
        ));
        previous = fence.end();
    }
    html.push_str(&escape(&text[previous..]).replace('\n', "<br>"));
    html = regex::Regex::new(r"(?:<br>)+<pre>")
        .unwrap()
        .replace_all(&html, "<pre>")
        .into_owned();
    html = regex::Regex::new(r"</pre>(?:<br>)+")
        .unwrap()
        .replace_all(&html, "</pre>")
        .into_owned();
    json!({"msgtype":"m.notice","body":text,"format":"org.matrix.custom.html","formatted_body":html})
}
pub fn call(activity: &Value, options: &Value, editable: bool) -> Option<String> {
    let mode = if options["showCalls"] == false {
        "off"
    } else {
        options["mode"].as_str().unwrap_or("events")
    };
    if activity["type"] != "tool_start" || mode == "off" || (editable && mode != "events") {
        return None;
    }
    let tool = activity["tool"].as_str().unwrap_or("unknown");
    let description = string(activity, "description").trim();
    Some(
        if options["showArguments"] == true && !description.is_empty() {
            format!("{description} [{tool}]")
        } else {
            format!("[{tool}]")
        },
    )
}
pub fn markdown(text: &str) -> String {
    use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, TagEnd};
    let parser = Parser::new_ext(text, Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TABLES).map(
        |event| match event {
            Event::Html(html) | Event::InlineHtml(html) => Event::Text(html),
            Event::Start(Tag::Link {
                link_type,
                dest_url,
                title,
                id,
            }) => {
                let lower = dest_url.to_ascii_lowercase();
                let safe = if lower.starts_with("javascript:")
                    || lower.starts_with("data:")
                    || lower.starts_with("vbscript:")
                {
                    CowStr::from("#")
                } else {
                    dest_url
                };
                Event::Start(Tag::Link {
                    link_type,
                    dest_url: safe,
                    title,
                    id,
                })
            }
            Event::Start(Tag::Image { .. }) => Event::Text(CowStr::from("[image]")),
            Event::End(TagEnd::Image) => Event::Text(CowStr::from("")),
            _ => event,
        },
    );
    let mut html = String::new();
    pulldown_cmark::html::push_html(&mut html, parser);
    html
}

#[cfg(test)]
mod terminal_presentation_tests {
    use super::*;
    #[test]
    fn terminal_string_arguments_and_streamed_output_are_routed_and_bounded() {
        let mut presenter = TurnPresenter::new(
            json!({"mode":"events","showCalls":true,"showOutputFor":["*"]}),
            "matrix",
            json!({"mode":"auto","threshold":3}),
        );
        let call=presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"opaque","title":"Run command","kind":"execute","rawInput":"{\"command\":\"printf synthetic\"}"}));
        assert!(
            call.iter().any(|e| e["type"] == "terminal_call"),
            "ACP string arguments must identify an executable command: {call:?}"
        );
        let output=presenter.update(&json!({"sessionUpdate":"tool_call_update","toolCallId":"opaque","status":"in_progress","rawOutput":{"output":"x".repeat(5000)}}));
        let event = output.iter().find(|e| e["type"] == "response").unwrap();
        assert_eq!(event["terminalToolCallId"], "opaque");
        assert!(event["text"].as_str().unwrap().encode_utf16().count() <= 2020);
    }
}
