//! Private loopback settings listener. Independent of the chat router.

mod disk;
mod document;

use crate::settings::{Error, Settings, console_scalar, error, parse_object, parse_value};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use disk::ConfigFile;
use document::Document;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json, value::RawValue};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};
use uuid::Uuid;

struct Console {
    file: ConfigFile,
    cwd: PathBuf,
    ambient: BTreeMap<String, String>,
    host: String,
    origin: String,
    authorization: String,
    revision_key: Uuid,
}

#[tokio::main(flavor = "current_thread")]
pub async fn serve(
    cwd: PathBuf,
    explicit: Option<PathBuf>,
    ambient: BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let result = serve_loop(cwd, explicit, ambient).await;
    result.map_err(|_| anyhow::anyhow!("settings_serve: Cannot start or run the local settings console. Check that the configuration parent is an existing trusted directory."))
}

async fn serve_loop(
    cwd: PathBuf,
    explicit: Option<PathBuf>,
    ambient: BTreeMap<String, String>,
) -> io::Result<()> {
    let file = ConfigFile::open(
        &cwd.join(explicit.unwrap_or_else(|| "soulsteads-messaging.json".into())),
    )?;
    let socket = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let host = socket.local_addr()?.to_string();
    let origin = format!("http://{host}");
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let state = Arc::new(Console {
        file,
        cwd,
        ambient,
        host,
        origin: origin.clone(),
        authorization: format!("Bearer {token}"),
        revision_key: Uuid::new_v4(),
    });
    let router = Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("settings_web/index.html")) }),
        )
        .route(
            "/console.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("settings_web/console.js"),
                )
            }),
        )
        .route(
            "/console.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("settings_web/console.css"),
                )
            }),
        )
        .route("/api/settings", get(inspect).post(update))
        .layer(DefaultBodyLimit::max(
            crate::config::SETTINGS_FILE_LIMIT as usize,
        ))
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state);

    {
        let mut terminal = io::stdout().lock();
        writeln!(terminal, "{origin}/#{token}")?;
        terminal.flush()?;
    }
    axum::serve(
        socket,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

fn single_header(headers: &HeaderMap, key: header::HeaderName) -> Option<&[u8]> {
    let mut values = headers.get_all(key).iter();
    let value = values.next()?;
    values.next().is_none().then(|| value.as_bytes())
}

async fn guard(State(state): State<Arc<Console>>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let peer = request.extensions().get::<ConnectInfo<SocketAddr>>();
    let api = request.uri().path() == "/api/settings";
    let denied = !peer.is_some_and(|peer| peer.0.ip().is_loopback())
        || single_header(headers, header::HOST) != Some(state.host.as_bytes())
        || request.uri().authority().is_some()
        || request.uri().query().is_some()
        || headers.keys().any(|name| {
            matches!(name.as_str(), "forwarded" | "x-real-ip" | "via")
                || name.as_str().starts_with("x-forwarded-")
        })
        || api
            && (headers.contains_key(header::ORIGIN) || request.method() == Method::POST)
            && single_header(headers, header::ORIGIN) != Some(state.origin.as_bytes())
        || api
            && headers
                .get("sec-fetch-site")
                .is_some_and(|site| site == "cross-site" || site == "same-site");
    let mut response = if denied {
        failure(StatusCode::FORBIDDEN, "request_denied")
    } else if api
        && !single_header(headers, header::AUTHORIZATION)
            .is_some_and(|value| token_matches(value, state.authorization.as_bytes()))
    {
        failure(StatusCode::UNAUTHORIZED, "authorization_required")
    } else {
        next.run(request).await
    };
    response.headers_mut().extend([
        (header::CACHE_CONTROL, header::HeaderValue::from_static("no-store")),
        (header::REFERRER_POLICY, header::HeaderValue::from_static("no-referrer")),
        (header::X_CONTENT_TYPE_OPTIONS, header::HeaderValue::from_static("nosniff")),
        (header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static(
            "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
        )),
    ]);
    response
}

fn token_matches(supplied: &[u8], expected: &[u8]) -> bool {
    supplied.len() == expected.len()
        && supplied
            .iter()
            .zip(expected)
            .fold(0, |difference, (a, b)| difference | (a ^ b))
            == 0
}

fn failure(status: StatusCode, category: &'static str) -> Response {
    (status, Json(json!({ "error": category }))).into_response()
}

fn settings_failure(error: Error) -> Response {
    failure(StatusCode::UNPROCESSABLE_ENTITY, error.category)
}

async fn inspect(State(state): State<Arc<Console>>) -> Response {
    blocking(move || {
        let Ok(snapshot) = state.file.read(&state.revision_key) else {
            return failure(StatusCode::UNPROCESSABLE_ENTITY, "config_read");
        };
        let document = match snapshot.document() {
            Ok(document) => document,
            Err(error) => return settings_failure(error),
        };
        let (entries, validation) = match state.settings(document) {
            Ok(settings) => {
                let defaults = crate::config::defaults();
                let entries = settings.console_entries().into_iter().map(|entry| {
                    let value = settings.console_scalar(&defaults, &entry.path);
                    let default = defaults.pointer(&entry.path)
                        .and_then(|value| console_scalar(&defaults, &entry.path, value));
                    json!({ "path": entry.path, "source": entry.source, "value": value, "default": default })
                }).collect::<Vec<_>>();
                (entries, settings.validate().err().map(|error| error.category))
            }
            Err(error) => (Vec::new(), Some(error.category)),
        };
        Json(json!({ "revision": snapshot.revision, "settings": entries, "validation": validation })).into_response()
    }).await
}

impl Console {
    fn settings(&self, document: Value) -> Result<Settings, Error> {
        Settings::from_document(&self.cwd, document, self.ambient.clone())
    }

    fn write(&self, change: Change) -> Response {
        let _lock = match self.file.lock() {
            Ok(Some(lock)) => lock,
            Ok(None) => return failure(StatusCode::CONFLICT, "concurrent_update"),
            Err(_) => return failure(StatusCode::UNPROCESSABLE_ENTITY, "config_read"),
        };
        let snapshot = match self.file.read(&self.revision_key) {
            Ok(snapshot) if snapshot.revision == change.revision => snapshot,
            Ok(_) => return failure(StatusCode::CONFLICT, "stale_revision"),
            Err(_) => return failure(StatusCode::UNPROCESSABLE_ENTITY, "config_read"),
        };
        let input = match snapshot
            .input()
            .and_then(|input| parse_object(input).map(|_| input))
        {
            Ok(input) => input,
            Err(error) => return settings_failure(error),
        };
        let Ok(mut document) = Document::parse(input) else {
            return failure(StatusCode::UNPROCESSABLE_ENTITY, "invalid_json");
        };
        let replacement = match (change.op, change.value.as_deref(), change.json.as_deref()) {
            (Edit::Set, Some(value), None) => Some(value.get()),
            (Edit::SetJson, None, Some(json)) => Some(json),
            (Edit::Reset, None, None) => None,
            _ => return failure(StatusCode::BAD_REQUEST, "invalid_request"),
        };
        let replacement = match replacement
            .map(|input| {
                parse_value(input)?;
                Document::parse(input).map_err(|_| error("invalid_json", "Use a valid JSON value."))
            })
            .transpose()
        {
            Ok(value) => value,
            Err(error) => return settings_failure(error),
        };
        if !apply(&mut document, &change.path, replacement) {
            return failure(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_or_empty_operation",
            );
        }
        let Ok(proposed) = serde_json::to_string_pretty(&document) else {
            return failure(StatusCode::UNPROCESSABLE_ENTITY, "config_write");
        };
        if let Err(error) = parse_object(&proposed)
            .and_then(|value| {
                if (matches!(change.path.as_str(), "/web" | "/web/port")
                    || change.path.starts_with("/web/port/"))
                    && let Some(port) = value.pointer("/web/port")
                {
                    crate::config::validate_file_web_port(port).map_err(|_| {
                        error("web_port", "Set web.port to an integer from 0 to 65535.")
                    })?;
                }
                self.settings(value)
            })
            .and_then(|settings| settings.validate())
        {
            return settings_failure(error);
        }
        match self
            .file
            .publish(&snapshot, &self.revision_key, proposed.as_bytes())
        {
            Ok(true) => Json(json!({ "saved": true })).into_response(),
            Ok(false) => failure(StatusCode::CONFLICT, "stale_revision"),
            Err(disk::PublishError::Before) => {
                failure(StatusCode::UNPROCESSABLE_ENTITY, "config_write")
            }
            Err(disk::PublishError::After) => {
                failure(StatusCode::INTERNAL_SERVER_ERROR, "durability_uncertain")
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    revision: String,
    op: Edit,
    path: String,
    #[serde(default, deserialize_with = "present_value")]
    value: Option<Box<RawValue>>,
    json: Option<String>,
}

fn present_value<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Box<RawValue>>, D::Error> {
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Edit {
    Set,
    SetJson,
    Reset,
}

async fn update(
    State(state): State<Arc<Console>>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    let Ok(body) = body else {
        return failure(StatusCode::PAYLOAD_TOO_LARGE, "request_body");
    };
    let Ok(input) = std::str::from_utf8(&body) else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(change) = parse_object(input).and_then(|_| {
        serde_json::from_str(input)
            .map_err(|_| error("invalid_request", "Send a single settings operation."))
    }) else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    };
    blocking(move || state.write(change)).await
}

async fn blocking(operation: impl FnOnce() -> Response + Send + 'static) -> Response {
    tokio::task::spawn_blocking(operation)
        .await
        .unwrap_or_else(|_| failure(StatusCode::INTERNAL_SERVER_ERROR, "console_error"))
}

fn decode(segment: &str) -> Option<String> {
    let mut parts = segment.split('~');
    let mut decoded = parts.next()?.to_owned();
    for part in parts {
        let (escape, rest) = part.split_at_checked(1)?;
        decoded.push(match escape {
            "0" => '~',
            "1" => '/',
            _ => return None,
        });
        decoded.push_str(rest);
    }
    Some(decoded)
}

fn index(segment: &str) -> Option<usize> {
    if segment.is_empty()
        || (segment.starts_with('0') && segment.len() > 1)
        || !segment.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    segment.parse().ok()
}

fn apply<'a>(document: &mut Document<'a>, path: &str, replacement: Option<Document<'a>>) -> bool {
    let Some(mut segments) = path.strip_prefix('/').and_then(|pointer| {
        pointer
            .split('/')
            .take(129)
            .map(decode)
            .collect::<Option<Vec<_>>>()
    }) else {
        return false;
    };
    if segments.len() > 127 {
        return false;
    }
    let leaf = segments.pop().expect("split yields at least one segment");
    let mut parent = document;
    let defaults = crate::config::defaults();
    let mut expected = Some(&defaults);
    let section = segments.is_empty();
    for segment in segments {
        expected = expected.and_then(|value| value.get(&segment));
        parent = match parent {
            Document::Object(fields) => match fields.entry(segment) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry)
                    if replacement.is_some() && expected.is_none_or(Value::is_object) =>
                {
                    entry.insert(Document::Object(BTreeMap::new()))
                }
                _ => return false,
            },
            Document::Array(items) => {
                match index(&segment).and_then(|index| items.get_mut(index)) {
                    Some(value) => value,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    if let Some((default, value)) = expected
        .and_then(|value| value.get(&leaf))
        .zip(replacement.as_ref())
        && ((default.is_array() && !matches!(value, Document::Array(_)))
            || (default.is_object()
                && !matches!(value, Document::Object(_))
                && !(section
                    && matches!(leaf.as_str(), "toolMessages" | "web")
                    && matches!(value, Document::Array(_)))))
    {
        return false;
    }
    match (parent, replacement) {
        (Document::Object(fields), Some(value)) if fields.get(&leaf) != Some(&value) => {
            fields.insert(leaf, value);
            true
        }
        (Document::Object(fields), None) => fields.remove(&leaf).is_some(),
        (Document::Array(items), Some(value)) if leaf == "-" => {
            items.push(value);
            true
        }
        (Document::Array(items), replacement) => {
            let Some(index) = index(&leaf).filter(|&index| index < items.len()) else {
                return false;
            };
            match replacement {
                Some(value) if items[index] == value => return false,
                Some(value) => items[index] = value,
                None => {
                    items.remove(index);
                }
            }
            true
        }
        _ => false,
    }
}
