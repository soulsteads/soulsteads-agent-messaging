use anyhow::{Result, bail};
use async_trait::async_trait;
use soulsteads_messaging::{media, runtime::Sink};
use regex::Regex;
use serde_json::Value;
use std::{
    collections::HashSet,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::Mutex,
};

#[derive(Default)]
struct Response {
    text: String,
    streamed: bool,
}

#[cfg_attr(
    not(target_os = "linux"),
    expect(
        dead_code,
        reason = "Renderer execution requires Linux descriptor publication"
    )
)]
struct Renderer {
    path: PathBuf,
    #[cfg(unix)]
    cwd: PathBuf,
    #[cfg(unix)]
    bindings: Vec<(PathBuf, RendererIdentity)>,
}

#[cfg(unix)]
#[derive(PartialEq, Eq)]
struct RendererIdentity {
    device: u64,
    inode: u64,
    mode: u32,
    owner: u32,
    group: u32,
    version: Option<(u64, i64, i64, i64, i64)>,
}

pub(super) struct Console {
    response: Mutex<Response>,
    show_images: bool,
    cache: PathBuf,
    patterns: Vec<Regex>,
    renderer: Option<Renderer>,
}

impl Console {
    pub(super) fn new(show_images: bool) -> Self {
        let cache = std::env::temp_dir().join("doclibrary_cache");
        let cache_pattern = regex::escape(&cache.to_string_lossy());
        let patterns = [
            r"(?i)Image file: ([^\n]+\.png)".to_owned(),
            format!(r"(?i)`({cache_pattern}/[^`]+\.png)`"),
            format!(r"(?i)({cache_pattern}/\S+\.png)"),
        ]
        .into_iter()
        .map(|pattern| Regex::new(&pattern).expect("fixed CLI image pattern"))
        .collect();
        Self {
            response: Mutex::new(Response::default()),
            show_images,
            cache,
            patterns,
            renderer: discover_renderer(),
        }
    }

    pub(super) async fn finish(&self, root: &Path) {
        let response = std::mem::take(&mut *self.response.lock().unwrap());
        if response.streamed {
            println!();
        }
        if !self.show_images {
            return;
        }
        let mut displayed = HashSet::new();
        let mut attempts = 0;
        for pattern in &self.patterns {
            for matched in pattern.captures_iter(&response.text) {
                if attempts >= 16 {
                    return;
                }
                attempts += 1;
                let Some((path, bytes)) = resolve_image(root, &self.cache, matched[1].trim())
                else {
                    continue;
                };
                if !displayed.insert(path.clone()) {
                    continue;
                }
                println!();
                let rendered = match &self.renderer {
                    Some(renderer) => render(renderer, &bytes).await,
                    None => Err(anyhow::anyhow!("Renderer unavailable")),
                };
                match rendered {
                    Ok(output) => print!("{output}"),
                    Err(_) => println!("[Image: {}]", path.display()),
                }
                let _ = io::stdout().flush();
            }
        }
    }
}

#[async_trait]
impl Sink for Console {
    async fn emit(&self, event: Value) -> Result<()> {
        match event["type"].as_str().unwrap_or("") {
            "chunk" => {
                let text = event["text"].as_str().unwrap_or("");
                let mut response = self.response.lock().unwrap();
                response.streamed = true;
                if response.text.len().saturating_add(text.len()) <= 2 * 1024 * 1024 {
                    response.text.push_str(text);
                }
                print!("{text}");
                io::stdout().flush()?;
            }
            "response" if !self.response.lock().unwrap().streamed => {
                println!("{}", event["text"].as_str().unwrap_or(""));
            }
            "error" | "permission_denied" => {
                eprintln!("{}", event["message"].as_str().unwrap_or("Request failed"));
            }
            "activity" => println!("{}", event["message"].as_str().unwrap_or("")),
            _ => {}
        }
        Ok(())
    }

    async fn upload(&self, name: &str, mime: &str, _bytes: Vec<u8>) -> Result<()> {
        if !mime.starts_with("image/") {
            bail!("CLI does not display documents");
        }
        let label = if name.is_empty() || name == "Image" {
            mime
        } else {
            name
        };
        println!("\n[Image received: {label}]");
        Ok(())
    }
}

fn resolve_image(root: &Path, cache: &Path, mentioned: &str) -> Option<(PathBuf, Vec<u8>)> {
    let supplied = Path::new(mentioned);
    if mentioned.len() > 4096
        || mentioned.contains('\0')
        || supplied
            .components()
            .any(|part| matches!(part, Component::ParentDir))
        || (supplied.is_absolute() && !supplied.starts_with(root) && !supplied.starts_with(cache))
    {
        return None;
    }
    let exact = if supplied.is_absolute() {
        supplied.to_owned()
    } else {
        root.join(supplied)
    };
    let basename = supplied.file_name()?.to_str()?;
    let without = Regex::new(r"_page_(\d+)")
        .ok()?
        .replace(basename, "_page$1");
    let with = Regex::new(r"_page(\d+)")
        .ok()?
        .replace(basename, "_page_$1");
    for path in [
        exact,
        cache.join(without.as_ref()),
        cache.join(with.as_ref()),
    ] {
        let path = path
            .components()
            .filter(|part| !matches!(part, Component::CurDir))
            .collect::<PathBuf>();
        let Ok(media) =
            media::read_supported(root, cache, &path.to_string_lossy(), 20 * 1024 * 1024)
        else {
            continue;
        };
        if image::guess_format(&media.bytes).ok() != Some(image::ImageFormat::Png) {
            continue;
        }
        let Ok((width, height)) = image::ImageReader::with_format(
            std::io::Cursor::new(&media.bytes),
            image::ImageFormat::Png,
        )
        .into_dimensions() else {
            continue;
        };
        if width == 0
            || height == 0
            || width > 32768
            || height > 32768
            || u64::from(width) * u64::from(height) > 20_000_000
        {
            continue;
        }
        return Some((path, media.bytes));
    }
    None
}

#[cfg(unix)]
fn discover_renderer() -> Option<Renderer> {
    let cwd = std::env::current_dir().ok()?;
    let search = std::env::var_os("PATH")?;
    if search.len() > 16 * 1024 {
        return None;
    }
    std::env::split_paths(&search)
        .take(64)
        .find_map(|directory| {
            let candidate = cwd.join(directory).join("chafa");
            let bindings = renderer_bindings(&candidate, &cwd)?;
            Some(Renderer {
                path: candidate,
                cwd: cwd.clone(),
                bindings,
            })
        })
}

#[cfg(unix)]
fn renderer_bindings(candidate: &Path, cwd: &Path) -> Option<Vec<(PathBuf, RendererIdentity)>> {
    use std::os::unix::fs::MetadataExt;
    let bounded =
        |path: &Path| path.as_os_str().len() <= 4096 && path.components().take(65).count() <= 64;
    if !bounded(candidate) {
        return None;
    }
    let owner = unsafe { libc::geteuid() };
    let trusted = |metadata: &std::fs::Metadata| {
        (metadata.uid() == owner || metadata.uid() == 0)
            && (metadata.is_symlink() || metadata.mode() & 0o022 == 0)
    };
    let metadata = candidate.metadata().ok()?;
    if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
        return None;
    }
    let mut bindings = Vec::new();
    let mut visited = HashSet::new();
    let mut admit = |path: &Path, metadata: std::fs::Metadata| {
        if !trusted(&metadata) {
            return None;
        }
        if visited.insert(path.to_owned()) {
            let identity = RendererIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
                mode: metadata.mode(),
                owner: metadata.uid(),
                group: metadata.gid(),
                version: (!metadata.is_dir()).then(|| {
                    (
                        metadata.len(),
                        metadata.mtime(),
                        metadata.mtime_nsec(),
                        metadata.ctime(),
                        metadata.ctime_nsec(),
                    )
                }),
            };
            bindings.push((path.to_owned(), identity));
        }
        Some(())
    };
    for original in candidate.ancestors() {
        admit(original, std::fs::symlink_metadata(original).ok()?)?;
        let resolved = original.canonicalize().ok()?;
        if !bounded(&resolved) {
            return None;
        }
        for ancestor in resolved.ancestors() {
            admit(ancestor, ancestor.metadata().ok()?)?;
            if ancestor == cwd {
                break;
            }
        }
        if original == cwd {
            break;
        }
    }
    Some(bindings)
}

#[cfg(not(unix))]
fn discover_renderer() -> Option<Renderer> {
    None
}

#[cfg(target_os = "linux")]
async fn render(renderer: &Renderer, bytes: &[u8]) -> Result<String> {
    use std::{
        os::fd::{AsRawFd, FromRawFd},
        process::Stdio,
        time::Duration,
    };
    use tokio::io::AsyncReadExt;

    let descriptor = unsafe {
        libc::memfd_create(
            c"cli-image".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if descriptor < 0 {
        bail!("Cannot create image snapshot");
    }
    let mut snapshot = unsafe { std::fs::File::from_raw_fd(descriptor) };
    use std::os::unix::fs::PermissionsExt;
    snapshot.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    snapshot.write_all(bytes)?;
    let descriptor = snapshot.as_raw_fd();
    if unsafe {
        libc::fcntl(
            descriptor,
            libc::F_ADD_SEALS,
            libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
        )
    } < 0
    {
        bail!("Cannot seal image snapshot");
    }
    let mut command = tokio::process::Command::new(&renderer.path);
    command
        .args(["--size=60", "--colors=256", "--symbols=block"])
        .arg(format!("/proc/self/fd/{descriptor}"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(descriptor, libc::F_SETFD, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    if renderer_bindings(&renderer.path, &renderer.cwd).as_ref() != Some(&renderer.bindings) {
        bail!("Renderer changed");
    }
    let mut child = command.spawn()?;
    struct ProcessGroup(u32);
    impl Drop for ProcessGroup {
        fn drop(&mut self) {
            unsafe {
                libc::kill(-(self.0 as i32), libc::SIGKILL);
            }
        }
    }
    let group = ProcessGroup(
        child
            .id()
            .ok_or_else(|| anyhow::anyhow!("Missing renderer process"))?,
    );
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Missing renderer output"))?
        .take(128 * 1024 + 1);
    let mut output = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let (status, _) = tokio::try_join!(child.wait(), stdout.read_to_end(&mut output))?;
        if !status.success() || output.len() > 128 * 1024 {
            bail!("Renderer failed");
        }
        Ok(String::from_utf8(output)?)
    })
    .await;
    drop(group);
    if result.is_err() {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    }
    result.map_err(|_| anyhow::anyhow!("Renderer timed out"))?
}

#[cfg(not(target_os = "linux"))]
async fn render(_renderer: &Renderer, _bytes: &[u8]) -> Result<String> {
    bail!("Owned renderer snapshots unavailable");
}
