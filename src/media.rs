use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    path::{Component, Path},
};
pub struct Media {
    pub name: String,
    pub mime: String,
    pub bytes: Vec<u8>,
}
pub fn read_supported(root: &Path, cache: &Path, path: &str, maximum: u64) -> Result<Media> {
    if Path::new(path).is_absolute() && Path::new(path).starts_with(cache) {
        let directory = DirectoryBinding::open(cache)?;
        let metadata = directory.file().metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o022 != 0
            {
                bail!("DocLibrary cache is not safely owned");
            }
        }
        binding_seam("after_cache_admission");
        read_admitted(directory, path, maximum)
    } else {
        read(root, path, maximum)
    }
}
pub fn save_inline(root: &Path, content: &Value) -> Result<String> {
    let data = content["data"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing image data"))?;
    if data.len() > 2 * 1024 * 1024 {
        bail!("Image data exceeds frame limit");
    }
    let bytes = STANDARD.decode(data)?;
    let format = image::guess_format(&bytes)?;
    let (extension, mime) = match format {
        image::ImageFormat::Png => ("png", "image/png"),
        image::ImageFormat::Jpeg => ("jpg", "image/jpeg"),
        image::ImageFormat::WebP => ("webp", "image/webp"),
        image::ImageFormat::Gif => ("gif", "image/gif"),
        _ => bail!("Unsupported image format"),
    };
    if content["mimeType"].as_str().unwrap_or("image/png") != mime {
        bail!("Image MIME does not match bytes");
    }
    let (width, height) = if format == image::ImageFormat::Gif && bytes.len() >= 10 {
        (
            u16::from_le_bytes([bytes[6], bytes[7]]) as u32,
            u16::from_le_bytes([bytes[8], bytes[9]]) as u32,
        )
    } else {
        image::ImageReader::with_format(std::io::Cursor::new(&bytes), format).into_dimensions()?
    };
    if width == 0
        || height == 0
        || width > 32768
        || height > 32768
        || u64::from(width) * u64::from(height) > 20_000_000
    {
        bail!("Invalid image dimensions");
    }
    let mut directory = DirectoryBinding::open(root)?;
    directory.revalidate()?;
    match create_private_directory(directory.file(), &directory.path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    directory.push(
        Path::new(".bridge-images"),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )?;
    let metadata = directory.file().metadata()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            bail!("Image directory is not privately owned");
        }
    }
    let name = format!("{:x}.{extension}", Sha256::digest(&bytes));
    let destination = directory.path.join(&name);
    binding_seam("before_inline_publish");
    directory.revalidate()?;
    match open_relative(
        directory.file(),
        &directory.path,
        Path::new(&name),
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    ) {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(&bytes)?;
            binding_seam("after_inline_write");
            directory.revalidate()?;
            let metadata = file.metadata()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.uid() != unsafe { libc::geteuid() } || metadata.nlink() != 1 {
                    bail!("Image publication is not an owned single-link file");
                }
            }
            if !metadata.is_file() || metadata.len() != bytes.len() as u64 {
                bail!("Image publication changed");
            }
            let named = open_relative(
                directory.file(),
                &directory.path,
                Path::new(&name),
                libc::O_RDONLY | libc::O_NONBLOCK,
                0,
            )?;
            if !same_snapshot(&metadata, &named.metadata()?) {
                bail!("Image publication changed");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_admitted(directory, &name, 2 * 1024 * 1024)?.bytes != bytes {
                bail!("Existing image bytes differ");
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(destination.to_string_lossy().into_owned())
}
pub fn read(root: &Path, path: &str, maximum: u64) -> Result<Media> {
    read_admitted(DirectoryBinding::open(root)?, path, maximum)
}

fn read_admitted(mut directory: DirectoryBinding, path: &str, maximum: u64) -> Result<Media> {
    let supplied = Path::new(path);
    if path.len() > 4096
        || path.contains('\0')
        || supplied
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        bail!("Invalid media path");
    }
    let candidate = if supplied.is_absolute() {
        supplied.to_owned()
    } else {
        directory.path.join(supplied)
    };
    let relative = candidate
        .strip_prefix(&directory.path)
        .map_err(|_| anyhow::anyhow!("Media path escapes session workspace"))?;
    let components = relative.components().collect::<Vec<_>>();
    let (leaf, parents) = components
        .split_last()
        .ok_or_else(|| anyhow::anyhow!("Missing media filename"))?;
    for component in parents {
        directory.push(
            Path::new(component.as_os_str()),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
    }
    binding_seam("before_media_open");
    directory.revalidate()?;
    directory.push(
        Path::new(leaf.as_os_str()),
        libc::O_RDONLY | libc::O_NONBLOCK,
        0,
    )?;
    let metadata = directory.file().metadata()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.nlink() != 1 {
            bail!("Media is not an owned single-link file");
        }
    }
    if !metadata.is_file() || metadata.len() > maximum {
        bail!("Media exceeds file size limit");
    }
    binding_seam("after_media_open");
    directory.revalidate()?;
    let mut bytes = Vec::new();
    directory
        .file()
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        bail!("Media exceeds file size limit");
    }
    binding_seam("after_media_read");
    directory.revalidate()?;
    if bytes.len() as u64 != metadata.len() {
        bail!("Media changed during read");
    }
    let name = candidate
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("attachment")
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    let mime = image::guess_format(&bytes)
        .map(|format| format.to_mime_type().to_owned())
        .unwrap_or_else(|_| {
            match candidate
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("")
                .to_ascii_lowercase()
                .as_str()
            {
                "pdf" => "application/pdf",
                "txt" | "md" | "csv" => "text/plain",
                _ => "application/octet-stream",
            }
            .into()
        });
    Ok(Media { name, mime, bytes })
}

struct BoundEntry {
    file: std::fs::File,
    name: std::path::PathBuf,
    metadata: std::fs::Metadata,
}

struct DirectoryBinding {
    entries: Vec<BoundEntry>,
    path: std::path::PathBuf,
}

impl DirectoryBinding {
    fn open(path: &Path) -> Result<Self> {
        let absolute = if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir()?.join(path)
        };
        let file = secure_open(Path::new("/"), libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let metadata = file.metadata()?;
        let mut binding = Self {
            entries: vec![BoundEntry {
                file,
                name: "/".into(),
                metadata,
            }],
            path: "/".into(),
        };
        for component in absolute.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(name) => {
                    binding.push(Path::new(name), libc::O_RDONLY | libc::O_DIRECTORY, 0)?
                }
                _ => bail!("Invalid media root"),
            }
        }
        binding.revalidate()?;
        Ok(binding)
    }

    fn file(&self) -> &std::fs::File {
        &self.entries.last().unwrap().file
    }

    fn push(&mut self, name: &Path, flags: i32, mode: u32) -> Result<()> {
        if name.components().count() != 1
            || !matches!(name.components().next(), Some(Component::Normal(_)))
        {
            bail!("Invalid media component");
        }
        let file = open_relative(self.file(), &self.path, name, flags, mode)?;
        let metadata = file.metadata()?;
        self.entries.push(BoundEntry {
            file,
            name: name.to_owned(),
            metadata,
        });
        self.path.push(name);
        self.revalidate()
    }

    fn revalidate(&self) -> Result<()> {
        let mut parent_path = std::path::PathBuf::from("/");
        for (index, entry) in self.entries.iter().enumerate() {
            if !same_snapshot(&entry.metadata, &entry.file.metadata()?) {
                bail!("Media object changed");
            }
            if index > 0 {
                let flags = libc::O_RDONLY
                    | libc::O_NONBLOCK
                    | if entry.metadata.is_dir() {
                        libc::O_DIRECTORY
                    } else {
                        0
                    };
                let named = open_relative(
                    &self.entries[index - 1].file,
                    &parent_path,
                    &entry.name,
                    flags,
                    0,
                )?;
                if !same_snapshot(&entry.metadata, &named.metadata()?) {
                    bail!("Media directory or leaf rebound");
                }
                parent_path.push(&entry.name);
            }
        }
        Ok(())
    }
}

fn same_snapshot(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        before.dev() == after.dev()
            && before.ino() == after.ino()
            && before.mode() == after.mode()
            && before.uid() == after.uid()
            && before.gid() == after.gid()
            && (before.is_dir()
                || (before.nlink() == after.nlink()
                    && before.len() == after.len()
                    && before.mtime() == after.mtime()
                    && before.mtime_nsec() == after.mtime_nsec()
                    && before.ctime() == after.ctime()
                    && before.ctime_nsec() == after.ctime_nsec()))
    }
    #[cfg(not(unix))]
    {
        before.is_dir() == after.is_dir()
            && before.len() == after.len()
            && before.modified().ok() == after.modified().ok()
    }
}

#[cfg(unix)]
fn open_relative(
    directory: &std::fs::File,
    _parent: &Path,
    name: &Path,
    flags: i32,
    mode: u32,
) -> std::io::Result<std::fs::File> {
    use std::os::unix::{
        ffi::OsStrExt,
        io::{AsRawFd, FromRawFd},
    };
    let name = std::ffi::CString::new(name.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid media name"))?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::fs::File::from_raw_fd(descriptor) })
}

#[cfg(not(unix))]
fn open_relative(
    _directory: &std::fs::File,
    parent: &Path,
    name: &Path,
    flags: i32,
    mode: u32,
) -> std::io::Result<std::fs::File> {
    secure_open(&parent.join(name), flags, mode)
}

#[cfg(unix)]
fn create_private_directory(directory: &std::fs::File, _parent: &Path) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    if unsafe { libc::mkdirat(directory.as_raw_fd(), c".bridge-images".as_ptr(), 0o700) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
fn create_private_directory(_directory: &std::fs::File, parent: &Path) -> std::io::Result<()> {
    std::fs::create_dir(parent.join(".bridge-images"))
}

fn binding_seam(_stage: &str) {
    #[cfg(test)]
    BINDING_HOOK.with(|hook| {
        let callback = hook.borrow_mut().take();
        if let Some((stage, callback)) = callback {
            if stage == _stage {
                callback();
            } else {
                *hook.borrow_mut() = Some((stage, callback));
            }
        }
    });
}

#[cfg(test)]
type BindingHook = Option<(&'static str, Box<dyn FnOnce()>)>;
#[cfg(test)]
thread_local! {
    static BINDING_HOOK: std::cell::RefCell<BindingHook> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, unix))]
mod binding_tests {
    use super::*;

    #[test]
    fn ordinary_directory_replacement_after_containment_is_rejected() {
        for replaced in ["root", "ancestor", "nested"] {
            let temporary = tempfile::tempdir().unwrap();
            let ancestor = temporary.path().join("ancestor");
            let root = ancestor.join("session");
            let parent = root.join("nested");
            std::fs::create_dir_all(&parent).unwrap();
            std::fs::write(parent.join("image.txt"), b"admitted").unwrap();
            let target = match replaced {
                "root" => root.clone(),
                "ancestor" => ancestor,
                _ => parent,
            };
            let replacement = temporary.path().join("replacement");
            let suffix = root
                .join("nested/image.txt")
                .strip_prefix(&target)
                .unwrap()
                .to_owned();
            std::fs::create_dir_all(replacement.join(&suffix).parent().unwrap()).unwrap();
            std::fs::write(replacement.join(&suffix), b"replacement bytes").unwrap();
            let detached = temporary.path().join("detached");
            let replacement_target = target.clone();
            BINDING_HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    "before_media_open",
                    Box::new(move || {
                        std::fs::rename(target, detached).unwrap();
                        std::fs::rename(replacement, replacement_target).unwrap();
                    }),
                ));
            });
            let result = read(&root, "nested/image.txt", 1024);
            assert!(
                result.is_err(),
                "{replaced}: replacement bytes returned: {:?}",
                result.ok().map(|media| media.bytes)
            );
        }
    }

    #[test]
    fn opened_media_rejects_root_parent_leaf_rebinding_and_mutation() {
        for stage in ["after_media_open", "after_media_read"] {
            for changed in ["root", "parent", "leaf", "same_size", "growth", "hardlink"] {
                let temporary = tempfile::tempdir().unwrap();
                let root = temporary.path().join("root");
                let parent = root.join("nested");
                std::fs::create_dir_all(&parent).unwrap();
                let leaf = parent.join("image.txt");
                std::fs::write(&leaf, b"admitted").unwrap();
                let detached = temporary.path().join("detached");
                let changed_leaf = leaf.clone();
                let changed_root = root.clone();
                BINDING_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some((
                        stage,
                        Box::new(move || match changed {
                            "root" | "parent" => {
                                let target = if changed == "root" {
                                    changed_root
                                } else {
                                    parent
                                };
                                std::fs::rename(&target, detached).unwrap();
                                std::fs::create_dir_all(&target).unwrap();
                            }
                            "leaf" => {
                                std::fs::rename(&changed_leaf, detached).unwrap();
                                std::fs::write(changed_leaf, b"admitted").unwrap();
                            }
                            "same_size" => std::fs::write(changed_leaf, b"mutated!").unwrap(),
                            "growth" => std::fs::write(changed_leaf, vec![0; 1025]).unwrap(),
                            _ => std::fs::hard_link(changed_leaf, detached).unwrap(),
                        }),
                    ));
                });
                assert!(
                    read(&root, "nested/image.txt", 1024).is_err(),
                    "{stage}/{changed}"
                );
            }
        }
    }

    #[test]
    fn doclibrary_admission_retains_the_owned_root_through_read() {
        for unsafe_mode in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let cache = temporary.path().join("doclibrary_cache");
            std::fs::create_dir(&cache).unwrap();
            std::fs::write(cache.join("image.txt"), b"admitted").unwrap();
            let replacement = temporary.path().join("replacement");
            std::fs::create_dir(&replacement).unwrap();
            std::fs::write(replacement.join("image.txt"), b"replacement bytes").unwrap();
            if unsafe_mode {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o777))
                    .unwrap();
            }
            let target = cache.clone();
            let detached = temporary.path().join("detached");
            BINDING_HOOK.with(|hook| {
                *hook.borrow_mut() = Some((
                    "after_cache_admission",
                    Box::new(move || {
                        std::fs::rename(&target, detached).unwrap();
                        std::fs::rename(replacement, target).unwrap();
                    }),
                ));
            });
            assert!(
                read_supported(
                    temporary.path(),
                    &cache,
                    cache.join("image.txt").to_str().unwrap(),
                    1024
                )
                .is_err()
            );
        }
    }

    #[test]
    fn inline_publication_rejects_rebinding_without_touching_replacement() {
        for stage in ["before_inline_publish", "after_inline_write"] {
            for changed in ["root", "ancestor", "private"] {
                let temporary = tempfile::tempdir().unwrap();
                let ancestor = temporary.path().join("ancestor");
                let root = ancestor.join("session");
                std::fs::create_dir_all(root.join(".bridge-images")).unwrap();
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    root.join(".bridge-images"),
                    std::fs::Permissions::from_mode(0o700),
                )
                .unwrap();
                let mut bytes = std::io::Cursor::new(Vec::new());
                image::DynamicImage::new_rgb8(1, 1)
                    .write_to(&mut bytes, image::ImageFormat::Png)
                    .unwrap();
                let content = serde_json::json!({"data": STANDARD.encode(bytes.into_inner()), "mimeType": "image/png"});
                let target = match changed {
                    "root" => root.clone(),
                    "ancestor" => ancestor,
                    _ => root.join(".bridge-images"),
                };
                let replacement = temporary.path().join("replacement");
                std::fs::create_dir(&replacement).unwrap();
                std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
                let detached = temporary.path().join("detached");
                let replacement_target = target.clone();
                BINDING_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some((
                        stage,
                        Box::new(move || {
                            std::fs::rename(&target, detached).unwrap();
                            std::fs::rename(replacement, target).unwrap();
                        }),
                    ));
                });
                assert!(save_inline(&root, &content).is_err(), "{stage}/{changed}");
                assert_eq!(
                    std::fs::read_dir(replacement_target).unwrap().count(),
                    0,
                    "replacement must remain untouched"
                );
            }
        }
    }

    #[test]
    fn retained_binding_preserves_regular_owner_link_size_and_nofollow_checks() {
        use std::os::unix::fs::symlink;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let leaf = root.join("image.txt");
        std::fs::write(&leaf, b"admitted").unwrap();
        assert_eq!(read(&root, "image.txt", 8).unwrap().bytes, b"admitted");
        assert!(read(&root, "image.txt", 7).is_err());
        assert!(read(&root, "../root/image.txt", 1024).is_err());
        assert!(read(&root, "image.txt\0", 1024).is_err());
        symlink(&root, temporary.path().join("root-link")).unwrap();
        assert!(read(&temporary.path().join("root-link"), "image.txt", 1024).is_err());
        symlink(&root, root.join("parent-link")).unwrap();
        assert!(read(&root, "parent-link/image.txt", 1024).is_err());
        symlink(&leaf, root.join("leaf-link")).unwrap();
        assert!(read(&root, "leaf-link", 1024).is_err());
        std::fs::create_dir(root.join("directory")).unwrap();
        assert!(read(&root, "directory", 1024).is_err());
        let fifo =
            std::ffi::CString::new(root.join("fifo").as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(read(&root, "fifo", 1024).is_err());
        std::fs::hard_link(&leaf, root.join("hardlink")).unwrap();
        assert!(read(&root, "image.txt", 1024).is_err());
    }
}

#[cfg(unix)]
pub(crate) fn secure_open(path: &Path, flags: i32, mode: u32) -> std::io::Result<std::fs::File> {
    use std::os::unix::{
        ffi::OsStrExt,
        io::{AsRawFd, FromRawFd},
    };
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let components = absolute
        .components()
        .filter(|component| !matches!(component, Component::RootDir | Component::CurDir))
        .collect::<Vec<_>>();
    if components
        .iter()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid media path",
        ));
    }
    let descriptor = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut directory = unsafe { std::fs::File::from_raw_fd(descriptor) };
    for (index, component) in components.iter().enumerate() {
        let name = std::ffi::CString::new(component.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid media path")
        })?;
        let requested = if index + 1 == components.len() {
            flags
        } else {
            libc::O_RDONLY | libc::O_DIRECTORY
        };
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                requested | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        directory = unsafe { std::fs::File::from_raw_fd(descriptor) };
    }
    Ok(directory)
}

#[cfg(not(unix))]
pub(crate) fn secure_open(path: &Path, flags: i32, _mode: u32) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .read(flags & libc::O_WRONLY == 0)
        .write(flags & libc::O_WRONLY != 0)
        .create_new(flags & libc::O_CREAT != 0);
    options.open(path)
}
