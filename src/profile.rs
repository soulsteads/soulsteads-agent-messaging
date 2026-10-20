use anyhow::{Result, bail};
#[cfg(target_os = "linux")]
use std::path::Component;
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
fn absolute(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                result.pop();
            }
            Component::CurDir => {}
            component => result.push(component.as_os_str()),
        }
    }
    Ok(result)
}

pub(crate) fn apply(profile: &str, workspace: &Path) -> Result<Applied> {
    #[cfg(target_os = "linux")]
    {
        linux::apply(
            &absolute(Path::new(profile))?,
            &absolute(workspace)?,
            &mut |_| {},
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (profile, workspace);
        bail!("ACP profile copying requires Linux descriptor publication")
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::Applied;
#[cfg(not(target_os = "linux"))]
pub(crate) struct Applied {
    pub(crate) directory: PathBuf,
}
#[cfg(not(target_os = "linux"))]
impl Applied {
    pub(crate) fn verify(&self) -> Result<()> {
        bail!("Unsupported ACP profile filesystem")
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::{
        ffi::{CStr, CString},
        fs::File,
        io::{Read, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::ffi::OsStrExt,
        },
        sync::Arc,
    };

    const MAX_ENTRIES: usize = 1024;
    const MAX_DEPTH: usize = 32;
    const MAX_FILE: u64 = 1024 * 1024;
    const MAX_TOTAL: u64 = 16 * 1024 * 1024;

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(super) enum Phase {
        AfterRead,
        BeforeCopy,
        BeforePublish,
        AfterPublish,
    }
    #[derive(Clone, Copy, PartialEq, Eq)]
    struct Identity {
        device: u64,
        inode: u64,
        owner: u32,
        mode: u32,
        links: u64,
        size: i64,
        modified: (i64, i64),
        changed: (i64, i64),
    }
    impl Identity {
        fn from(stat: libc::stat) -> Self {
            Self {
                device: stat.st_dev,
                inode: stat.st_ino,
                owner: stat.st_uid,
                mode: stat.st_mode,
                links: stat.st_nlink,
                size: stat.st_size,
                modified: (stat.st_mtime, stat.st_mtime_nsec),
                changed: (stat.st_ctime, stat.st_ctime_nsec),
            }
        }
        fn directory_matches(self, other: Self) -> bool {
            (self.device, self.inode, self.owner, self.mode)
                == (other.device, other.inode, other.owner, other.mode)
        }
        fn directory(self, owned: bool) -> Result<()> {
            let owner = unsafe { libc::geteuid() };
            if self.mode & libc::S_IFMT != libc::S_IFDIR
                || (self.owner != owner && (owned || self.owner != 0))
            {
                bail!("ACP profile requires owned directories without symlinks");
            }
            Ok(())
        }
        fn regular(self) -> Result<()> {
            if self.mode & libc::S_IFMT != libc::S_IFREG
                || self.links != 1
                || self.owner != unsafe { libc::geteuid() }
            {
                bail!("ACP profile requires owned single-link regular files");
            }
            Ok(())
        }
    }
    fn checked(result: i32) -> Result<i32> {
        if result < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(result)
    }
    fn identity(file: &File) -> Result<Identity> {
        let mut stat = std::mem::MaybeUninit::uninit();
        checked(unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) })?;
        Ok(Identity::from(unsafe { stat.assume_init() }))
    }
    fn named(parent: &Directory, name: &CStr) -> Result<Option<Identity>> {
        let mut stat = std::mem::MaybeUninit::uninit();
        let result = unsafe {
            libc::fstatat(
                parent.file.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
            return Ok(None);
        }
        checked(result)?;
        Ok(Some(Identity::from(unsafe { stat.assume_init() })))
    }
    fn open(parent: &File, name: &CStr, flags: i32) -> Result<File> {
        let descriptor = checked(unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0o600,
            )
        })?;
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
    struct Directory {
        file: File,
        admitted: Identity,
        parent: Option<Arc<Directory>>,
        name: CString,
    }
    impl Directory {
        fn verify(&self) -> Result<()> {
            if !self.admitted.directory_matches(identity(&self.file)?) {
                bail!("ACP profile directory changed");
            }
            if let Some(parent) = &self.parent {
                parent.verify()?;
                if !named(parent, &self.name)?
                    .is_some_and(|current| self.admitted.directory_matches(current))
                {
                    bail!("ACP profile directory binding changed");
                }
            }
            Ok(())
        }
        fn verify_source(&self) -> Result<()> {
            self.verify()?;
            if identity(&self.file)? != self.admitted {
                bail!("ACP profile directory contents changed");
            }
            Ok(())
        }
    }
    fn child(
        parent: Arc<Directory>,
        name: CString,
        create: bool,
        owned: bool,
    ) -> Result<Arc<Directory>> {
        parent.verify()?;
        let mut observed = named(&parent, &name)?;
        if observed.is_none() && create {
            checked(unsafe { libc::mkdirat(parent.file.as_raw_fd(), name.as_ptr(), 0o700) })?;
            observed = named(&parent, &name)?;
        }
        let observed =
            observed.ok_or_else(|| anyhow::anyhow!("ACP profile directory does not exist"))?;
        observed.directory(owned)?;
        let file = open(&parent.file, &name, libc::O_RDONLY | libc::O_DIRECTORY)?;
        if !observed.directory_matches(identity(&file)?) {
            bail!("ACP profile directory replaced during open");
        }
        let directory = Arc::new(Directory {
            file,
            admitted: observed,
            parent: Some(parent),
            name,
        });
        directory.verify()?;
        Ok(directory)
    }
    fn root(path: &Path, create: bool) -> Result<Arc<Directory>> {
        if path == Path::new("/") {
            bail!("ACP profile root must be an owned directory tree");
        }
        let file = unsafe {
            File::from_raw_fd(checked(libc::open(
                c"/".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            ))?)
        };
        let mut directory = Arc::new(Directory {
            admitted: identity(&file)?,
            file,
            parent: None,
            name: CString::new("/")?,
        });
        let components: Vec<_> = path
            .components()
            .filter_map(|component| match component {
                Component::Normal(name) => Some(name),
                _ => None,
            })
            .collect();
        if components.len() > 128 {
            bail!("ACP profile path exceeds ancestor limit");
        }
        for (index, name) in components.iter().enumerate() {
            directory = child(
                directory,
                CString::new(name.as_bytes())?,
                create,
                index + 1 == components.len(),
            )?;
        }
        Ok(directory)
    }
    fn names(directory: &Directory) -> Result<Vec<CString>> {
        let descriptor = checked(unsafe { libc::dup(directory.file.as_raw_fd()) })?;
        let stream = unsafe { libc::fdopendir(descriptor) };
        if stream.is_null() {
            unsafe {
                libc::close(descriptor);
            }
            return Err(std::io::Error::last_os_error().into());
        }
        struct Stream(*mut libc::DIR);
        impl Drop for Stream {
            fn drop(&mut self) {
                unsafe {
                    libc::closedir(self.0);
                }
            }
        }
        let stream = Stream(stream);
        let mut result = Vec::new();
        loop {
            unsafe {
                *libc::__errno_location() = 0;
            }
            let entry = unsafe { libc::readdir(stream.0) };
            if entry.is_null() {
                if std::io::Error::last_os_error().raw_os_error() != Some(0) {
                    return Err(std::io::Error::last_os_error().into());
                }
                break;
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name == c"." || name == c".." {
                continue;
            }
            result.push(name.to_owned());
            if result.len() > MAX_ENTRIES {
                bail!("ACP profile entry limit exceeded");
            }
        }
        result.sort();
        Ok(result)
    }
    struct SourceFile {
        parent: Arc<Directory>,
        name: CString,
        file: File,
        admitted: Identity,
        bytes: Vec<u8>,
    }
    impl SourceFile {
        fn verify(&self) -> Result<()> {
            self.parent.verify_source()?;
            if identity(&self.file)? != self.admitted
                || named(&self.parent, &self.name)? != Some(self.admitted)
            {
                bail!("ACP profile source file changed");
            }
            Ok(())
        }
    }
    enum Source {
        Directory {
            name: CString,
            directory: Arc<Directory>,
            children: Vec<Source>,
        },
        File(SourceFile),
    }
    impl Source {
        fn verify(&self) -> Result<()> {
            match self {
                Self::Directory {
                    directory,
                    children,
                    ..
                } => {
                    directory.verify_source()?;
                    for child in children {
                        child.verify()?;
                    }
                }
                Self::File(file) => file.verify()?,
            }
            Ok(())
        }
    }
    fn scan(
        directory: Arc<Directory>,
        depth: usize,
        count: &mut usize,
        total: &mut u64,
        hook: &mut impl FnMut(Phase),
    ) -> Result<Vec<Source>> {
        if depth > MAX_DEPTH {
            bail!("ACP profile depth limit exceeded");
        }
        let mut children = Vec::new();
        for name in names(&directory)? {
            *count += 1;
            if *count > MAX_ENTRIES {
                bail!("ACP profile entry limit exceeded");
            }
            directory.verify_source()?;
            let observed = named(&directory, &name)?
                .ok_or_else(|| anyhow::anyhow!("ACP profile entry disappeared"))?;
            if observed.mode & libc::S_IFMT == libc::S_IFDIR {
                let nested = child(directory.clone(), name.clone(), false, true)?;
                let entries = scan(nested.clone(), depth + 1, count, total, hook)?;
                children.push(Source::Directory {
                    name,
                    directory: nested,
                    children: entries,
                });
            } else {
                observed.regular()?;
                if observed.size < 0 || observed.size as u64 > MAX_FILE {
                    bail!("ACP profile file limit exceeded");
                }
                *total += observed.size as u64;
                if *total > MAX_TOTAL {
                    bail!("ACP profile aggregate limit exceeded");
                }
                let mut file = open(&directory.file, &name, libc::O_RDONLY)?;
                if identity(&file)? != observed {
                    bail!("ACP profile source replaced during open");
                }
                let mut bytes = Vec::new();
                (&mut file).take(MAX_FILE + 1).read_to_end(&mut bytes)?;
                hook(Phase::AfterRead);
                if bytes.len() as i64 != observed.size {
                    bail!("ACP profile source size changed");
                }
                let source = SourceFile {
                    parent: directory.clone(),
                    name,
                    file,
                    admitted: observed,
                    bytes,
                };
                source.verify()?;
                children.push(Source::File(source));
            }
        }
        directory.verify_source()?;
        Ok(children)
    }
    struct Leaf {
        file: File,
        admitted: Identity,
    }
    fn verify_leaf(parent: &Directory, name: &CStr, expected: Option<&Leaf>) -> Result<()> {
        parent.verify()?;
        if named(parent, name)? != expected.map(|leaf| leaf.admitted) {
            bail!("ACP profile destination file changed");
        }
        if let Some(leaf) = expected
            && identity(&leaf.file)? != leaf.admitted
        {
            bail!("ACP profile retained destination file changed");
        }
        Ok(())
    }
    enum Destination {
        Directory {
            name: CString,
            directory: Option<Arc<Directory>>,
            children: Vec<Destination>,
        },
        File {
            name: CString,
            admitted: Option<Leaf>,
        },
    }
    fn preflight(source: &[Source], parent: Option<&Arc<Directory>>) -> Result<Vec<Destination>> {
        let mut result = Vec::new();
        for entry in source {
            let name = match entry {
                Source::Directory { name, .. } => name,
                Source::File(file) => &file.name,
            };
            let observed = parent
                .map(|parent| named(parent, name))
                .transpose()?
                .flatten();
            match entry {
                Source::Directory { children, .. } => {
                    let directory = if observed.is_some() {
                        Some(child(parent.unwrap().clone(), name.clone(), false, true)?)
                    } else {
                        None
                    };
                    result.push(Destination::Directory {
                        name: name.clone(),
                        children: preflight(children, directory.as_ref())?,
                        directory,
                    });
                }
                Source::File(_) => {
                    let admitted = observed
                        .map(|observed| -> Result<Leaf> {
                            observed.regular()?;
                            let file = open(&parent.unwrap().file, name, libc::O_PATH)?;
                            let leaf = Leaf {
                                file,
                                admitted: observed,
                            };
                            verify_leaf(parent.unwrap(), name, Some(&leaf))?;
                            Ok(leaf)
                        })
                        .transpose()?;
                    result.push(Destination::File {
                        name: name.clone(),
                        admitted,
                    });
                }
            }
        }
        Ok(result)
    }
    fn verify_destination(entries: &[Destination], parent: &Arc<Directory>) -> Result<()> {
        parent.verify()?;
        for entry in entries {
            match entry {
                Destination::Directory {
                    name,
                    directory,
                    children,
                } => {
                    if let Some(directory) = directory {
                        verify_destination(children, directory)?;
                    } else if named(parent, name)?.is_some() {
                        bail!("ACP profile destination appeared");
                    }
                }
                Destination::File { name, admitted } => {
                    verify_leaf(parent, name, admitted.as_ref())?;
                }
            }
        }
        Ok(())
    }
    fn publish(
        source: &SourceFile,
        parent: &Arc<Directory>,
        expected: Option<&Leaf>,
        hook: &mut impl FnMut(Phase),
    ) -> Result<Leaf> {
        source.verify()?;
        parent.verify()?;
        verify_leaf(parent, &source.name, expected)?;
        let mut stage = open(&parent.file, c".", libc::O_TMPFILE | libc::O_WRONLY)?;
        let initial = identity(&stage)?;
        if initial.mode & libc::S_IFMT != libc::S_IFREG
            || initial.links != 0
            || initial.owner != unsafe { libc::geteuid() }
        {
            bail!("Unsafe ACP profile staging file");
        }
        stage.write_all(&source.bytes)?;
        stage.sync_all()?;
        hook(Phase::BeforePublish);
        source.verify()?;
        parent.verify()?;
        verify_leaf(parent, &source.name, expected)?;
        let stage_name = CString::new(format!(".bridge-profile-{}", uuid::Uuid::new_v4()))?;
        let descriptor_path = CString::new(format!("/proc/self/fd/{}", stage.as_raw_fd()))?;
        checked(unsafe {
            libc::linkat(
                libc::AT_FDCWD,
                descriptor_path.as_ptr(),
                parent.file.as_raw_fd(),
                stage_name.as_ptr(),
                libc::AT_SYMLINK_FOLLOW,
            )
        })?;
        let staged = identity(&stage)?;
        staged.regular()?;
        if named(parent, &stage_name)? != Some(staged) || staged.size != source.bytes.len() as i64 {
            bail!("ACP profile staging binding changed");
        }
        source.verify()?;
        parent.verify()?;
        verify_leaf(parent, &source.name, expected)?;
        checked(unsafe {
            libc::renameat(
                parent.file.as_raw_fd(),
                stage_name.as_ptr(),
                parent.file.as_raw_fd(),
                source.name.as_ptr(),
            )
        })?;
        hook(Phase::AfterPublish);
        parent.file.sync_all()?;
        let published = identity(&stage)?;
        published.regular()?;
        parent.verify()?;
        source.verify()?;
        if named(parent, &source.name)? != Some(published) {
            bail!("ACP profile published binding changed");
        }
        Ok(Leaf {
            file: stage,
            admitted: published,
        })
    }
    fn copy(
        source: &[Source],
        destination: &mut [Destination],
        parent: &Arc<Directory>,
        source_root: &Arc<Directory>,
        hook: &mut impl FnMut(Phase),
    ) -> Result<()> {
        for (source, destination) in source.iter().zip(destination) {
            source_root.verify_source()?;
            source.verify()?;
            parent.verify()?;
            match (source, destination) {
                (
                    Source::Directory {
                        children: source_children,
                        ..
                    },
                    Destination::Directory {
                        name,
                        directory,
                        children,
                    },
                ) => {
                    if directory.is_none() {
                        if named(parent, name)?.is_some() {
                            bail!("ACP profile destination appeared");
                        }
                        *directory = Some(child(parent.clone(), name.clone(), true, true)?);
                    }
                    copy(
                        source_children,
                        children,
                        directory.as_ref().unwrap(),
                        source_root,
                        hook,
                    )?;
                }
                (Source::File(source), Destination::File { admitted, .. }) => {
                    *admitted = Some(publish(source, parent, admitted.as_ref(), hook)?);
                }
                _ => bail!("ACP profile copy plan mismatch"),
            }
        }
        Ok(())
    }
    pub(crate) struct Applied {
        pub(crate) directory: PathBuf,
        source_root: Arc<Directory>,
        source: Vec<Source>,
        destination_root: Arc<Directory>,
        destination: Vec<Destination>,
    }
    impl Applied {
        pub(crate) fn verify(&self) -> Result<()> {
            self.source_root.verify_source()?;
            for entry in &self.source {
                entry.verify()?;
            }
            verify_destination(&self.destination, &self.destination_root)
        }
    }
    pub(super) fn apply(
        source: &Path,
        destination: &Path,
        hook: &mut impl FnMut(Phase),
    ) -> Result<Applied> {
        let source_root = root(source, false)?;
        let source_entries = scan(source_root.clone(), 0, &mut 0, &mut 0, hook)?;
        let destination_root = root(destination, true)?;
        if source_root
            .admitted
            .directory_matches(destination_root.admitted)
        {
            bail!("ACP profile source and destination must differ");
        }
        let destination_entries = preflight(&source_entries, Some(&destination_root))?;
        let mut applied = Applied {
            directory: destination.to_owned(),
            source_root,
            source: source_entries,
            destination_root,
            destination: destination_entries,
        };
        hook(Phase::BeforeCopy);
        applied.verify()?;
        copy(
            &applied.source,
            &mut applied.destination,
            &applied.destination_root,
            &applied.source_root,
            hook,
        )?;
        applied.verify()?;
        Ok(applied)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::unix::fs::symlink;

        fn file(path: &Path, bytes: &[u8]) {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }

        #[test]
        fn profile_destination_collision_descriptor_is_retained_through_admission() {
            use std::os::unix::fs::MetadataExt;
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("profile");
            let destination = root.path().join("workspace");
            file(&source.join("AGENTS.md"), b"synthetic");
            file(&destination.join("AGENTS.md"), b"preexisting");
            let expected = std::fs::metadata(destination.join("AGENTS.md")).unwrap();
            let mut fired = false;
            let applied = apply(&source, &destination, &mut |phase| {
                if phase != Phase::BeforeCopy {
                    return;
                }
                fired = true;
                let retained = std::fs::read_dir("/proc/self/fd")
                    .unwrap()
                    .filter_map(|entry| entry.ok())
                    .any(|entry| {
                        std::fs::metadata(entry.path()).is_ok_and(|current| {
                            current.dev() == expected.dev() && current.ino() == expected.ino()
                        })
                    });
                assert!(
                    retained,
                    "destination collision must have a retained no-follow leaf descriptor"
                );
            })
            .unwrap();
            assert!(fired);
            applied.verify().unwrap();
        }

        #[test]
        fn profile_rebinding_at_read_copy_and_publication_boundaries_never_changes_external_sentinel()
         {
            for phase in [
                Phase::AfterRead,
                Phase::BeforeCopy,
                Phase::BeforePublish,
                Phase::AfterPublish,
            ] {
                for target in [
                    "source-root",
                    "source-ancestor",
                    "source-nested",
                    "source-leaf",
                    "destination-root",
                    "destination-ancestor",
                    "destination-nested",
                    "destination-leaf",
                ] {
                    if phase == Phase::AfterRead && target.starts_with("destination") {
                        continue;
                    }
                    let root = tempfile::tempdir().unwrap();
                    let source = root.path().join("sources/profile");
                    let destination = root.path().join("sessions/workspace");
                    file(&source.join("nested/AGENTS.md"), b"synthetic private bytes");
                    file(&destination.join("nested/AGENTS.md"), b"preexisting");
                    let external = root.path().join("external");
                    let sentinel = external.join("AGENTS.md");
                    file(&sentinel, b"external sentinel");
                    let selected = match target {
                        "source-root" => source.clone(),
                        "source-ancestor" => source.parent().unwrap().to_owned(),
                        "source-nested" => source.join("nested"),
                        "source-leaf" => source.join("nested/AGENTS.md"),
                        "destination-root" => destination.clone(),
                        "destination-ancestor" => destination.parent().unwrap().to_owned(),
                        "destination-nested" => destination.join("nested"),
                        _ => destination.join("nested/AGENTS.md"),
                    };
                    let mut fired = false;
                    let result = apply(&source, &destination, &mut |current| {
                        if fired || current != phase {
                            return;
                        }
                        fired = true;
                        std::fs::rename(&selected, root.path().join("detached")).unwrap();
                        if target.ends_with("leaf") {
                            symlink(&sentinel, &selected).unwrap();
                        } else {
                            symlink(&external, &selected).unwrap();
                        }
                    });
                    assert!(fired);
                    assert!(result.is_err(), "{target} must fail closed");
                    assert_eq!(std::fs::read(&sentinel).unwrap(), b"external sentinel");
                    assert_eq!(std::fs::read_dir(&external).unwrap().count(), 1);
                }
            }
        }

        #[test]
        fn profile_ordinary_directory_and_same_bytes_leaf_replacement_are_not_adopted() {
            for target in [
                "source-root",
                "source-parent",
                "source-leaf",
                "destination-root",
                "destination-parent",
                "destination-leaf",
            ] {
                let root = tempfile::tempdir().unwrap();
                let source = root.path().join("sources/profile");
                let destination = root.path().join("sessions/workspace");
                file(&source.join("nested/AGENTS.md"), b"synthetic");
                file(&destination.join("nested/AGENTS.md"), b"preexisting");
                let mut fired = false;
                let result = apply(&source, &destination, &mut |phase| {
                    if fired || phase != Phase::BeforeCopy {
                        return;
                    }
                    fired = true;
                    let base = if target.starts_with("source") {
                        &source
                    } else {
                        &destination
                    };
                    let bytes = if target.starts_with("source") {
                        b"synthetic".as_slice()
                    } else {
                        b"preexisting".as_slice()
                    };
                    if target.ends_with("leaf") {
                        let replacement = root.path().join("replacement");
                        file(&replacement, bytes);
                        std::fs::rename(replacement, base.join("nested/AGENTS.md")).unwrap();
                    } else {
                        let selected = if target.ends_with("parent") {
                            base.parent().unwrap()
                        } else {
                            base
                        };
                        std::fs::rename(selected, root.path().join("detached")).unwrap();
                        file(&base.join("nested/AGENTS.md"), bytes);
                    }
                });
                assert!(result.is_err(), "{target}");
                assert_eq!(
                    std::fs::read(destination.join("nested/AGENTS.md")).unwrap(),
                    b"preexisting"
                );
            }
        }

        #[test]
        fn profile_post_read_growth_and_hardlinking_fail_without_copying() {
            for case in ["growth", "hardlink", "new-entry"] {
                let root = tempfile::tempdir().unwrap();
                let source = root.path().join("profile");
                let destination = root.path().join("workspace");
                file(&source.join("AGENTS.md"), b"synthetic");
                let mut fired = false;
                let result = apply(&source, &destination, &mut |phase| {
                    if fired || phase != Phase::AfterRead {
                        return;
                    }
                    fired = true;
                    match case {
                        "growth" => {
                            std::fs::write(source.join("AGENTS.md"), b"changed contents").unwrap()
                        }
                        "hardlink" => std::fs::hard_link(
                            source.join("AGENTS.md"),
                            root.path().join("external-link"),
                        )
                        .unwrap(),
                        _ => symlink(root.path(), source.join("unsupported")).unwrap(),
                    }
                });
                assert!(result.is_err(), "{case}");
                assert!(!destination.exists());
            }
        }

        #[test]
        fn profile_limits_reject_before_copy_and_accept_exact_file_budget() {
            for case in ["file", "aggregate", "entries", "depth", "exact-file"] {
                let root = tempfile::tempdir().unwrap();
                let source = root.path().join("profile");
                let destination = root.path().join("workspace");
                std::fs::create_dir(&source).unwrap();
                match case {
                    "file" | "exact-file" => {
                        let size = MAX_FILE + u64::from(case == "file");
                        let output = File::create(source.join("AGENTS.md")).unwrap();
                        output.set_len(size).unwrap();
                    }
                    "aggregate" => {
                        for index in 0..17 {
                            File::create(source.join(format!("file-{index}")))
                                .unwrap()
                                .set_len(MAX_FILE)
                                .unwrap();
                        }
                    }
                    "entries" => {
                        for index in 0..=MAX_ENTRIES {
                            file(&source.join(format!("file-{index}")), b"");
                        }
                    }
                    _ => {
                        let mut nested = source.clone();
                        for _ in 0..=MAX_DEPTH {
                            nested.push("nested");
                        }
                        std::fs::create_dir_all(nested).unwrap();
                    }
                }
                let result = apply(&source, &destination, &mut |_| {});
                assert_eq!(result.is_ok(), case == "exact-file", "{case}");
                if case != "exact-file" {
                    assert!(!destination.exists());
                }
            }
        }

        #[test]
        fn profile_foreign_directory_and_leaf_ownership_is_rejected() {
            let current = unsafe { libc::geteuid() };
            let foreign = current.wrapping_add(1);
            for owner in [foreign, 0] {
                if owner == current {
                    continue;
                }
                let identity = Identity {
                    device: 1,
                    inode: 2,
                    owner,
                    mode: libc::S_IFREG | 0o600,
                    links: 1,
                    size: 0,
                    modified: (0, 0),
                    changed: (0, 0),
                };
                assert!(identity.regular().is_err());
            }
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("profile");
            let destination = root.path().join("workspace");
            file(&source.join("AGENTS.md"), b"synthetic");
            let source_root = super::root(&source, false).unwrap();
            let mut observed = identity(&source_root.file).unwrap();
            observed.owner = foreign;
            assert!(observed.directory(true).is_err());
            assert!(observed.directory(false).is_err());
            observed.owner = 0;
            assert!(observed.directory(false).is_ok());
            if current != 0 {
                assert!(observed.directory(true).is_err());
            }
            assert!(!destination.exists());
        }
    }
}
