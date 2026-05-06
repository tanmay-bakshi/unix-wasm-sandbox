use std::{
    collections::HashMap,
    future::Future,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite, AsyncWriteExt, ReadBuf};
use virtual_fs::{
    create_dir_all, host_fs, FileOpener, FileSystem, FsError, NullFile, OpenOptionsConfig,
    OverlayFileSystem, StaticFile, TmpFileSystem, UnionFileSystem, UnionMergeMode, VirtualFile,
};
use wasmer::sys::{BaseTunables, Cranelift, EngineBuilder, Features, NativeEngineExt};
use wasmer_package::utils::from_bytes;
use wasmer_wasix::{
    bin_factory::{spawn_exec, BinaryPackage},
    runtime::{
        package_loader::BuiltinPackageLoader,
        resolver::InMemorySource,
        task_manager::{tokio::TokioTaskManager, VirtualTaskManagerExt},
    },
    wasmer_wasix_types::types::Signal,
    PluggableRuntime, Runtime, WasiEnvBuilder,
};
use webc::metadata::annotations::Wasi;

static CATALOGS: Lazy<Mutex<HashMap<PathBuf, Arc<PackageCatalog>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

const STANDARD_PACKAGE_NAMES: &[&str] = &[
    "coreutils",
    "bash",
    "grep",
    "sed",
    "find",
    "tar",
    "gzip",
    "python",
];

const COMMAND_PATH_PREFIXES: &[&str] = &["/bin", "/usr/bin"];

#[derive(Clone)]
pub struct Limits {
    pub output_bytes: usize,
    pub wall_time_seconds: Option<f64>,
}

#[derive(Clone)]
pub struct HostMount {
    pub source: String,
    pub target: String,
    pub read_only: bool,
}

#[derive(Clone)]
pub struct CompletedProcess {
    pub args: Vec<String>,
    pub returncode: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Clone)]
pub struct SandboxState {
    pub fs: TmpFileSystem,
    pub cwd: String,
    pub env: HashMap<String, String>,
    pub limits: Limits,
    pub catalog: Arc<PackageCatalog>,
    pub events: EventBus,
}

#[derive(Clone)]
struct CommandTarget {
    package: String,
    command: String,
}

pub struct PackageCatalog {
    runtime: Arc<dyn Runtime + Send + Sync>,
    handle: tokio::runtime::Handle,
    packages: HashMap<String, Arc<BinaryPackage>>,
    command_paths: HashMap<PathBuf, CommandTarget>,
}

pub struct RunRequest {
    pub args: Vec<String>,
    pub input: Option<Vec<u8>>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
}

struct ProcessIo {
    args: Vec<String>,
    env: HashMap<String, String>,
    cwd: PathBuf,
    stdin: Box<dyn VirtualFile + Send + Sync + 'static>,
    stdout: Box<dyn VirtualFile + Send + Sync + 'static>,
    stderr: Box<dyn VirtualFile + Send + Sync + 'static>,
}

#[derive(Clone, Debug)]
pub struct FileSystemEvent {
    pub sequence: u64,
    pub kind: FileSystemEventKind,
    pub path: String,
    pub target_path: Option<String>,
    pub dropped_count: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum FileSystemEventKind {
    FileCreated,
    FileModified,
    FileMetadataModified,
    FileRemoved,
    DirectoryCreated,
    DirectoryRemoved,
    PathRenamed,
    EventsDropped,
}

#[derive(Clone, Debug)]
pub struct EventBus {
    inner: Arc<EventBusInner>,
}

#[derive(Debug)]
struct EventBusInner {
    sender: tokio::sync::mpsc::Sender<FileSystemEvent>,
    enabled: AtomicBool,
    sequence: AtomicU64,
    dropped_count: AtomicU64,
}

#[derive(Clone, Debug)]
struct ReadOnlyFileSystem {
    inner: Arc<dyn FileSystem + Send + Sync>,
}

#[derive(Debug)]
struct ReadOnlyVirtualFile {
    inner: Box<dyn VirtualFile + Send + Sync + 'static>,
}

#[derive(Clone, Debug)]
struct ObservableFileSystem {
    inner: Arc<dyn FileSystem + Send + Sync>,
    events: EventBus,
}

#[derive(Debug)]
struct ObservableVirtualFile {
    inner: Box<dyn VirtualFile + Send + Sync + 'static>,
    events: EventBus,
    path: String,
}

#[derive(Debug)]
struct RelativeOrAbsolutePathHack<F>(F);

impl FileSystemEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FileCreated => "file_created",
            Self::FileModified => "file_modified",
            Self::FileMetadataModified => "file_metadata_modified",
            Self::FileRemoved => "file_removed",
            Self::DirectoryCreated => "directory_created",
            Self::DirectoryRemoved => "directory_removed",
            Self::PathRenamed => "path_renamed",
            Self::EventsDropped => "events_dropped",
        }
    }
}

impl EventBus {
    pub fn new(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<FileSystemEvent>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (
            Self {
                inner: Arc::new(EventBusInner {
                    sender,
                    enabled: AtomicBool::new(false),
                    sequence: AtomicU64::new(0),
                    dropped_count: AtomicU64::new(0),
                }),
            },
            receiver,
        )
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.inner.enabled.store(enabled, Ordering::Release);
        if enabled {
            return;
        }
        self.inner.dropped_count.store(0, Ordering::Release);
    }

    fn emit(&self, kind: FileSystemEventKind, path: String, target_path: Option<String>) {
        if !self.inner.enabled.load(Ordering::Acquire) {
            return;
        }

        let dropped_count = self.inner.dropped_count.swap(0, Ordering::AcqRel);
        if dropped_count > 0 {
            let dropped_event = self.event(
                FileSystemEventKind::EventsDropped,
                "/".to_string(),
                None,
                dropped_count,
            );
            if self.inner.sender.try_send(dropped_event).is_err() {
                self.inner
                    .dropped_count
                    .fetch_add(dropped_count.saturating_add(1), Ordering::AcqRel);
                return;
            }
        }

        let event = self.event(kind, path, target_path, 0);
        if self.inner.sender.try_send(event).is_ok() {
            return;
        }

        self.inner.dropped_count.fetch_add(1, Ordering::AcqRel);
    }

    fn event(
        &self,
        kind: FileSystemEventKind,
        path: String,
        target_path: Option<String>,
        dropped_count: u64,
    ) -> FileSystemEvent {
        FileSystemEvent {
            sequence: self.inner.sequence.fetch_add(1, Ordering::AcqRel) + 1,
            kind,
            path,
            target_path,
            dropped_count,
        }
    }
}

impl ReadOnlyFileSystem {
    fn new(inner: Arc<dyn FileSystem + Send + Sync>) -> Self {
        Self { inner }
    }
}

impl ObservableFileSystem {
    fn new(inner: Arc<dyn FileSystem + Send + Sync>, events: EventBus) -> Self {
        Self { inner, events }
    }
}

impl FileSystem for ReadOnlyFileSystem {
    fn readlink(&self, path: &Path) -> virtual_fs::Result<PathBuf> {
        self.inner.readlink(path)
    }

    fn read_dir(&self, path: &Path) -> virtual_fs::Result<virtual_fs::ReadDir> {
        self.inner.read_dir(path)
    }

    fn create_dir(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }

    fn remove_dir(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }

    fn rename<'a>(
        &'a self,
        _from: &'a Path,
        _to: &'a Path,
    ) -> Pin<Box<dyn Future<Output = virtual_fs::Result<()>> + Send + 'a>> {
        Box::pin(async { Err(FsError::PermissionDenied) })
    }

    fn metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.inner.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.inner.symlink_metadata(path)
    }

    fn remove_file(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }

    fn new_open_options(&self) -> virtual_fs::OpenOptions<'_> {
        virtual_fs::OpenOptions::new(self)
    }

    fn mount(
        &self,
        _name: String,
        _path: &Path,
        _fs: Box<dyn FileSystem + Send + Sync>,
    ) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }
}

impl FileOpener for ReadOnlyFileSystem {
    fn open(
        &self,
        path: &Path,
        config: &OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync + 'static>> {
        if config.create() || config.create_new() || config.append() || config.truncate() {
            return Err(FsError::PermissionDenied);
        }

        let mut read_config = config.clone();
        read_config.read = true;
        read_config.write = false;

        let mut options = self.inner.new_open_options();
        let file = options.options(read_config).open(path)?;
        Ok(Box::new(ReadOnlyVirtualFile { inner: file }))
    }
}

impl AsyncRead for ReadOnlyVirtualFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for ReadOnlyVirtualFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        Pin::new(&mut *self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut *self.inner).poll_complete(cx)
    }
}

impl AsyncWrite for ReadOnlyVirtualFile {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(read_only_mount_error()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(read_only_mount_error()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl VirtualFile for ReadOnlyVirtualFile {
    fn last_accessed(&self) -> u64 {
        self.inner.last_accessed()
    }

    fn last_modified(&self) -> u64 {
        self.inner.last_modified()
    }

    fn created_time(&self) -> u64 {
        self.inner.created_time()
    }

    fn set_times(&mut self, _atime: Option<u64>, _mtime: Option<u64>) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn set_len(&mut self, _new_size: u64) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    fn get_special_fd(&self) -> Option<u32> {
        self.inner.get_special_fd()
    }

    fn poll_read_ready(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<usize>> {
        let inner = self.get_mut();
        Pin::new(&mut *inner.inner).poll_read_ready(cx)
    }

    fn poll_write_ready(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(0))
    }
}

impl FileSystem for ObservableFileSystem {
    fn readlink(&self, path: &Path) -> virtual_fs::Result<PathBuf> {
        self.inner.readlink(path)
    }

    fn read_dir(&self, path: &Path) -> virtual_fs::Result<virtual_fs::ReadDir> {
        self.inner.read_dir(path)
    }

    fn create_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        let result = self.inner.create_dir(path);
        if result.is_ok() {
            self.events.emit(
                FileSystemEventKind::DirectoryCreated,
                event_path(path),
                None,
            );
        }
        result
    }

    fn remove_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        let result = self.inner.remove_dir(path);
        if result.is_ok() {
            self.events.emit(
                FileSystemEventKind::DirectoryRemoved,
                event_path(path),
                None,
            );
        }
        result
    }

    fn rename<'a>(
        &'a self,
        from: &'a Path,
        to: &'a Path,
    ) -> Pin<Box<dyn Future<Output = virtual_fs::Result<()>> + Send + 'a>> {
        let inner = Arc::clone(&self.inner);
        let events = self.events.clone();
        let from_path = from.to_path_buf();
        let to_path = to.to_path_buf();
        Box::pin(async move {
            inner.rename(&from_path, &to_path).await?;
            events.emit(
                FileSystemEventKind::PathRenamed,
                event_path(&from_path),
                Some(event_path(&to_path)),
            );
            Ok(())
        })
    }

    fn metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.inner.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.inner.symlink_metadata(path)
    }

    fn remove_file(&self, path: &Path) -> virtual_fs::Result<()> {
        let result = self.inner.remove_file(path);
        if result.is_ok() {
            self.events
                .emit(FileSystemEventKind::FileRemoved, event_path(path), None);
        }
        result
    }

    fn new_open_options(&self) -> virtual_fs::OpenOptions<'_> {
        virtual_fs::OpenOptions::new(self)
    }

    fn mount(
        &self,
        name: String,
        path: &Path,
        fs: Box<dyn FileSystem + Send + Sync>,
    ) -> virtual_fs::Result<()> {
        self.inner.mount(name, path, fs)
    }
}

impl FileOpener for ObservableFileSystem {
    fn open(
        &self,
        path: &Path,
        config: &OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync + 'static>> {
        let existed = self.inner.metadata(path).is_ok();
        let mut options = self.inner.new_open_options();
        let file = options.options(config.clone()).open(path)?;
        let path = event_path(path);

        if !existed && (config.create() || config.create_new()) {
            self.events
                .emit(FileSystemEventKind::FileCreated, path.clone(), None);
        } else if config.truncate() {
            self.events
                .emit(FileSystemEventKind::FileModified, path.clone(), None);
        }

        if config.would_mutate() {
            return Ok(Box::new(ObservableVirtualFile {
                inner: file,
                events: self.events.clone(),
                path,
            }));
        }

        Ok(file)
    }
}

impl AsyncRead for ObservableVirtualFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for ObservableVirtualFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        Pin::new(&mut *self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut *self.inner).poll_complete(cx)
    }
}

impl AsyncWrite for ObservableVirtualFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut *self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes_written)) => {
                if bytes_written > 0 {
                    self.events
                        .emit(FileSystemEventKind::FileModified, self.path.clone(), None);
                }
                Poll::Ready(Ok(bytes_written))
            }
            result => result,
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut *self.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(bytes_written)) => {
                if bytes_written > 0 {
                    self.events
                        .emit(FileSystemEventKind::FileModified, self.path.clone(), None);
                }
                Poll::Ready(Ok(bytes_written))
            }
            result => result,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

impl VirtualFile for ObservableVirtualFile {
    fn last_accessed(&self) -> u64 {
        self.inner.last_accessed()
    }

    fn last_modified(&self) -> u64 {
        self.inner.last_modified()
    }

    fn created_time(&self) -> u64 {
        self.inner.created_time()
    }

    fn set_times(&mut self, atime: Option<u64>, mtime: Option<u64>) -> virtual_fs::Result<()> {
        self.inner.set_times(atime, mtime)?;
        self.events.emit(
            FileSystemEventKind::FileMetadataModified,
            self.path.clone(),
            None,
        );
        Ok(())
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn set_len(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        self.inner.set_len(new_size)?;
        self.events
            .emit(FileSystemEventKind::FileModified, self.path.clone(), None);
        Ok(())
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        self.inner.unlink()?;
        self.events
            .emit(FileSystemEventKind::FileRemoved, self.path.clone(), None);
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    fn get_special_fd(&self) -> Option<u32> {
        self.inner.get_special_fd()
    }

    fn write_from_mmap(&mut self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.write_from_mmap(offset, len)?;
        self.events
            .emit(FileSystemEventKind::FileModified, self.path.clone(), None);
        Ok(())
    }

    fn poll_read_ready(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<usize>> {
        let inner = self.get_mut();
        Pin::new(&mut *inner.inner).poll_read_ready(cx)
    }

    fn poll_write_ready(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<usize>> {
        let inner = self.get_mut();
        Pin::new(&mut *inner.inner).poll_write_ready(cx)
    }
}

impl<F: FileSystem> RelativeOrAbsolutePathHack<F> {
    fn execute<Func, Ret>(&self, path: &Path, operation: Func) -> virtual_fs::Result<Ret>
    where
        Func: Fn(&F, &Path) -> virtual_fs::Result<Ret>,
    {
        let result = operation(&self.0, path);
        if result.is_err() && !path.is_absolute() {
            return operation(&self.0, &Path::new("/").join(path));
        }
        result
    }
}

impl<F: FileSystem> FileSystem for RelativeOrAbsolutePathHack<F> {
    fn readlink(&self, path: &Path) -> virtual_fs::Result<PathBuf> {
        self.execute(path, |fs, candidate| fs.readlink(candidate))
    }

    fn read_dir(&self, path: &Path) -> virtual_fs::Result<virtual_fs::ReadDir> {
        self.execute(path, |fs, candidate| fs.read_dir(candidate))
    }

    fn create_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        self.execute(path, |fs, candidate| fs.create_dir(candidate))
    }

    fn remove_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        self.execute(path, |fs, candidate| fs.remove_dir(candidate))
    }

    fn rename<'a>(
        &'a self,
        from: &'a Path,
        to: &'a Path,
    ) -> Pin<Box<dyn Future<Output = virtual_fs::Result<()>> + Send + 'a>> {
        Box::pin(async move { self.0.rename(from, to).await })
    }

    fn metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.execute(path, |fs, candidate| fs.metadata(candidate))
    }

    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        self.execute(path, |fs, candidate| fs.symlink_metadata(candidate))
    }

    fn remove_file(&self, path: &Path) -> virtual_fs::Result<()> {
        self.execute(path, |fs, candidate| fs.remove_file(candidate))
    }

    fn new_open_options(&self) -> virtual_fs::OpenOptions<'_> {
        virtual_fs::OpenOptions::new(self)
    }

    fn mount(
        &self,
        name: String,
        path: &Path,
        fs: Box<dyn FileSystem + Send + Sync>,
    ) -> virtual_fs::Result<()> {
        let fs = Arc::new(fs);
        self.execute(path, move |inner, candidate| {
            inner.mount(name.clone(), candidate, Box::new(Arc::clone(&fs)))
        })
    }
}

impl<F: FileSystem> FileOpener for RelativeOrAbsolutePathHack<F> {
    fn open(
        &self,
        path: &Path,
        config: &OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync + 'static>> {
        self.execute(path, |fs, candidate| {
            fs.new_open_options()
                .options(config.clone())
                .open(candidate)
        })
    }
}

impl SandboxState {
    pub fn new(
        files: HashMap<String, Option<Vec<u8>>>,
        host_mounts: Vec<HostMount>,
        cwd: String,
        env: HashMap<String, String>,
        asset_dir: String,
        limits: Limits,
        events: EventBus,
    ) -> Result<Self> {
        let catalog = catalog_for(asset_dir)?;
        let fs = TmpFileSystem::new();
        create_default_layout(&catalog, &fs)?;
        let cwd = normalize_path(&cwd)?;
        let cwd = cwd
            .to_str()
            .ok_or_else(|| anyhow!("sandbox cwd must be valid UTF-8"))?
            .to_string();
        let mut sandbox_env = default_env();
        sandbox_env.extend(env);

        let state = Self {
            fs,
            cwd,
            env: sandbox_env,
            limits,
            catalog,
            events,
        };

        for (path, contents) in files {
            match contents {
                Some(data) => state.write_file_silent_blocking(&path, data)?,
                None => state.create_directory_silent(&path)?,
            }
        }

        for mount in host_mounts {
            state.mount_host(mount)?;
        }

        Ok(state)
    }

    pub fn exists(&self, path: &str) -> Result<bool> {
        let path = normalize_path(path)?;
        Ok(self.fs.metadata(&path).is_ok())
    }

    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let path = normalize_path(path)?;
        read_file_from_fs(&self.fs, &path)
            .await
            .with_context(|| format!("unable to read {}", path.display()))
    }

    pub async fn write_file(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let path = normalize_path(path)?;
        let existed = self.fs.metadata(&path).is_ok();
        self.create_parent_directories(&path)?;
        write_file_to_fs(&self.fs, &path, data)
            .await
            .with_context(|| format!("unable to write {}", path.display()))?;
        let kind = if existed {
            FileSystemEventKind::FileModified
        } else {
            FileSystemEventKind::FileCreated
        };
        self.events.emit(kind, event_path(&path), None);
        Ok(())
    }

    fn write_file_silent_blocking(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.catalog.block_on(self.write_file_silent(path, data))
    }

    async fn write_file_silent(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let path = normalize_path(path)?;
        create_parent_directories(&self.fs, &path)?;
        write_file_to_fs(&self.fs, &path, data)
            .await
            .with_context(|| format!("unable to write {}", path.display()))
    }

    fn create_directory_silent(&self, path: &str) -> Result<()> {
        let path = normalize_path(path)?;
        create_dir_all(&self.fs, &path)
            .with_context(|| format!("unable to create {}", path.display()))
    }

    fn create_parent_directories(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            for created_path in create_directories(&self.fs, parent)? {
                self.events.emit(
                    FileSystemEventKind::DirectoryCreated,
                    event_path(&created_path),
                    None,
                );
            }
        }
        Ok(())
    }

    pub fn mount_host(&self, mount: HostMount) -> Result<()> {
        let target = normalize_path(&mount.target)?;
        if target == Path::new("/") {
            return Err(anyhow!("host mount target cannot be the sandbox root"));
        }

        create_parent_directories(&self.fs, &target)?;
        if let Ok(metadata) = self.fs.metadata(&target) {
            if !metadata.is_dir() {
                return Err(anyhow!(
                    "host mount target is not a directory: {}",
                    target.display()
                ));
            }
        }

        let source = validate_host_mount_source(&mount.source)?;
        let host_fs = host_fs::FileSystem::new(self.catalog.handle.clone(), source.clone())
            .with_context(|| format!("unable to mount host source {}", source.display()))?;
        let host_fs: Arc<dyn FileSystem + Send + Sync> = Arc::new(host_fs);
        let mounted_fs: Arc<dyn FileSystem + Send + Sync> = if mount.read_only {
            Arc::new(ReadOnlyFileSystem::new(host_fs))
        } else {
            host_fs
        };

        self.fs
            .mount(target.clone(), &mounted_fs, PathBuf::from("/"))
            .with_context(|| format!("unable to mount host source at {}", target.display()))
    }

    pub fn listdir(&self, path: &str) -> Result<Vec<String>> {
        let path = normalize_path(path)?;
        let mut names = self
            .fs
            .read_dir(&path)
            .with_context(|| format!("unable to list {}", path.display()))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }

    pub fn run_blocking(&self, request: RunRequest) -> Result<CompletedProcess> {
        self.catalog.run(self, request)
    }
}

impl PackageCatalog {
    fn load(asset_dir: PathBuf) -> Result<Arc<Self>> {
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("unix-sandbox-wasmer")
            .build()
            .context("unable to create Wasmer runtime")?;
        let handle = tokio_runtime.handle().clone();
        let task_manager = Arc::new(TokioTaskManager::new(tokio_runtime));
        let _runtime_guard = handle.enter();
        let mut runtime = PluggableRuntime::new(task_manager);
        runtime.set_engine(sandbox_engine());
        runtime.set_package_loader(BuiltinPackageLoader::new());
        runtime.set_source(
            InMemorySource::from_directory_tree(&asset_dir)
                .with_context(|| format!("unable to index assets in {}", asset_dir.display()))?,
        );

        let runtime = Arc::new(runtime);
        let mut packages = HashMap::new();
        let mut command_paths = HashMap::new();

        for package_name in STANDARD_PACKAGE_NAMES {
            let package = load_package(
                &handle,
                runtime.as_ref(),
                &asset_dir.join(format!("{package_name}.webc")),
            )?;
            register_package(package_name, &package, &mut command_paths);
            packages.insert((*package_name).to_string(), Arc::new(package));
        }

        register_command_alias("python3", "python", "python", &mut command_paths);

        Ok(Arc::new(Self {
            runtime,
            handle,
            packages,
            command_paths,
        }))
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    fn run(&self, state: &SandboxState, request: RunRequest) -> Result<CompletedProcess> {
        if request.args.is_empty() {
            return Err(anyhow!("command arguments cannot be empty"));
        }

        let mut env = state.env.clone();
        if let Some(overrides) = request.env {
            env.extend(overrides);
        }

        let cwd = request.cwd.unwrap_or_else(|| state.cwd.clone());
        let cwd = normalize_path(&cwd)?;
        validate_directory(&state.fs, &cwd, "cwd")?;
        let target = self.resolve_command(&request.args[0], &cwd, env.get("PATH"))?;
        let package = self
            .packages
            .get(&target.package)
            .ok_or_else(|| anyhow!("package not loaded: {}", target.package))?;

        let stdout = CapturedOutput::new(state.limits.output_bytes);
        let stderr = CapturedOutput::new(state.limits.output_bytes);
        let stdin = StaticFile::new(request.input.unwrap_or_default());

        let injected_packages = self.injected_packages(&target.package);

        let wall_time = match state.limits.wall_time_seconds {
            Some(seconds) => Some(duration_from_seconds(seconds)?),
            None => None,
        };
        let run_result = self.run_package_command(
            ProcessIo {
                args: request.args.iter().skip(1).cloned().collect(),
                env,
                cwd,
                stdin: Box::new(stdin),
                stdout: Box::new(stdout.file()),
                stderr: Box::new(stderr.file()),
            },
            &target.command,
            package,
            injected_packages,
            state.fs.clone(),
            state.events.clone(),
            wall_time,
        );

        let stdout = stdout.capture("stdout")?;
        let stderr = stderr.capture("stderr")?;
        let returncode = run_result?;
        let (returncode, stderr) = normalize_process_outcome(&target.command, returncode, stderr);

        Ok(CompletedProcess {
            args: request.args,
            returncode,
            stdout,
            stderr,
        })
    }

    fn resolve_command(
        &self,
        command: &str,
        cwd: &Path,
        path_env: Option<&String>,
    ) -> Result<CommandTarget> {
        if command.as_bytes().contains(&0) {
            return Err(anyhow!("command cannot contain NUL bytes"));
        }
        if command.is_empty() {
            return Err(anyhow!("command cannot be empty"));
        }

        if command.contains('/') {
            let path = normalize_command_path(command, cwd)?;
            return self
                .command_paths
                .get(&path)
                .cloned()
                .ok_or_else(|| anyhow!("command not found: {command}"));
        }

        self.resolve_path_command(command, cwd, path_env)?
            .ok_or_else(|| anyhow!("command not found: {command}"))
    }

    fn resolve_path_command(
        &self,
        command: &str,
        cwd: &Path,
        path_env: Option<&String>,
    ) -> Result<Option<CommandTarget>> {
        for directory in path_env.map_or("", String::as_str).split(':') {
            let candidate = command_path_from_path_entry(directory, command, cwd)?;
            if let Some(target) = self.command_paths.get(&candidate) {
                return Ok(Some(target.clone()));
            }
        }
        Ok(None)
    }

    fn injected_packages(&self, target_package: &str) -> Vec<BinaryPackage> {
        self.packages
            .iter()
            .filter_map(|(name, package)| {
                if name == target_package {
                    return None;
                }
                Some((**package).clone())
            })
            .collect()
    }

    fn run_package_command(
        &self,
        io: ProcessIo,
        command_name: &str,
        package: &BinaryPackage,
        injected_packages: Vec<BinaryPackage>,
        root_fs: TmpFileSystem,
        events: EventBus,
        wall_time: Option<Duration>,
    ) -> Result<i32> {
        let command = package
            .get_command(command_name)
            .with_context(|| format!("package does not contain command {command_name}"))?;
        let wasi = command
            .metadata()
            .annotation("wasi")?
            .unwrap_or_else(|| Wasi::new(command_name));
        let exec_name = wasi.exec_name.as_deref().unwrap_or(command_name);
        let mut builder = WasiEnvBuilder::new(exec_name);
        builder.set_runtime(Arc::clone(&self.runtime));
        builder.set_module_hash(package.hash());
        builder.add_webc(package.clone());
        builder.include_packages(package.package_ids.clone());

        let package_files = process_package_files(package, &injected_packages)?;
        for injected_package in injected_packages {
            builder.add_webc(injected_package.clone());
            builder.include_packages(injected_package.package_ids.clone());
        }

        builder.set_current_dir(io.cwd.clone());
        if let Some(package_cwd) = &wasi.cwd {
            builder.set_current_dir(package_cwd);
        }

        if let Some(main_args) = &wasi.main_args {
            builder.add_args(main_args);
        }
        builder.add_args(io.args);

        for item in wasi.env.as_deref().unwrap_or_default() {
            match item.split_once('=') {
                Some((key, value)) => builder.add_env(key, value),
                None => builder.add_env(item, String::new()),
            }
        }
        builder.add_envs(io.env);

        let current_dir = builder.get_current_dir().unwrap_or(PathBuf::from("/"));
        builder.add_map_dir(".", current_dir)?;
        builder.add_preopen_dir("/")?;
        builder.set_fs(process_filesystem(root_fs, package_files, events));
        builder.set_stdin(io.stdin);
        builder.set_stdout(io.stdout);
        builder.set_stderr(io.stderr);

        let env = builder.build()?;
        let runtime = env.runtime.clone();
        let process = env.process.clone();
        let tasks = runtime.task_manager().clone();
        let package = package.clone();
        let command_name = command_name.to_string();

        let exit_code = tasks.spawn_and_block_on(async move {
            let run = async move {
                let mut task_handle = spawn_exec(package, &command_name, env, &runtime)
                    .await
                    .context("spawn failed")?;
                let exit_code = task_handle
                    .wait_finished()
                    .await
                    .map_err(|error| anyhow!(error.to_string()))?;
                Ok::<_, anyhow::Error>(exit_code)
            };

            let exit_code = match wall_time {
                Some(timeout) => match tokio::time::timeout(timeout, run).await {
                    Ok(result) => result?,
                    Err(_) => {
                        process.signal_process(Signal::Sigkill);
                        return Err(anyhow!(
                            "process exceeded wall time limit of {:.3} seconds",
                            timeout.as_secs_f64()
                        ));
                    }
                },
                None => run.await?,
            };
            Ok::<_, anyhow::Error>(exit_code)
        })??;

        Ok(exit_code.raw())
    }
}

fn process_package_files(
    package: &BinaryPackage,
    injected_packages: &[BinaryPackage],
) -> Result<Option<UnionFileSystem>> {
    let mut package_files = package.webc_fs.as_deref().map(UnionFileSystem::duplicate);
    for injected_package in injected_packages {
        let Some(injected_files) = injected_package.webc_fs.as_deref() else {
            continue;
        };
        match &mut package_files {
            Some(files) => files.merge(injected_files, UnionMergeMode::Skip)?,
            None => package_files = Some(injected_files.duplicate()),
        }
    }
    Ok(package_files)
}

fn process_filesystem(
    root_fs: TmpFileSystem,
    package_files: Option<UnionFileSystem>,
    events: EventBus,
) -> Arc<dyn FileSystem + Send + Sync> {
    match package_files {
        Some(files) => {
            let overlay = OverlayFileSystem::new(root_fs, [RelativeOrAbsolutePathHack(files)]);
            Arc::new(ObservableFileSystem::new(Arc::new(overlay), events))
        }
        None => Arc::new(ObservableFileSystem::new(Arc::new(root_fs), events)),
    }
}

#[derive(Clone, Debug)]
struct CapturedOutput {
    state: Arc<Mutex<CapturedOutputState>>,
}

#[derive(Debug)]
struct CapturedOutputState {
    data: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

#[derive(Debug)]
struct LimitedCaptureFile {
    state: Arc<Mutex<CapturedOutputState>>,
    cursor: u64,
}

impl CapturedOutput {
    fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(CapturedOutputState {
                data: Vec::new(),
                limit,
                exceeded: false,
            })),
        }
    }

    fn file(&self) -> LimitedCaptureFile {
        LimitedCaptureFile {
            state: Arc::clone(&self.state),
            cursor: 0,
        }
    }

    fn capture(&self, stream_name: &str) -> Result<Vec<u8>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("captured {stream_name} lock failed"))?;
        if state.exceeded {
            return Err(anyhow!(
                "process {stream_name} output exceeded {} bytes",
                state.limit
            ));
        }
        Ok(state.data.clone())
    }
}

impl LimitedCaptureFile {
    fn write_limited(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("captured output lock failed"))?;
        if state.exceeded {
            return Err(output_limit_error(state.limit));
        }

        let available = state.limit.saturating_sub(state.data.len());
        if available == 0 {
            state.exceeded = true;
            return Err(output_limit_error(state.limit));
        }

        let write_len = available.min(buf.len());
        state.data.extend_from_slice(&buf[..write_len]);
        self.cursor = state.data.len() as u64;

        if write_len < buf.len() {
            state.exceeded = true;
        }

        Ok(write_len)
    }
}

impl AsyncSeek for LimitedCaptureFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("captured output lock failed"))?;
        let len = state.data.len() as i128;
        let current = self.cursor as i128;
        let next = match position {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => len + offset as i128,
            SeekFrom::Current(offset) => current + offset as i128,
        };
        if next < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek before start",
            ));
        }
        drop(state);
        self.cursor = next as u64;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.cursor))
    }
}

impl AsyncWrite for LimitedCaptureFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.write_limited(buf))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        for candidate in bufs {
            if !candidate.is_empty() {
                return Poll::Ready(self.write_limited(candidate));
            }
        }
        Poll::Ready(self.write_limited(&[]))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for LimitedCaptureFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(io::Error::other("captured output lock failed"))),
        };
        let start = (self.cursor as usize).min(state.data.len());
        let available = &state.data[start..];
        let read_len = available.len().min(buf.remaining());
        buf.put_slice(&available[..read_len]);
        drop(state);
        self.cursor += read_len as u64;
        Poll::Ready(Ok(()))
    }
}

impl VirtualFile for LimitedCaptureFile {
    fn last_accessed(&self) -> u64 {
        1_000_000_000
    }

    fn last_modified(&self) -> u64 {
        1_000_000_000
    }

    fn created_time(&self) -> u64 {
        1_000_000_000
    }

    fn size(&self) -> u64 {
        self.state
            .lock()
            .map(|state| state.data.len() as u64)
            .unwrap_or_default()
    }

    fn set_len(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        let mut state = self.state.lock().map_err(|_| FsError::Lock)?;
        if new_size > state.limit as u64 {
            state.exceeded = true;
            return Err(FsError::StorageFull);
        }
        state.data.resize(new_size as usize, 0);
        self.cursor = self.cursor.min(new_size);
        Ok(())
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        Ok(())
    }

    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<usize>> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(io::Error::other("captured output lock failed"))),
        };
        let remaining = state.data.len().saturating_sub(self.cursor as usize);
        Poll::Ready(Ok(remaining))
    }

    fn poll_write_ready(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<io::Result<usize>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(io::Error::other("captured output lock failed"))),
        };
        if state.exceeded {
            return Poll::Ready(Err(output_limit_error(state.limit)));
        }
        let remaining = state.limit.saturating_sub(state.data.len());
        if remaining == 0 {
            state.exceeded = true;
            return Poll::Ready(Err(output_limit_error(state.limit)));
        }
        Poll::Ready(Ok(remaining.min(8192)))
    }
}

fn output_limit_error(limit: usize) -> io::Error {
    io::Error::other(format!("process output exceeded {limit} bytes"))
}

fn read_only_mount_error() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "read-only host mount")
}

fn normalize_process_outcome(command: &str, returncode: i32, stderr: Vec<u8>) -> (i32, Vec<u8>) {
    const FIND_CWD_RESTORE_ERROR: &[u8] =
        b"(null): Failed to restore initial working directory: Not a directory\n";

    if command == "find" && returncode == 1 && stderr == FIND_CWD_RESTORE_ERROR {
        return (0, Vec::new());
    }

    (returncode, stderr)
}

fn catalog_for(asset_dir: String) -> Result<Arc<PackageCatalog>> {
    let asset_dir = PathBuf::from(asset_dir)
        .canonicalize()
        .context("unable to resolve asset directory")?;

    if let Some(catalog) = CATALOGS
        .lock()
        .map_err(|_| anyhow!("package catalog lock failed"))?
        .get(&asset_dir)
        .cloned()
    {
        return Ok(catalog);
    }

    let catalog = PackageCatalog::load(asset_dir.clone())?;
    CATALOGS
        .lock()
        .map_err(|_| anyhow!("package catalog lock failed"))?
        .insert(asset_dir, Arc::clone(&catalog));
    Ok(catalog)
}

fn load_package(
    handle: &tokio::runtime::Handle,
    runtime: &(dyn Runtime + Send + Sync),
    path: &Path,
) -> Result<BinaryPackage> {
    let data = std::fs::read(path).with_context(|| format!("unable to read {}", path.display()))?;
    let container =
        from_bytes(data).with_context(|| format!("unable to parse {}", path.display()))?;
    handle
        .block_on(BinaryPackage::from_webc(&container, runtime))
        .with_context(|| format!("unable to load {}", path.display()))
}

fn register_package(
    name: &str,
    package: &BinaryPackage,
    command_paths: &mut HashMap<PathBuf, CommandTarget>,
) {
    for command in &package.commands {
        register_command_alias(command.name(), name, command.name(), command_paths);
    }
}

fn register_command_alias(
    alias: &str,
    package: &str,
    command: &str,
    command_paths: &mut HashMap<PathBuf, CommandTarget>,
) {
    let target = CommandTarget {
        package: package.to_string(),
        command: command.to_string(),
    };
    for prefix in COMMAND_PATH_PREFIXES {
        command_paths.insert(Path::new(prefix).join(alias), target.clone());
    }
}

fn sandbox_engine() -> wasmer::Engine {
    let mut features = Features::default();
    features.exceptions(true);

    let mut engine: wasmer::Engine = EngineBuilder::new(Cranelift::default())
        .set_features(Some(features))
        .into();
    let tunables = BaseTunables::for_target(engine.target());
    engine.set_tunables(tunables);
    engine
}

fn duration_from_seconds(seconds: f64) -> Result<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(anyhow!(
            "wall_time_seconds must be a positive finite number"
        ));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn create_default_layout(catalog: &PackageCatalog, fs: &TmpFileSystem) -> Result<()> {
    for path in [
        "/bin",
        "/usr",
        "/usr/bin",
        "/dev",
        "/tmp",
        "/work",
        "/home",
        "/home/sandbox",
        "/etc",
    ] {
        create_dir_all(fs, Path::new(path)).with_context(|| format!("unable to create {path}"))?;
    }
    catalog.block_on(write_file_to_fs(
        fs,
        Path::new("/etc/passwd"),
        b"sandbox:x:1000:1000:Sandbox User:/home/sandbox:/bin/sh\n".to_vec(),
    ))?;
    catalog.block_on(write_file_to_fs(
        fs,
        Path::new("/etc/group"),
        b"sandbox:x:1000:\n".to_vec(),
    ))?;
    fs.new_open_options_ext()
        .insert_device_file(PathBuf::from("/dev/null"), Box::<NullFile>::default())
        .context("unable to create /dev/null")?;
    Ok(())
}

fn default_env() -> HashMap<String, String> {
    HashMap::from([
        ("HOME".to_string(), "/home/sandbox".to_string()),
        ("LANG".to_string(), "C.UTF-8".to_string()),
        ("LOGNAME".to_string(), "sandbox".to_string()),
        ("PATH".to_string(), "/bin:/usr/bin".to_string()),
        ("TMPDIR".to_string(), "/tmp".to_string()),
        ("USER".to_string(), "sandbox".to_string()),
    ])
}

fn command_path_from_path_entry(directory: &str, command: &str, cwd: &Path) -> Result<PathBuf> {
    let command_path = if directory.is_empty() || directory == "." {
        cwd.join(command)
    } else {
        Path::new(directory).join(command)
    };
    let command_path = command_path
        .to_str()
        .ok_or_else(|| anyhow!("command path must be valid UTF-8"))?;

    if command_path.starts_with('/') {
        return normalize_path(command_path);
    }

    normalize_command_path(command_path, cwd)
}

fn validate_directory(fs: &TmpFileSystem, path: &Path, name: &str) -> Result<()> {
    let metadata = match fs.metadata(path) {
        Ok(metadata) => metadata,
        Err(FsError::EntryNotFound) => {
            return Err(anyhow!("{name} does not exist: {}", path.display()));
        }
        Err(FsError::BaseNotDirectory | FsError::NotAFile) => {
            return Err(anyhow!("{name} is not a directory: {}", path.display()));
        }
        Err(error) => {
            return Err(anyhow!(error))
                .with_context(|| format!("unable to inspect {name}: {}", path.display()));
        }
    };
    if metadata.is_dir() {
        return Ok(());
    }
    Err(anyhow!("{name} is not a directory: {}", path.display()))
}

fn validate_host_mount_source(path: &str) -> Result<PathBuf> {
    if path.as_bytes().contains(&0) {
        return Err(anyhow!("host mount source cannot contain NUL bytes"));
    }

    let path = PathBuf::from(path);
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(anyhow!(
                "host mount source does not exist: {}",
                path.display()
            ));
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("unable to inspect host mount source {}", path.display())
            });
        }
    };
    if !metadata.is_dir() {
        return Err(anyhow!(
            "host mount source is not a directory: {}",
            path.display()
        ));
    }

    path.canonicalize()
        .with_context(|| format!("unable to resolve host mount source {}", path.display()))
}

fn create_parent_directories(fs: &TmpFileSystem, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(fs, parent)
            .with_context(|| format!("unable to create {}", parent.display()))?;
    }
    Ok(())
}

fn create_directories(fs: &TmpFileSystem, path: &Path) -> Result<Vec<PathBuf>> {
    let mut current = PathBuf::from("/");
    let mut created_paths = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        match fs.metadata(&current) {
            Ok(metadata) if metadata.is_dir() => continue,
            Ok(_) => {
                return Err(anyhow!("path is not a directory: {}", current.display()));
            }
            Err(FsError::EntryNotFound) => {}
            Err(error) => {
                return Err(anyhow!(error))
                    .with_context(|| format!("unable to inspect {}", current.display()));
            }
        }
        fs.create_dir(&current)
            .with_context(|| format!("unable to create {}", current.display()))?;
        created_paths.push(current.clone());
    }
    Ok(created_paths)
}

async fn read_file_from_fs(fs: &TmpFileSystem, path: &Path) -> Result<Vec<u8>> {
    let mut file = fs.new_open_options().read(true).open(path)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await?;
    Ok(contents)
}

async fn write_file_to_fs(fs: &TmpFileSystem, path: &Path, data: Vec<u8>) -> Result<()> {
    let mut file = fs
        .new_open_options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(&data).await?;
    file.flush().await?;
    Ok(())
}

fn normalize_path(path: &str) -> Result<PathBuf> {
    if path.as_bytes().contains(&0) {
        return Err(anyhow!("sandbox paths cannot contain NUL bytes"));
    }
    if !path.starts_with('/') {
        return Err(anyhow!("sandbox paths must be absolute"));
    }

    let mut normalized = PathBuf::from("/");
    for component in path.split('/') {
        if component.is_empty() || component == "." {
            continue;
        }
        if component == ".." {
            if !normalized.pop() {
                return Err(anyhow!("sandbox paths cannot escape root"));
            }
            continue;
        }
        normalized.push(component);
    }

    Ok(normalized)
}

fn event_path(path: &Path) -> String {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new("/").join(path)
    };
    let Some(path) = absolute.to_str() else {
        return absolute.to_string_lossy().to_string();
    };
    match normalize_path(path) {
        Ok(normalized) => normalized.to_string_lossy().to_string(),
        Err(_) => absolute.to_string_lossy().to_string(),
    }
}

fn normalize_command_path(command: &str, cwd: &Path) -> Result<PathBuf> {
    if command.starts_with('/') {
        return normalize_path(command);
    }

    let mut path = cwd.to_path_buf();
    for component in command.split('/') {
        if component.is_empty() || component == "." {
            continue;
        }
        if component == ".." {
            if !path.pop() {
                return Err(anyhow!("command paths cannot escape root"));
            }
            continue;
        }
        path.push(component);
    }

    normalize_path(
        path.to_str()
            .ok_or_else(|| anyhow!("command path must be valid UTF-8"))?,
    )
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn limited_capture_stops_storing_at_limit() {
        let captured = CapturedOutput::new(4);
        let mut file = captured.file();

        file.write_all(b"abcdef").await.unwrap_err();

        let state = captured.state.lock().expect("capture state should lock");
        assert_eq!(state.data, b"abcd");
        assert!(state.exceeded);
    }

    #[test]
    fn command_path_normalization_uses_cwd_for_relative_paths() {
        let path = normalize_command_path("./tool", Path::new("/work")).unwrap();
        assert_eq!(path, PathBuf::from("/work/tool"));
    }

    #[test]
    fn path_entries_resolve_like_sandbox_paths() {
        let absolute = command_path_from_path_entry("/bin", "cat", Path::new("/work")).unwrap();
        assert_eq!(absolute, PathBuf::from("/bin/cat"));

        let current = command_path_from_path_entry("", "cat", Path::new("/bin")).unwrap();
        assert_eq!(current, PathBuf::from("/bin/cat"));

        let relative = command_path_from_path_entry("usr/bin", "cat", Path::new("/")).unwrap();
        assert_eq!(relative, PathBuf::from("/usr/bin/cat"));
    }

    #[test]
    fn find_cleanup_error_is_normalized_only_when_exact() {
        let cleanup_error =
            b"(null): Failed to restore initial working directory: Not a directory\n".to_vec();
        let (returncode, stderr) = normalize_process_outcome("find", 1, cleanup_error);
        assert_eq!(returncode, 0);
        assert_eq!(stderr, b"");

        let real_error = b"find: missing argument\n(null): Failed to restore initial working directory: Not a directory\n"
            .to_vec();
        let (returncode, stderr) = normalize_process_outcome("find", 1, real_error);
        assert_eq!(returncode, 1);
        assert!(stderr.starts_with(b"find: missing argument"));
    }
}
