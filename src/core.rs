use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use virtual_fs::{create_dir_all, ArcBoxFile, BufferFile, FileSystem, StaticFile, TmpFileSystem};
use wasmer::sys::{BaseTunables, Cranelift, EngineBuilder, Features, NativeEngineExt};
use wasmer_package::utils::from_bytes;
use wasmer_wasix::{
    bin_factory::{spawn_exec, BinaryPackage},
    runners::wasi::{PackageOrHash, RuntimeOrEngine, WasiRunner},
    runtime::{
        package_loader::BuiltinPackageLoader,
        resolver::InMemorySource,
        task_manager::{tokio::TokioTaskManager, VirtualTaskManagerExt},
    },
    PluggableRuntime, Runtime,
};
use webc::metadata::annotations::Wasi;

static CATALOGS: Lazy<Mutex<HashMap<PathBuf, Arc<PackageCatalog>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
pub struct Limits {
    pub output_bytes: usize,
    pub wall_time_seconds: Option<f64>,
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
    commands: HashMap<String, CommandTarget>,
}

pub struct RunRequest {
    pub args: Vec<String>,
    pub input: Option<Vec<u8>>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
}

impl SandboxState {
    pub fn new(
        files: HashMap<String, Option<Vec<u8>>>,
        cwd: String,
        env: HashMap<String, String>,
        asset_dir: String,
        limits: Limits,
    ) -> Result<Self> {
        let catalog = catalog_for(asset_dir)?;
        let fs = TmpFileSystem::new();
        create_default_layout(&catalog, &fs)?;

        let state = Self {
            fs,
            cwd,
            env,
            limits,
            catalog,
        };

        for (path, contents) in files {
            match contents {
                Some(data) => state.write_file_blocking(&path, data)?,
                None => state.create_directory(&path)?,
            }
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
        create_parent_directories(&self.fs, &path)?;
        write_file_to_fs(&self.fs, &path, data)
            .await
            .with_context(|| format!("unable to write {}", path.display()))
    }

    pub fn write_file_blocking(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.catalog.block_on(self.write_file(path, data))
    }

    pub fn create_directory(&self, path: &str) -> Result<()> {
        let path = normalize_path(path)?;
        create_dir_all(&self.fs, &path)
            .with_context(|| format!("unable to create {}", path.display()))
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
        let mut commands = HashMap::new();

        let coreutils = load_package(&handle, runtime.as_ref(), &asset_dir.join("coreutils.webc"))?;
        register_package("coreutils", &coreutils, &mut commands);
        packages.insert("coreutils".to_string(), Arc::new(coreutils));

        let python = load_package(&handle, runtime.as_ref(), &asset_dir.join("python.webc"))?;
        register_package("python", &python, &mut commands);
        commands.insert(
            "python3".to_string(),
            CommandTarget {
                package: "python".to_string(),
                command: "python".to_string(),
            },
        );
        packages.insert("python".to_string(), Arc::new(python));

        Ok(Arc::new(Self {
            runtime,
            handle,
            packages,
            commands,
        }))
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    fn run(&self, state: &SandboxState, request: RunRequest) -> Result<CompletedProcess> {
        let _wall_time_seconds = state.limits.wall_time_seconds;
        if request.args.is_empty() {
            return Err(anyhow!("command arguments cannot be empty"));
        }

        let executable = command_name(&request.args[0])?;
        let target = self
            .commands
            .get(&executable)
            .ok_or_else(|| anyhow!("command not found: {executable}"))?
            .clone();
        let package = self
            .packages
            .get(&target.package)
            .ok_or_else(|| anyhow!("package not loaded: {}", target.package))?;
        let cwd = request.cwd.unwrap_or_else(|| state.cwd.clone());
        let cwd = normalize_path(&cwd)?;

        let mut env = state.env.clone();
        if let Some(overrides) = request.env {
            env.extend(overrides);
        }

        let stdout = ArcBoxFile::new(Box::new(BufferFile::default()));
        let stderr = ArcBoxFile::new(Box::new(BufferFile::default()));
        let stdin = StaticFile::new(request.input.unwrap_or_default());

        let mut runner = WasiRunner::new();
        runner.with_args(request.args.iter().skip(1).cloned());
        runner.with_envs(env);
        runner.with_current_dir(cwd);
        runner.with_stdin(Box::new(stdin));
        runner.with_stdout(Box::new(stdout.clone()));
        runner.with_stderr(Box::new(stderr.clone()));

        let returncode =
            self.run_package_command(&mut runner, &target.command, package, state.fs.clone())?;

        let stdout = self.capture(stdout)?;
        let stderr = self.capture(stderr)?;
        if stdout.len() > state.limits.output_bytes || stderr.len() > state.limits.output_bytes {
            return Err(anyhow!(
                "process output exceeded {} bytes",
                state.limits.output_bytes
            ));
        }

        Ok(CompletedProcess {
            args: request.args,
            returncode,
            stdout,
            stderr,
        })
    }

    fn capture(&self, mut file: ArcBoxFile) -> Result<Vec<u8>> {
        self.block_on(async move {
            file.seek(SeekFrom::Start(0))
                .await
                .context("unable to rewind captured output")?;
            let mut output = Vec::new();
            file.read_to_end(&mut output)
                .await
                .context("unable to read captured output")?;
            Ok(output)
        })
    }

    fn run_package_command(
        &self,
        runner: &mut WasiRunner,
        command_name: &str,
        package: &BinaryPackage,
        root_fs: TmpFileSystem,
    ) -> Result<i32> {
        let command = package
            .get_command(command_name)
            .with_context(|| format!("package does not contain command {command_name}"))?;
        let wasi = command
            .metadata()
            .annotation("wasi")?
            .unwrap_or_else(|| Wasi::new(command_name));
        let exec_name = wasi.exec_name.as_deref().unwrap_or(command_name);
        let builder = runner.prepare_webc_env(
            exec_name,
            &wasi,
            PackageOrHash::Package(package),
            RuntimeOrEngine::Runtime(Arc::clone(&self.runtime)),
            Some(root_fs),
        )?;
        let env = builder.build()?;
        let runtime = env.runtime.clone();
        let tasks = runtime.task_manager().clone();
        let package = package.clone();
        let command_name = command_name.to_string();

        let exit_code = tasks.spawn_and_block_on(async move {
            let mut task_handle = spawn_exec(package, &command_name, env, &runtime)
                .await
                .context("spawn failed")?;
            let exit_code = task_handle
                .wait_finished()
                .await
                .map_err(|error| anyhow!(error.to_string()))?;
            Ok::<_, anyhow::Error>(exit_code)
        })??;

        Ok(exit_code.raw())
    }
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
    commands: &mut HashMap<String, CommandTarget>,
) {
    for command in &package.commands {
        commands.insert(
            command.name().to_string(),
            CommandTarget {
                package: name.to_string(),
                command: command.name().to_string(),
            },
        );
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

fn create_default_layout(catalog: &PackageCatalog, fs: &TmpFileSystem) -> Result<()> {
    for path in ["/tmp", "/work", "/home", "/home/sandbox", "/etc"] {
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
    Ok(())
}

fn create_parent_directories(fs: &TmpFileSystem, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(fs, parent)
            .with_context(|| format!("unable to create {}", parent.display()))?;
    }
    Ok(())
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

fn command_name(command: &str) -> Result<String> {
    let command = command
        .rsplit('/')
        .next()
        .ok_or_else(|| anyhow!("command cannot be empty"))?;
    if command.is_empty() {
        return Err(anyhow!("command cannot be empty"));
    }
    Ok(command.to_string())
}
