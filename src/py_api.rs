use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use pyo3::{exceptions::PyRuntimeError, prelude::*};

#[derive(Clone)]
enum FsEntry {
    Directory,
    File(Vec<u8>),
}

#[allow(dead_code)]
struct SandboxState {
    cwd: String,
    env: HashMap<String, String>,
    files: BTreeMap<String, FsEntry>,
    output_limit: usize,
    wall_time_seconds: Option<f64>,
}

impl SandboxState {
    fn new(
        files: HashMap<String, Option<Vec<u8>>>,
        cwd: String,
        env: HashMap<String, String>,
        output_limit: usize,
        wall_time_seconds: Option<f64>,
    ) -> PyResult<Self> {
        let mut state = Self {
            cwd,
            env,
            files: BTreeMap::new(),
            output_limit,
            wall_time_seconds,
        };

        state.create_directory("/")?;
        state.create_directory("/tmp")?;
        state.create_directory("/work")?;
        state.create_directory("/home")?;
        state.create_directory("/home/sandbox")?;
        state.create_directory("/etc")?;
        state.write_file(
            "/etc/passwd",
            b"sandbox:x:1000:1000:Sandbox User:/home/sandbox:/bin/sh\n".to_vec(),
        )?;
        state.write_file("/etc/group", b"sandbox:x:1000:\n".to_vec())?;

        for (path, contents) in files {
            match contents {
                Some(data) => state.write_file(&path, data)?,
                None => state.create_directory(&path)?,
            }
        }

        Ok(state)
    }

    fn normalize_path(path: &str) -> PyResult<String> {
        if path.as_bytes().contains(&0) {
            return Err(PyRuntimeError::new_err(
                "sandbox paths cannot contain NUL bytes",
            ));
        }
        if !path.starts_with('/') {
            return Err(PyRuntimeError::new_err("sandbox paths must be absolute"));
        }

        let mut components = Vec::new();
        for component in path.split('/') {
            if component.is_empty() || component == "." {
                continue;
            }
            if component == ".." {
                if components.pop().is_none() {
                    return Err(PyRuntimeError::new_err("sandbox paths cannot escape root"));
                }
                continue;
            }
            components.push(component);
        }

        if components.is_empty() {
            return Ok("/".to_string());
        }

        Ok(format!("/{}", components.join("/")))
    }

    fn create_directory(&mut self, path: &str) -> PyResult<()> {
        let normalized = Self::normalize_path(path)?;
        self.files.insert(normalized, FsEntry::Directory);
        Ok(())
    }

    fn write_file(&mut self, path: &str, data: Vec<u8>) -> PyResult<()> {
        let normalized = Self::normalize_path(path)?;
        let mut current = String::new();
        let components = normalized
            .split('/')
            .filter(|component| !component.is_empty())
            .collect::<Vec<_>>();
        let parent_count = components.len().saturating_sub(1);
        for component in components.into_iter().take(parent_count) {
            current.push('/');
            current.push_str(component);
            self.files
                .entry(current.clone())
                .or_insert(FsEntry::Directory);
        }
        self.files.insert(normalized, FsEntry::File(data));
        Ok(())
    }
}

#[pyclass(module = "unix_sandbox._native")]
pub struct CompletedProcess {
    #[pyo3(get)]
    pub args: Vec<String>,
    #[pyo3(get)]
    pub returncode: i32,
    #[pyo3(get)]
    pub stdout: Vec<u8>,
    #[pyo3(get)]
    pub stderr: Vec<u8>,
}

#[pyclass(module = "unix_sandbox._native")]
pub struct Sandbox {
    #[allow(dead_code)]
    state: Arc<Mutex<SandboxState>>,
    #[allow(dead_code)]
    asset_dir: String,
}

#[pymethods]
impl Sandbox {
    #[new]
    pub fn new(
        files: HashMap<String, Option<Vec<u8>>>,
        cwd: String,
        env: HashMap<String, String>,
        asset_dir: String,
        output_limit: usize,
        wall_time_seconds: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            state: Arc::new(Mutex::new(SandboxState::new(
                files,
                cwd,
                env,
                output_limit,
                wall_time_seconds,
            )?)),
            asset_dir,
        })
    }

    fn exists<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let normalized = SandboxState::normalize_path(&path)?;
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
            Ok(state.files.contains_key(&normalized))
        })
    }

    fn read_file<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let normalized = SandboxState::normalize_path(&path)?;
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
            match state.files.get(&normalized) {
                Some(FsEntry::File(data)) => Ok(data.clone()),
                Some(FsEntry::Directory) => Err(PyRuntimeError::new_err(format!(
                    "{normalized} is a directory"
                ))),
                None => Err(PyRuntimeError::new_err(format!(
                    "{normalized} does not exist"
                ))),
            }
        })
    }

    fn write_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
            state.write_file(&path, data)?;
            Ok(())
        })
    }

    fn listdir<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let normalized = SandboxState::normalize_path(&path)?;
            let prefix = if normalized == "/" {
                "/".to_string()
            } else {
                format!("{normalized}/")
            };
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
            if !matches!(state.files.get(&normalized), Some(FsEntry::Directory)) {
                return Err(PyRuntimeError::new_err(format!(
                    "{normalized} is not a directory"
                )));
            }
            let mut entries = Vec::new();
            for candidate in state.files.keys() {
                if !candidate.starts_with(&prefix) || candidate == &normalized {
                    continue;
                }
                let remainder = candidate.trim_start_matches(&prefix);
                if remainder.contains('/') {
                    continue;
                }
                entries.push(remainder.to_string());
            }
            Ok(entries)
        })
    }

    fn run<'py>(
        &self,
        py: Python<'py>,
        args: Vec<String>,
        _input: Option<Vec<u8>>,
        _env: Option<HashMap<String, String>>,
        _cwd: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if args.is_empty() {
                return Err(PyRuntimeError::new_err("command arguments cannot be empty"));
            }
            Ok(CompletedProcess {
                args,
                returncode: 127,
                stdout: Vec::new(),
                stderr: b"Wasmer runtime assets are not initialized yet\n".to_vec(),
            })
        })
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CompletedProcess>()?;
    module.add_class::<Sandbox>()?;
    Ok(())
}
