use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::core::{
    CompletedProcess as CoreCompletedProcess, HostMount, Limits, RunRequest, SandboxState,
};

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

impl From<CoreCompletedProcess> for CompletedProcess {
    fn from(process: CoreCompletedProcess) -> Self {
        Self {
            args: process.args,
            returncode: process.returncode,
            stdout: process.stdout,
            stderr: process.stderr,
        }
    }
}

#[pyclass(module = "unix_sandbox._native")]
pub struct Sandbox {
    state: Arc<Mutex<SandboxState>>,
}

#[pymethods]
impl Sandbox {
    #[new]
    pub fn new(
        files: HashMap<String, Option<Vec<u8>>>,
        host_mounts: Vec<(String, String, bool)>,
        cwd: String,
        env: HashMap<String, String>,
        asset_dir: String,
        output_limit: usize,
        wall_time_seconds: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            state: Arc::new(Mutex::new(
                SandboxState::new(
                    files,
                    host_mounts
                        .into_iter()
                        .map(|(source, target, read_only)| HostMount {
                            source,
                            target,
                            read_only,
                        })
                        .collect(),
                    cwd,
                    env,
                    asset_dir,
                    Limits {
                        output_bytes: output_limit,
                        wall_time_seconds,
                    },
                )
                .map_err(py_error)?,
            )),
        })
    }

    fn exists<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
            state.exists(&path).map_err(py_error)
        })
    }

    fn read_file<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?
                .clone();
            state.read_file(&path).await.map_err(py_error)
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
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?
                .clone();
            state.write_file(&path, data).await.map_err(py_error)
        })
    }

    fn listdir<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
            state.listdir(&path).map_err(py_error)
        })
    }

    fn run<'py>(
        &self,
        py: Python<'py>,
        args: Vec<String>,
        input: Option<Vec<u8>>,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?
                .clone();
            let process = tokio::task::spawn_blocking(move || {
                state.run_blocking(RunRequest {
                    args,
                    input,
                    env,
                    cwd,
                })
            })
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
            .map_err(py_error)?;

            Ok(CompletedProcess::from(process))
        })
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CompletedProcess>()?;
    module.add_class::<Sandbox>()?;
    Ok(())
}

fn py_error(error: anyhow::Error) -> PyErr {
    let message = error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join(": ");
    PyRuntimeError::new_err(message)
}
