use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::core::{
    CompletedProcess as CoreCompletedProcess, EventBus, FileSystemEvent, HostMount, Limits,
    RunRequest, SandboxState,
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
    events: EventBus,
    event_receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<FileSystemEvent>>>,
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
        event_queue_size: usize,
    ) -> PyResult<Self> {
        let (events, event_receiver) = EventBus::new(event_queue_size);
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
                    events.clone(),
                )
                .map_err(py_error)?,
            )),
            events,
            event_receiver: Arc::new(tokio::sync::Mutex::new(event_receiver)),
        })
    }

    fn set_event_notifications_enabled(&self, enabled: bool) {
        self.events.set_enabled(enabled);
    }

    fn clear_events_now(&self) {
        let Ok(mut event_receiver) = self.event_receiver.try_lock() else {
            return;
        };
        while event_receiver.try_recv().is_ok() {}
    }

    fn clear_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let event_receiver = self.event_receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut event_receiver = event_receiver.lock().await;
            while event_receiver.try_recv().is_ok() {}
            Ok(())
        })
    }

    fn next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let event_receiver = self.event_receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut event_receiver = event_receiver.lock().await;
            let event = event_receiver
                .recv()
                .await
                .ok_or_else(|| PyRuntimeError::new_err("sandbox event stream closed"))?;
            Ok((
                event.sequence,
                event.kind.as_str().to_string(),
                event.path,
                event.target_path,
                event.dropped_count,
            ))
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
