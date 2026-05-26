use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::core::{
    CancellationSource, CapturedOutput, CompletedProcess as CoreCompletedProcess, EventBus,
    FileSystemEvent, HostMount, InteractiveStdin, Limits, PackageCommandAlias, PackageSpec,
    ProcessStreams, RunRequest, SandboxState, VirtualExecutableBridge, VirtualProcessRequest,
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

#[derive(Clone)]
enum ProcessOutcome {
    Completed(CoreCompletedProcess),
    Failed(String),
}

#[pyclass(module = "unix_sandbox._native")]
pub struct StartedProcess {
    id: u64,
    args: Vec<String>,
    stdin: InteractiveStdin,
    stdout: CapturedOutput,
    stderr: CapturedOutput,
    result_receiver: tokio::sync::watch::Receiver<Option<ProcessOutcome>>,
    running_processes: Arc<Mutex<HashMap<u64, CancellationSource>>>,
}

#[pymethods]
impl StartedProcess {
    #[getter]
    fn args(&self) -> Vec<String> {
        self.args.clone()
    }

    #[getter]
    fn returncode(&self) -> Option<i32> {
        match &*self.result_receiver.borrow() {
            Some(ProcessOutcome::Completed(process)) => Some(process.returncode),
            Some(ProcessOutcome::Failed(_)) | None => None,
        }
    }

    #[getter]
    fn stdin_closed(&self) -> PyResult<bool> {
        self.stdin.is_closed().map_err(py_error)
    }

    #[getter]
    fn stdout(&self) -> PyResult<Vec<u8>> {
        self.stdout.capture("stdout").map_err(py_error)
    }

    #[getter]
    fn stderr(&self) -> PyResult<Vec<u8>> {
        self.stderr.capture("stderr").map_err(py_error)
    }

    fn is_running(&self) -> bool {
        self.result_receiver.borrow().is_none()
    }

    fn write_stdin(&self, data: Vec<u8>) -> PyResult<()> {
        self.stdin.write(data).map_err(py_error)
    }

    fn close_stdin(&self) -> PyResult<()> {
        self.stdin.close().map_err(py_error)
    }

    fn cancel(&self) {
        let _ = self.stdin.close();
        cancel_process(&self.running_processes, self.id);
    }

    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result_receiver = self.result_receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, wait_process_outcome(result_receiver))
    }

    fn wait_blocking(&self) -> PyResult<CompletedProcess> {
        let result_receiver = self.result_receiver.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(wait_process_outcome(result_receiver))
    }
}

impl Drop for StartedProcess {
    fn drop(&mut self) {
        if self.result_receiver.borrow().is_some() {
            return;
        }
        let _ = self.stdin.close();
        cancel_process(&self.running_processes, self.id);
    }
}

struct RunningProcessGuard {
    id: u64,
    processes: Arc<Mutex<HashMap<u64, CancellationSource>>>,
    active: bool,
}

impl RunningProcessGuard {
    fn new(
        id: u64,
        processes: Arc<Mutex<HashMap<u64, CancellationSource>>>,
        source: CancellationSource,
    ) -> PyResult<Self> {
        processes
            .lock()
            .map_err(|_| PyRuntimeError::new_err("process cancellation lock failed"))?
            .insert(id, source);
        Ok(Self {
            id,
            processes,
            active: true,
        })
    }

    fn finish(&mut self) {
        let Ok(mut processes) = self.processes.lock() else {
            return;
        };
        processes.remove(&self.id);
        self.active = false;
    }
}

impl Drop for RunningProcessGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        };
        cancel_process(&self.processes, self.id);
    }
}

#[pyclass(module = "unix_sandbox._native")]
pub struct Sandbox {
    state: Arc<Mutex<SandboxState>>,
    events: EventBus,
    event_receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<FileSystemEvent>>>,
    virtual_process_receiver:
        Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<VirtualProcessRequest>>>,
    pending_virtual_processes: Arc<Mutex<HashMap<u64, VirtualProcessRequest>>>,
    running_processes: Arc<Mutex<HashMap<u64, CancellationSource>>>,
}

#[pymethods]
impl Sandbox {
    #[new]
    pub fn new(
        files: HashMap<String, Option<Vec<u8>>>,
        host_mounts: Vec<(String, String, bool)>,
        packages: Vec<(String, String, String, Vec<(String, String)>)>,
        cwd: String,
        env: HashMap<String, String>,
        output_limit: usize,
        wall_time_seconds: Option<f64>,
        event_queue_size: usize,
    ) -> PyResult<Self> {
        let (events, event_receiver) = EventBus::new(event_queue_size);
        let (virtual_processes, virtual_process_receiver) =
            VirtualExecutableBridge::new(event_queue_size);
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
                    packages
                        .into_iter()
                        .map(
                            |(name, webc_path, content_sha256, command_aliases)| PackageSpec {
                                name,
                                webc_path,
                                content_sha256,
                                command_aliases: command_aliases
                                    .into_iter()
                                    .map(|(alias, command)| PackageCommandAlias { alias, command })
                                    .collect(),
                            },
                        )
                        .collect(),
                    cwd,
                    env,
                    Limits {
                        output_bytes: output_limit,
                        wall_time_seconds,
                    },
                    events.clone(),
                    virtual_processes,
                )
                .map_err(py_error)?,
            )),
            events,
            event_receiver: Arc::new(tokio::sync::Mutex::new(event_receiver)),
            virtual_process_receiver: Arc::new(tokio::sync::Mutex::new(virtual_process_receiver)),
            pending_virtual_processes: Arc::new(Mutex::new(HashMap::new())),
            running_processes: Arc::new(Mutex::new(HashMap::new())),
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

    fn register_virtual_executable(
        &self,
        token: u64,
        paths: Vec<String>,
        replace: bool,
    ) -> PyResult<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
        state
            .register_virtual_executable(token, paths, replace)
            .map_err(py_error)
    }

    fn unregister_virtual_executable(&self, token: u64) -> PyResult<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?;
        state.unregister_virtual_executable(token).map_err(py_error)
    }

    fn next_virtual_process<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let virtual_process_receiver = self.virtual_process_receiver.clone();
        let pending_virtual_processes = self.pending_virtual_processes.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut receiver = virtual_process_receiver.lock().await;
            let request = receiver
                .recv()
                .await
                .ok_or_else(|| PyRuntimeError::new_err("virtual process stream closed"))?;
            let id = request.id;
            let payload = request.payload.clone();
            pending_virtual_processes
                .lock()
                .map_err(|_| PyRuntimeError::new_err("virtual process lock failed"))?
                .insert(id, request);
            Ok((id, payload))
        })
    }

    fn complete_virtual_process(&self, id: u64, response: Vec<u8>) -> PyResult<()> {
        let request = self
            .pending_virtual_processes
            .lock()
            .map_err(|_| PyRuntimeError::new_err("virtual process lock failed"))?
            .remove(&id)
            .ok_or_else(|| PyRuntimeError::new_err("virtual process request not found"))?;
        request.respond(response).map_err(py_error)
    }

    fn wait_virtual_process_cancelled<'py>(
        &self,
        py: Python<'py>,
        id: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut cancellation = self
            .pending_virtual_processes
            .lock()
            .map_err(|_| PyRuntimeError::new_err("virtual process lock failed"))?
            .get(&id)
            .ok_or_else(|| PyRuntimeError::new_err("virtual process request not found"))?
            .cancellation_token();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            cancellation.cancelled().await;
            Ok(())
        })
    }

    fn cancel_process(&self, id: u64) {
        cancel_process(&self.running_processes, id);
    }

    fn start(
        &self,
        id: u64,
        args: Vec<String>,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
    ) -> PyResult<StartedProcess> {
        let state = self.state.clone();
        let output_limit = state
            .lock()
            .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?
            .limits
            .output_bytes;
        let stdin = InteractiveStdin::new();
        let stdout = CapturedOutput::new(output_limit);
        let stderr = CapturedOutput::new(output_limit);
        let cancellation_source = CancellationSource::new();
        let cancellation = cancellation_source.token();
        let process_guard =
            RunningProcessGuard::new(id, self.running_processes.clone(), cancellation_source)?;
        let request = RunRequest {
            args: args.clone(),
            input: None,
            env,
            cwd,
        };
        let stdin_for_task = stdin.clone();
        let streams = ProcessStreams {
            stdin: Box::new(stdin.file()),
            stdout: stdout.clone(),
            stderr: stderr.clone(),
        };
        let (result_sender, result_receiver) = tokio::sync::watch::channel(None);
        let process_task = pyo3_async_runtimes::tokio::get_runtime().spawn_blocking(move || {
            let mut process_guard = process_guard;
            let result = (|| {
                let state = state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("sandbox state lock failed"))?
                    .clone();
                state.run_with_stdio_blocking(request, streams, cancellation)
            })();
            process_guard.finish();
            let _ = stdin_for_task.close();
            match result {
                Ok(process) => ProcessOutcome::Completed(process),
                Err(error) => ProcessOutcome::Failed(error_message(error)),
            }
        });
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let outcome = match process_task.await {
                Ok(outcome) => outcome,
                Err(error) => ProcessOutcome::Failed(error.to_string()),
            };
            let _ = result_sender.send(Some(outcome));
        });
        Ok(StartedProcess {
            id,
            args,
            stdin,
            stdout,
            stderr,
            result_receiver,
            running_processes: self.running_processes.clone(),
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
        id: u64,
        args: Vec<String>,
        input: Option<Vec<u8>>,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let cancellation_source = CancellationSource::new();
        let cancellation = cancellation_source.token();
        let process_guard =
            RunningProcessGuard::new(id, self.running_processes.clone(), cancellation_source)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut process_guard = process_guard;
            let state = state
                .lock()
                .map_err(|_| PyRuntimeError::new_err("sandbox state lock failed"))?
                .clone();
            let process = tokio::task::spawn_blocking(move || {
                state.run_blocking(
                    RunRequest {
                        args,
                        input,
                        env,
                        cwd,
                    },
                    cancellation,
                )
            })
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            process_guard.finish();
            let process = process.map_err(py_error)?;

            Ok(CompletedProcess::from(process))
        })
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CompletedProcess>()?;
    module.add_class::<StartedProcess>()?;
    module.add_class::<Sandbox>()?;
    Ok(())
}

fn py_error(error: anyhow::Error) -> PyErr {
    PyRuntimeError::new_err(error_message(error))
}

fn error_message(error: anyhow::Error) -> String {
    let message = error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join(": ");
    message
}

async fn wait_process_outcome(
    mut result_receiver: tokio::sync::watch::Receiver<Option<ProcessOutcome>>,
) -> PyResult<CompletedProcess> {
    loop {
        let outcome = { result_receiver.borrow().clone() };
        if let Some(outcome) = outcome {
            return process_outcome_result(outcome);
        }
        result_receiver
            .changed()
            .await
            .map_err(|_| PyRuntimeError::new_err("process wait channel closed"))?;
    }
}

fn process_outcome_result(outcome: ProcessOutcome) -> PyResult<CompletedProcess> {
    match outcome {
        ProcessOutcome::Completed(process) => Ok(CompletedProcess::from(process)),
        ProcessOutcome::Failed(message) => Err(PyRuntimeError::new_err(message)),
    }
}

fn cancel_process(processes: &Arc<Mutex<HashMap<u64, CancellationSource>>>, id: u64) {
    let Ok(mut processes) = processes.lock() else {
        return;
    };
    let Some(source) = processes.remove(&id) else {
        return;
    };
    source.cancel();
}
