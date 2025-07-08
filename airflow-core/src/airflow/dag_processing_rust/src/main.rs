// Add this to use the `table!` macro from schema.rs
#[macro_use]
extern crate diesel;
#[macro_use]
extern crate log;

use anyhow::{anyhow, Result};
use chrono;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::Bound;
use pyo3::{prelude::*, FromPyObject};
use serde_json;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use walkdir::WalkDir;


// --- Add module declarations ---
pub mod db;
pub mod schema;

// ... (Your existing EdgeInfoType and Dag structs are unchanged)
#[derive(Debug, Clone, FromPyObject)]
struct EdgeInfoType {
    label: Option<String>,
}

#[derive(Debug, Clone)]
struct Dag {
    dag_id: String,
    description: Option<String>,
    start_date: Option<chrono::DateTime<chrono::Utc>>,
    max_active_tasks: Option<u32>,
    fail_fast: Option<bool>,
    max_active_runs: Option<u32>,
    doc_md: Option<String>,
    fileloc: Option<String>,
    edge_info: Option<std::collections::HashMap<String, std::collections::HashMap<String, EdgeInfoType>>>,
    max_consecutive_failed_dag_runs: Option<u32>,
    render_template_as_native_obj: Option<bool>,
    owner_links: Option<std::collections::HashMap<String, String>>,
    tags: Option<Vec<String>>,
    is_paused_upon_creation: Option<bool>,
    default_args: Option<std::collections::HashMap<String, serde_json::Value>>,
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    disable_bundle_versioning: bool,
    relative_fileloc: Option<String>,
    catchup: bool,
    dag_display_name: Option<String>,
    deadline: Option<std::collections::HashMap<String, serde_json::Value>>,
    timetable: Option<String>,
    timezone: Option<String>,
    access_control: Option<String>,
    dagrun_timeout: Option<String>,
    task_group: Option<String>,
}

// ... (Your py_any_to_serde_json_value and python_executor_worker are unchanged)
fn py_any_to_serde_json_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let json = py.import("json")?;
    let result_str: String = json.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&result_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to deserialize JSON: {}", e)))
}
fn python_executor_worker(
    dags_dir: String,
    mut rx: mpsc::Receiver<String>,
    serialized_dags: Arc<Mutex<Vec<Dag>>>,
) -> Result<()> {
    // ... (This function's implementation remains the same)
    Python::with_gil(|py| -> Result<()> {
        let builtins = py.import("builtins")?;
        let exec_func = builtins.getattr("exec")?;

        let sys = py.import("sys")?;

        // We explicitly map the PyDowncastError to a Send-able anyhow::Error.
        // This satisfies the compiler's requirement that the closure is Send.
        let sys_path_obj = sys.getattr("path")?;
        let sys_path: &Bound<'_, PyList> = sys_path_obj
            .downcast()
            .map_err(|e| anyhow!("sys.path was not a list: {}", e))?;
        sys_path.insert(0, &dags_dir)?;

        info!("Python executor worker started. sys.path extended with: {}", dags_dir);
        let dag_module = py.import("airflow.sdk.definitions.dag")?;
        let dag_class = dag_module.getattr("DAG")?;
        info!("Successfully imported 'airflow.sdk.definitions.dag.DAG' class.");

        // SOLUTION: The local `serialized_dags` vector is removed. We will use the shared one.

        while let Some(file_path_str) = rx.blocking_recv() {
            info!("[PYTHON] Received request to execute file: {}", file_path_str);
            let gil_start_time = std::time::Instant::now();

            let file_content = match std::fs::read(&file_path_str) {
                Ok(content) => content,
                Err(e) => {
                    error!("Failed to read file {}: {}. Skipping.", file_path_str, e);
                    continue;
                }
            };

            let code_bytes = PyBytes::new(py, &file_content);
            let globals = PyDict::new(py);
            globals.set_item("__name__", "__main__")?;
            globals.set_item("__file__", &file_path_str)?;

            let result = exec_func.call((code_bytes, globals.clone(), globals.clone()), None);

            match result {
                Ok(_) => {
                    info!(
                        "Successfully executed file '{}' in {:?}.",
                        file_path_str,
                        gil_start_time.elapsed(),
                    );
                    let mut dag_found = false;
                    for (name, obj) in globals.iter() {
                        if obj.is_instance(&dag_class)? {
                            let dag_repr: String = obj.repr()?.to_str()?.to_string();
                            let var_name = name.to_string();
                            info!(
                                "Found DAG object in '{}' (variable: '{}'): {}",
                                file_path_str, var_name, dag_repr
                            );
                            dag_found = true;

                            let dag = Dag {
                                dag_id: obj.getattr("dag_id")?.extract()?,
                                description: obj.getattr("description").ok().and_then(|v| v.extract().ok()),
                                start_date: obj.getattr("start_date").ok().and_then(|v| v.extract().ok()),
                                max_active_tasks: obj.getattr("max_active_tasks").ok().and_then(|v| v.extract().ok()),
                                fail_fast: obj.getattr("fail_fast").ok().and_then(|v| v.extract().ok()),
                                max_active_runs: obj.getattr("max_active_runs").ok().and_then(|v| v.extract().ok()),
                                doc_md: obj.getattr("doc_md").ok().and_then(|v| v.extract().ok()),
                                fileloc: obj.getattr("fileloc").ok().and_then(|v| v.extract().ok()),
                                edge_info: obj.getattr("edge_info").ok().and_then(|v| v.extract().ok()),
                                max_consecutive_failed_dag_runs: obj.getattr("max_consecutive_failed_dag_runs").ok().and_then(|v| v.extract().ok()),
                                render_template_as_native_obj: obj.getattr("render_template_as_native_obj").ok().and_then(|v| v.extract().ok()),
                                owner_links: obj.getattr("owner_links").ok().and_then(|v| v.extract().ok()),
                                tags: obj.getattr("tags").ok().and_then(|v| v.extract().ok()),
                                is_paused_upon_creation: obj.getattr("is_paused_upon_creation").ok().and_then(|v| v.extract().ok()),
                                end_date: obj.getattr("end_date").ok().and_then(|v| v.extract().ok()),
                                disable_bundle_versioning: obj.getattr("disable_bundle_versioning").ok().and_then(|v| v.extract().ok()).unwrap_or(false),
                                relative_fileloc: obj.getattr("relative_fileloc").ok().and_then(|v| v.extract().ok()),
                                catchup: obj.getattr("catchup").ok().and_then(|v| v.extract().ok()).unwrap_or(false),
                                dag_display_name: obj.getattr("dag_display_name").ok().and_then(|v| v.extract().ok()),
                                timetable: obj.getattr("timetable").ok().and_then(|v| v.extract().ok()),
                                timezone: obj.getattr("timezone").ok().and_then(|v| v.extract().ok()),
                                access_control: obj.getattr("access_control").ok().and_then(|v| v.extract().ok()),
                                dagrun_timeout: obj.getattr("dagrun_timeout").ok().and_then(|v| v.extract().ok()),
                                task_group: obj.getattr("task_group").ok().and_then(|v| v.extract().ok()),
                                default_args: obj.getattr("default_args").ok().and_then(|v| {
                                    py_any_to_serde_json_value(py, &v)
                                        .ok()
                                        .and_then(|json_val| serde_json::from_value(json_val).ok())
                                }),
                                deadline: obj.getattr("deadline").ok().and_then(|v| {
                                    py_any_to_serde_json_value(py, &v)
                                        .ok()
                                        .and_then(|json_val| serde_json::from_value(json_val).ok())
                                }),
                            };

                            // SOLUTION: Lock the mutex to get exclusive access, then push the new DAG.
                            // The lock is released automatically when `dags_guard` goes out of scope.
                            {
                                let mut dags_guard = serialized_dags.lock().unwrap();
                                // To avoid duplicates if a file is processed multiple times
                                dags_guard.retain(|d| d.dag_id != dag.dag_id);
                                dags_guard.push(dag);
                            }
                            break;
                        }
                    }
                    if !dag_found {
                        warn!("No instance of DAG found in file '{}'", file_path_str);
                    }
                }
                Err(e) => {
                    error!("Python error executing file '{}':", file_path_str);
                    e.print(py);
                }
            }
        }
        info!("Python worker channel closed. Shutting down.");
        Ok(())
    })
}


#[tokio::main]
async fn main() -> Result<()> {
    // ... (Your main function setup is the same until the shutdown logic)
    pyo3::prepare_freethreaded_python();
    env_logger::init();

    let dags_dir = "/files/dags"; // Use a distinct directory for testing
    std::fs::create_dir_all(dags_dir)?;
    let dags_dir_path = PathBuf::from(dags_dir);

    info!("Using DAGs directory: {}", dags_dir);

    let serialized_dags = Arc::new(Mutex::new(Vec::<Dag>::new()));

    let (tx, rx) = mpsc::channel::<String>(100);
    let worker_handle = {
        let dags_dir_clone = dags_dir.to_string();
        let worker_dags_handle = Arc::clone(&serialized_dags);
        spawn_blocking(move || {
            if let Err(e) = python_executor_worker(dags_dir_clone, rx, worker_dags_handle) {
                error!("Python executor worker thread failed: {:?}", e);
            }
        })
    };

    info!("Performing initial scan of '{}'...", dags_dir);
    let mut initial_files = HashSet::new();
    for entry in WalkDir::new(dags_dir)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "py") {
            initial_files.insert(path.to_string_lossy().into_owned());
        }
    }
    for file_path in initial_files {
        info!("[INIT] Queuing existing file for execution: {}", file_path);
        tx.send(file_path).await?;
    }
    info!("Initial scan complete.");


    let watcher_tx = tx.clone();
    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        match res {
            Ok(event) => match event.kind {
                EventKind::Create(_) | EventKind::Modify(_) => {
                    for path in event.paths {
                        if path.is_file() && path.extension().map_or(false, |ext| ext == "py") {
                            info!("[FS WATCH] Detected change in: {:?}", path);
                            let path_str = path.to_string_lossy().into_owned();
                            if let Err(e) = watcher_tx.blocking_send(path_str) {
                                error!("Failed to send file update to worker: {}", e);
                            }
                        }
                    }
                }
                _ => (),
            },
            Err(e) => error!("File watcher error: {:?}", e),
        }
    })?;

    watcher.watch(&dags_dir_path, RecursiveMode::Recursive)?;
    info!("Now watching for file changes in '{}'", dags_dir);
    info!("Starting DAG processor. Press Ctrl+C to exit.");

    wait_for_shutdown_signal().await;

    // --- MODIFIED SHUTDOWN LOGIC ---

    info!("Signal received. Shutting down gracefully...");
    drop(tx); // Close the channel to signal the worker to stop

    let shutdown_timeout = Duration::from_secs(5);
    match tokio::time::timeout(shutdown_timeout, worker_handle).await {
        Ok(Ok(_)) => info!("Python worker shut down gracefully."),
        Ok(Err(e)) => error!("Python worker task failed during shutdown: {:?}", e),
        Err(_) => warn!(
            "Graceful shutdown timed out after {:?}. Exiting.",
            shutdown_timeout
        ),
    }

    // --- NEW DATABASE SAVE LOGIC ---
    info!("Attempting to save all parsed DAGs to the database...");
    let dags_to_save = serialized_dags.lock().unwrap();
    if !dags_to_save.is_empty() {
        match db::establish_connection() {
            Ok(mut conn) => {
                let mut success_count = 0;
                for dag in dags_to_save.iter() {
                    match db::save_dag(&mut conn, dag) {
                        Ok(_) => {
                            info!("Successfully saved/updated DAG '{}' in DB.", dag.dag_id);
                            success_count += 1;
                        }
                        Err(e) => error!("Failed to save DAG '{}' to DB: {}", dag.dag_id, e),
                    }
                }
                info!("DB sync complete. Saved/updated {} of {} DAGs.", success_count, dags_to_save.len());
            }
            Err(e) => {
                error!("Failed to establish database connection, cannot save DAGs: {}", e)
            }
        }
    } else {
        info!("No new or updated DAGs to save.");
    }

    info!("Shutdown complete.");
    Ok(())
}

// ... (Your wait_for_shutdown_signal function is unchanged)
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Failed to install SIGINT handler");
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = sigint.recv() => info!("SIGINT received."),
            _ = sigterm.recv() => info!("SIGTERM received."),
        };
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await.expect("Failed to install Ctrl-C handler");
    }
}