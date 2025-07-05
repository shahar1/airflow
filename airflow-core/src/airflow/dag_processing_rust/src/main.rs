#[macro_use]
extern crate log;
use anyhow::{anyhow, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::Bound;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio::signal;
use walkdir::WalkDir;

/// Reads a Python file and executes its content as bytes using Python's `exec()`.
///
/// This worker runs in a dedicated thread. It receives full file paths, reads them,
/// and executes the content within a fresh, isolated Python environment.
fn python_executor_worker(
    dags_dir: String,
    mut rx: mpsc::Receiver<String>, // Receives a full file path as a String
) -> Result<()> {
    Python::with_gil(|py| -> Result<()> {
        let builtins = py.import("builtins")?;
        let exec_func = builtins.getattr("exec")?;

        let sys = py.import("sys")?;

        // --- FIX IS HERE ---
        // We explicitly map the PyDowncastError to a Send-able anyhow::Error.
        // This satisfies the compiler's requirement that the closure is Send.
        let sys_path_obj = sys.getattr("path")?;
        let sys_path: &Bound<'_, PyList> = sys_path_obj
            .downcast()
            .map_err(|e| anyhow!("sys.path was not a list: {}", e))?;
        // --- END FIX ---

        sys_path.insert(0, &dags_dir)?;

        info!("Python executor worker started. sys.path extended with: {}", dags_dir);

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

// This function is no longer used, but we'll keep a similar check inline.
// fn path_to_module_name(path: &Path) -> Option<String> { ... }


#[tokio::main]
async fn main() -> Result<()> {
    pyo3::prepare_freethreaded_python();
    env_logger::init();

    let dags_dir = "/files/dags"; // Use a distinct directory for testing
    std::fs::create_dir_all(dags_dir)?;
    let dags_dir_path = PathBuf::from(dags_dir);

    info!("Using DAGs directory: {}", dags_dir);

    // The channel now sends the full path of the file to be executed.
    let (tx, rx) = mpsc::channel::<String>(100);
    let worker_handle = {
        let dags_dir_clone = dags_dir.to_string();
        spawn_blocking(move || {
            // Renamed worker function for clarity
            if let Err(e) = python_executor_worker(dags_dir_clone, rx) {
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
        // We only care about .py files
        if path.is_file() && path.extension().map_or(false, |ext| ext == "py") {
            // Add the full path as a string
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
                        // Check if it's a python file and send its full path
                        if path.is_file() && path.extension().map_or(false, |ext| ext == "py") {
                            info!("[FS WATCH] Detected change in: {:?}", path);
                            // Send the full path as a string
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
    info!("Signal received. Shutting down gracefully...");

    drop(tx);

    let shutdown_timeout = Duration::from_secs(5);
    match tokio::time::timeout(shutdown_timeout, worker_handle).await {
        Ok(Ok(_)) => {
            info!("Python worker shut down gracefully.");
        }
        Ok(Err(e)) => {
            error!("Python worker task failed during shutdown: {:?}", e);
        }
        Err(_) => {
            warn!(
                "Graceful shutdown timed out after {:?}. Exiting.",
                shutdown_timeout
            );
        }
    }

    info!("Shutdown complete.");
    Ok(())
}

/// Sets up signal handlers for graceful shutdown.
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