#[macro_use]
extern crate log;
use anyhow::{anyhow, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::Bound;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio::signal;
use walkdir::WalkDir;

/// Executes a Python module using `runpy`. This is akin to `python -m <module_name>`.
///
/// It runs in a dedicated thread, acquiring the Python GIL only when it needs to
/// execute Python code.
fn python_importer_worker(
    dags_dir: String,
    mut rx: mpsc::Receiver<String>,
) -> Result<()> {
    Python::with_gil(|py| -> Result<()> {
        // Import the necessary Python modules. We now need 'runpy'.
        let sys = py.import("sys")?;
        let runpy = py.import("runpy")?;
        let run_module_func = runpy.getattr("run_module")?;

        // Add the dags directory to Python's path so it can find the modules.
        let sys_path_obj = sys.getattr("path")?;
        let sys_path: &Bound<'_, PyList> = sys_path_obj
            .downcast()
            .map_err(|e| anyhow!("sys.path was not a list: {}", e))?;
        sys_path.insert(0, &dags_dir)?;

        info!("Python worker started. sys.path extended with: {}", dags_dir);

        // Process module execution requests from the channel.
        while let Some(module_name) = rx.blocking_recv() {
            info!("[PYTHON] Received request to execute module: {}", module_name);
            let gil_start_time = std::time::Instant::now();

            // Use `runpy.run_module` to execute the script's code.
            // This ensures the code is run fresh each time.
            let kwargs = PyDict::new(py);
            // We pass `run_name="__main__"` so that `if __name__ == "__main__":` blocks execute.
            kwargs.set_item("run_name", "__main__")?;

            debug!("Executing module: {} as __main__", module_name);
            let result = run_module_func.call((&module_name,), Some(&kwargs));

            match result {
                Ok(module_globals) => {
                    // runpy.run_module returns a dictionary of the module's global variables.
                    info!(
                        "Successfully executed module '{}' in {:?}. Globals count: {}",
                        module_name,
                        gil_start_time.elapsed(),
                        module_globals.len().unwrap_or(0) // module_globals is a PyDict
                    );
                }
                Err(e) => {
                    error!("Python error executing module '{}':", module_name);
                    // Print the full Python traceback.
                    e.print(py);
                }
            }
        }

        info!("Python worker channel closed. Shutting down.");
        Ok(())
    })
}

/// Converts a file path to a Python module name if it's a `.py` file.
fn path_to_module_name(path: &Path) -> Option<String> {
    if path.extension().map_or(false, |ext| ext == "py") {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .map(String::from)
    } else {
        None
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    pyo3::prepare_freethreaded_python();
    // Use RUST_LOG=info cargo run to see logs
    env_logger::init();

    // Create the directory if it doesn't exist for easier testing.
    let dags_dir = "/files/dags"; // Using /tmp for easier testing
    std::fs::create_dir_all(dags_dir)?;
    let dags_dir_path = PathBuf::from(dags_dir);

    info!("Using DAGs directory: {}", dags_dir);


    let (tx, rx) = mpsc::channel::<String>(100);
    let worker_handle = {
        let dags_dir_clone = dags_dir.to_string();
        spawn_blocking(move || {
            if let Err(e) = python_importer_worker(dags_dir_clone, rx) {
                error!("Python worker thread failed: {:?}", e);
            }
        })
    };

    info!("Performing initial scan of '{}'...", dags_dir);
    let mut initial_modules = HashSet::new();
    for entry in WalkDir::new(dags_dir)
        .into_iter()
        .filter_map(Result::ok)
    {
        if let Some(module_name) = path_to_module_name(entry.path()) {
            initial_modules.insert(module_name);
        }
    }
    for module in initial_modules {
        info!("[INIT] Queuing existing module for execution: {}", module);
        tx.send(module).await?;
    }
    info!("Initial scan complete.");


    let watcher_tx = tx.clone();
    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        match res {
            Ok(event) => match event.kind {
                EventKind::Create(_) | EventKind::Modify(_) => {
                    for path in event.paths {
                        if let Some(module_name) = path_to_module_name(&path) {
                            info!("[FS WATCH] Detected change in: {:?}", path);
                            if let Err(e) = watcher_tx.blocking_send(module_name) {
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

    let shutdown_timeout = Duration::from_secs(1);
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