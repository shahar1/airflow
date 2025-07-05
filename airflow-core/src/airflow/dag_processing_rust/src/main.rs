#[macro_use]
extern crate log;
use anyhow::Result;
use futures::stream::StreamExt; // Required for FuturesUnordered
use pyo3::prelude::*;
use pyo3::types::PyDict;
use signal_hook::consts::signal;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::fs;
use tokio::sync::mpsc; // We only need mpsc now
use tokio::task::spawn_blocking;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
struct CachedDag {
    mtime: SystemTime,
    marshaled_code: Vec<u8>,
}

type DagCacheMap = HashMap<PathBuf, CachedDag>;
type DagCache = Arc<tokio::sync::Mutex<DagCacheMap>>;

enum FileContent {
    Unchanged(Vec<u8>),
    Modified(String),
}

struct FileProcessingData {
    path: PathBuf,
    mtime: SystemTime,
    content: FileContent,
}

type CacheUpdate = (PathBuf, SystemTime, Vec<u8>);

async fn check_file(path: PathBuf, cache: DagCache) -> Result<FileProcessingData> {
    let mtime = fs::metadata(&path).await?.modified()?;
    let cache_guard = cache.lock().await;
    let cached_entry = cache_guard.get(&path);

    let content = match cached_entry {
        Some(cached) if cached.mtime == mtime => {
            FileContent::Unchanged(cached.marshaled_code.clone())
        }
        _ => FileContent::Modified(fs::read_to_string(&path).await?),
    };

    Ok(FileProcessingData { path, mtime, content })
}

// OPTIMIZED: This worker is now a long-running consumer that pulls from a queue.
fn process_python_queue_blocking(
    mut rx: mpsc::Receiver<FileProcessingData>,
) -> Result<Vec<CacheUpdate>> {
    let mut new_cache_entries = Vec::new();

    Python::with_gil(|py| -> Result<()> {
        let marshal = py.import("marshal")?;
        let marshal_dumps = marshal.getattr("dumps")?;
        let marshal_loads = marshal.getattr("loads")?;
        let builtins = py.import("builtins")?;
        let compile_func = builtins.getattr("compile")?;
        let exec_func = builtins.getattr("exec")?;

        // This loop processes items as they arrive from the async I/O tasks.
        while let Some(data) = rx.blocking_recv() {
            let path_str = data.path.to_string_lossy();
            let code_to_exec = match data.content {
                FileContent::Modified(code_string) => {
                    info!("[CACHE MISS] Re-compiling {}", path_str);
                    let compiled = compile_func.call1((&code_string, &*path_str, "exec"))?;
                    let marshaled: Vec<u8> = marshal_dumps.call1((compiled.clone(),))?.extract()?;
                    new_cache_entries.push((data.path.clone(), data.mtime, marshaled));
                    compiled
                }
                FileContent::Unchanged(marshaled_code) => {
                    trace!("[CACHE HIT] Using cached code for {}", path_str);
                    marshal_loads.call1((marshaled_code.as_slice(),))?
                }
            };

            let globals = PyDict::new(py);
            exec_func.call1((code_to_exec, &globals))?;
            trace!("Finished execution for {}. Globals: {:?}", path_str, globals);
        }
        Ok(())
    })?;

    Ok(new_cache_entries)
}

async fn run_processing_cycle(dags_dir: &str, cache: DagCache) -> Result<()> {
    let cycle_start_time = Instant::now();
    info!("--- Starting new async processing cycle ---");

    let current_paths: HashSet<PathBuf> = WalkDir::new(dags_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "py"))
        .map(|e| e.path().to_path_buf())
        .collect();
    cache.lock().await.retain(|path, _| current_paths.contains(path));

    if current_paths.is_empty() {
        info!("No Python files found.");
        return Ok(());
    }

    // --- Setup the pipeline ---
    let (tx, rx) = mpsc::channel::<FileProcessingData>(current_paths.len());
    let python_consumer_handle = spawn_blocking(move || process_python_queue_blocking(rx));

    // --- Stream I/O results into the pipeline ---
    let mut io_tasks = current_paths
        .into_iter()
        .map(|path| tokio::spawn(check_file(path, Arc::clone(&cache))))
        .collect::<futures::stream::FuturesUnordered<_>>();

    while let Some(res) = io_tasks.next().await {
        match res {
            Ok(Ok(data)) => {
                if tx.send(data).await.is_err() {
                    error!("Python worker channel closed prematurely. Aborting cycle.");
                    break;
                }
            }
            Ok(Err(e)) => error!("I/O error during file check: {}", e),
            Err(e) => error!("Tokio task failed: {}", e),
        }
    }

    // --- Finalize the pipeline ---
    // Drop the sender to signal the Python worker that no more work is coming.
    drop(tx);

    // Wait for the Python worker to finish processing everything in its queue and get the updates.
    let cache_updates = python_consumer_handle.await??;

    // Apply the delta updates to the main cache.
    if !cache_updates.is_empty() {
        let mut cache_guard = cache.lock().await;
        for (path, mtime, marshaled_code) in cache_updates {
            cache_guard.insert(path, CachedDag { mtime, marshaled_code });
        }
    }

    info!("--- Cycle Summary ---");
    info!("Processed files. Cache size: {}", cache.lock().await.len());
    info!("Processing cycle took: {:?}", cycle_start_time.elapsed());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    pyo3::prepare_freethreaded_python();
    env_logger::init();

    if env::var("AIRFLOW_HOME").is_err() {
        env::set_var("AIRFLOW_HOME", ".");
    }

    let dags_dir = "/files/dags";
    let cycle_interval = Duration::from_millis(100);

    let dag_cache: DagCache = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal::SIGINT, Arc::clone(&term))?;
    signal_hook::flag::register(signal::SIGTERM, Arc::clone(&term))?;

    info!("Starting pipelined DAG processor. Press Ctrl+C to exit.");

    while !term.load(Ordering::Relaxed) {
        if let Err(e) = run_processing_cycle(dags_dir, Arc::clone(&dag_cache)).await {
            error!("A critical error occurred during the processing cycle: {:?}", e);
        }
        tokio::time::sleep(cycle_interval).await;
    }

    info!("Signal received. Shutting down gracefully...");
    Ok(())
}