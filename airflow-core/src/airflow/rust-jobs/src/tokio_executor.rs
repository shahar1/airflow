use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;


#[derive(Debug, Clone)]
pub struct Workload {
    pub id: u32,
    pub command: String
}

#[derive(Debug, Clone)]
pub struct WorkResult {
    pub workload_id: u32,
    pub state: String
}

pub struct TokioExecutor {
    /// The sender part of the channel to send workloads to workers.
    task_tx: mpsc::Sender<Option<Workload>>,

    /// The receiver part of the channel to send workloads to workers.
    /// Needs to be shared between worker tasks.
    task_rx: Arc<Mutex<mpsc::Receiver<Option<Workload>>>>,

    /// The sender part of the channel to send results from workers.
    result_tx: mpsc::Sender<WorkResult>,

    /// The receiver part of the channel to get results from workers.
    result_rx: mpsc::Receiver<WorkResult>,

    /// Handles for the spawned worker tasks.
    worker_handles: Vec<JoinHandle<()>>,

    /// A shared counter for tasks that have been queued but not yet processed.
    unread_messages: Arc<AtomicUsize>,

    /// Maximum number of concurrent tasks.
    parallelism: usize,

    /// A counter for the number of active workers.
    active_workers: Arc<AtomicUsize>,
}

async fn run_worker(
    task_rx: Arc<Mutex<mpsc::Receiver<Option<Workload>>>>,
    result_tx: mpsc::Sender<WorkResult>,
    unread_messages: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
) {
    println!("Worker starting up");
    active_workers.fetch_add(1, Ordering::SeqCst);
    loop {
        let mut receiver = task_rx.lock().await;
        let work = receiver.recv().await;

        drop(receiver);

        if let Some(Some(workload)) = work {
            unread_messages.fetch_sub(1, Ordering::SeqCst);
            println!("Worker processing workload id: {}", workload.id);

            tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&workload.command)
                .spawn()
                .expect("Failed to execute command");

            let result = WorkResult {
                workload_id: workload.id,
                state: "SUCCESS".to_string()
            };

            if result_tx.send(result).await.is_err() {
                eprintln!("Failed to send result, receiver dropped.,");
                break;
            }
        }
        else {
            // Received None (shutdown signal) or channel closed.
            let remaining = active_workers.fetch_sub(1, Ordering::SeqCst) - 1;
            println!("Worker shutting down. Remaining active workers: {}", remaining);
            break;
        }
    }
}

impl TokioExecutor {
    pub fn new(parallelism: usize) -> Self {
        let (task_tx, task_rx) = mpsc::channel(100);
        let (result_tx, result_rx) = mpsc::channel(100);

        Self {
            task_tx,
            task_rx: Arc::new(Mutex::new(task_rx)),
            result_tx,
            result_rx,
            worker_handles: Vec::new(),
            unread_messages: Arc::new(AtomicUsize::new(0)),
            parallelism,
            active_workers: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn spawn_worker(&mut self) {
        if self.worker_handles.len() >= self.parallelism && self.parallelism > 0 {
            return;
        }

        let task_rx_clone = Arc::clone(&self.task_rx);
        let result_tx_clone = self.result_tx.clone();
        let unread_messages_clone = Arc::clone(&self.unread_messages);
        let active_workers_clone = Arc::clone(&self.active_workers);

        let handle = tokio::spawn(run_worker(
            task_rx_clone,
            result_tx_clone,
            unread_messages_clone,
            active_workers_clone,
        ));
        self.worker_handles.push(handle);
    }

    pub async fn queue_workload(&mut self, workload: Workload) {
        if self.task_tx.send(Some(workload)).await.is_ok() {
            self.unread_messages.fetch_add(1, Ordering::SeqCst);
            // Spawn a worker if there are pending tasks and we are below the parallelism limit.
            self.check_and_spawn_worker();
        } else {
            eprintln!("Failed to queue workload, channel closed.");
        }
    }

    /// Checks for pending tasks and spawns a worker if necessary.
    fn check_and_spawn_worker(&mut self) {
        let num_outstanding = self.unread_messages.load(Ordering::SeqCst);
        if num_outstanding > 0
            && (self.parallelism == 0 || self.worker_handles.len() < self.parallelism)
        {
            self.spawn_worker();
        }
    }

    /// Reads and processes available results from workers.
    pub async fn read_results(&mut self) {
        while let Ok(result) = self.result_rx.try_recv() {
            println!(
                "Result received for workload {}: {}",
                result.workload_id, result.state
            );
        }
    }

    /// Shuts down the executor, waiting for all tasks to complete.
    pub async fn shutdown(self) {
        println!("Shutting down executor...");
        // Send a shutdown signal for each worker to ensure they all shut down.
        for _ in 0..self.worker_handles.len() {
            if self.task_tx.send(None).await.is_err() {
                // If sending fails, it likely means all receivers have been dropped,
                // which is expected during shutdown.
                break;
            }
        }

        // Wait for all worker handles to complete.
        for handle in self.worker_handles {
            handle.await.unwrap();
        }

        // Process any final results.
        // Close the result sender to ensure the receiver loop terminates.
        let Self { result_tx, mut result_rx, .. } = self;
        drop(result_tx);

        while let Some(result) = result_rx.recv().await {
            println!(
                "Result received for workload {}: {}",
                result.workload_id, result.state
            );
        }
        println!("Executor shutdown complete.");
    }

}
