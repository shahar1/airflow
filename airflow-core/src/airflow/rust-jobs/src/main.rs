use std::io::Error;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;

use futures::stream::StreamExt;

use log::{debug, error, info};

use env_logger;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel::sql_types::{Interval, Timestamptz};
use diesel_async::pooled_connection::PoolError;

use crate::schema::job::dsl::*;

mod config;
mod database_callback_sink;
pub mod models;
pub mod schema;
mod tokio_executor;
mod serialized_objects;

use crate::config::global_config;
use crate::tokio_executor::{TokioExecutor, Workload};
use config::SchedulerConfig;
use database_callback_sink::DatabaseCallbackSink;
use models::{Job, TaskInstance};
use tokio_cron_scheduler::{
    Job as TCSJob, JobScheduler as TCSJobScheduler, JobSchedulerError as TCSJobSchedulerError,
};
use url::Url;
use uuid::Uuid;

use anyhow::{Context, Result};
use serialized_objects::Serialized;

async fn register_signals<S>(mut signals: S, signal_count: Arc<AtomicUsize>)
where
    S: futures::Stream<Item = i32> + Unpin,
{
    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT => {
                let prev = signal_count.fetch_add(1, Ordering::SeqCst);
                if prev == 0 {
                    info!("Exiting gracefully upon receiving signal {}", signal);
                } else {
                    // In a test context, we don't want to exit the process
                    #[cfg(not(test))]
                    std::process::exit(1);
                }
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    info!("Starting the scheduler");
    let config = global_config();
    if config.parallelism < 1 {
        anyhow::bail!("AIRFLOW__CORE__PARALLELISM must be >= 1");
    }
    let signals_vec = vec![SIGTERM, SIGINT, SIGUSR2];

    if config.enable_tracemalloc {
        anyhow::bail!("Tracemalloc is currently unimplemented for Rust");
    }
    let signals = Signals::new(&signals_vec).context("Failed to create signal handler")?;

    let handle = signals.handle();

    let signal_count = Arc::new(AtomicUsize::new(0));
    let signals_task = tokio::spawn(register_signals(signals, Arc::clone(&signal_count)));

    // TODO: Initialize and use the DatabaseCallbackSink as needed
    /*
    debug!("Using DatabaseCallbackSink as callback sink.");
    let mut callback_sink = DatabaseCallbackSink::new();
    let _ = callback_sink.send();
     */

    debug!("Using TokioExecutor as the executor.");
    // let mut executor = TokioExecutor::new(config.parallelism as usize);
    // let mut next_workload_id = 0;

    let mut sched = TCSJobScheduler::new()
        .await
        .context("Failed to create job scheduler")?;
    sched
        .start()
        .await
        .context("Failed to start job scheduler")?;

    let orphaned_tasks_check_interval =
        Duration::from_secs_f64(config.orphaned_tasks_check_interval);
    debug!(
        "Scheduling orphaned_tasks_check_interval every {:?}",
        orphaned_tasks_check_interval
    );
    adopt_or_reset_orphaned_tasks()
        .await
        .context("Initial check for orphaned tasks failed")?; // Add context

    check_triggers_timeout().await.context("Initial check for trigger timeouts failed")?;

    sched
        .add(TCSJob::new_repeated_async(
            orphaned_tasks_check_interval,
            |uuid, mut l| {
                Box::pin(async move {
                    if let Err(e) = adopt_or_reset_orphaned_tasks().await {
                        error!("Error in orphaned_tasks_check_interval job: {:?}", e);
                    }

                    // Query the next execution time for this job
                    let next_tick = l.next_tick_for_job(uuid).await;
                    match next_tick {
                        Ok(Some(ts)) => debug!(
                            "Next time for orphaned_tasks_check_interval job is {:?}",
                            ts
                        ),
                        Err(e) => {
                            error!("Could not get next tick for orphaned_tasks_check_interval job: {:?}", e);
                        }
                        _ => {
                            debug!("Could not get next tick for orphaned_tasks_check_interval job")
                        }
                    }
                })
            },
        ).context("Failed to add orphaned tasks check job")?)
        .await.context("Failed to schedule orphaned tasks check job")?;

    let trigger_timeout_check_interval = Duration::from_secs_f64(config.trigger_timeout_check_interval);
    debug!(
        "Scheduling check_trigger_timeouts every {:?}",
        trigger_timeout_check_interval
    );

    sched
        .add(TCSJob::new_repeated_async(
            trigger_timeout_check_interval,
            |uuid, mut l| {
                Box::pin(async move {
                    if let Err(e) = check_triggers_timeout().await {
                        error!("Error in check_trigger_timeouts job: {:?}", e);
                    }

                    // Query the next execution time for this job
                    let next_tick = l.next_tick_for_job(uuid).await;
                    match next_tick {
                        Ok(Some(ts)) => {
                            debug!("Next time for check_trigger_timeouts job is {:?}", ts)
                        }
                        Err(e) => {
                            error!("Could not get next tick for check_trigger_timeouts job: {:?}", e);
                        }
                        _ => debug!("Could not get next tick for check_trigger_timeouts job"),
                    }
                })
            },
        ).context("Failed to add trigger timeout check job")?)
        .await.context("Failed to schedule trigger timeout check job")?;

    while signal_count.load(Ordering::SeqCst) == 0 {
        // for _ in 0..2 {
        //     let workload = Workload {
        //         id: next_workload_id,
        //         command: format!("echo 'executing job {}'", next_workload_id),
        //     };
        //     info!("Queuing workload {}", workload.id);
        //     executor.queue_workload(workload).await;
        //     next_workload_id += 1;
        // }

        // Check for results from completed jobs
        // executor.read_results().await;

        info!("I just run for fun");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // executor.shutdown().await;
    handle.close();
    signals_task.await.context("Signal handling task failed")?;

    Ok(())
}

fn convert_sqlalchemy_to_postgres(sqlalchemy_url: &str, db_port: u16) -> Result<String> {
    let clean_url = sqlalchemy_url.replacen("+psycopg2", "", 1);
    let parsed_url = Url::parse(&clean_url).context("Failed to parse SQLAlchemy URL")?;
    let host = parsed_url.host_str().context("Database URL missing host")?;
    let port = parsed_url.port().unwrap_or(db_port);
    let username = parsed_url.username();
    let password = parsed_url.password().unwrap_or("airflow");
    let path_segments: Vec<_> = parsed_url
        .path_segments()
        .context("Database URL missing path segments")?
        .collect();
    let db_name = path_segments
        .get(0)
        .context("Database URL missing database name")?;

    Ok(format!(
        "postgresql://{}:{}/{}?user={}&password={}",
        host, port, db_name, username, password
    ))
}

async fn adopt_or_reset_orphaned_tasks() -> Result<()> {
    // Use anyhow::Result<()>
    use diesel_async::RunQueryDsl;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::pooled_connection::deadpool::Pool;
    let config = global_config();
    info!("Adopting or resetting orphaned tasks for active dag runs");
    let timeout = config.scheduler_health_check_threshold;
    let max_db_retries = config.max_db_retries;

    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(5))
        .with_jitter()
        .with_max_times(max_db_retries as usize);

    let task = || async {
        debug!("Calling SchedulerJob.adopt_or_reset_orphaned_tasks method");

        let conn_str = convert_sqlalchemy_to_postgres(&config.sql_alchemy_conn, 5432)
            .context("Failed to convert SQLAlchemy URL to Postgres URL")?;
        let conn_config =
            AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&conn_str);
        let _pool = Pool::builder(conn_config)
            .build()
            .context("Failed to build database connection pool")?; // Underscored to avoid conflict with "pool" table
        let mut conn = _pool
            .get()
            .await
            .context("Failed to get connection from pool")?;

        let timeout_interval_sql = format!("'{} seconds'", timeout);

        let changes = Job {
            state: Some("failed".to_string()),
            ..Default::default()
        };
        let query = diesel::update(job)
            .filter(job_type.eq("SchedulerJob"))
            .filter(state.eq("running"))
            .filter(latest_heartbeat.is_not_null())
            .filter(
                diesel::dsl::sql::<diesel::sql_types::Bool>("latest_heartbeat < now() - interval ")
                    .sql(&timeout_interval_sql),
            )
            .set(&changes);
        debug!(
            "Executing query: {:?}",
            diesel::debug_query::<diesel::pg::Pg, _>(&query)
        );

        let num_failed = RunQueryDsl::execute(query, &mut conn)
            .await
            .context("Failed to execute database update query")?;

        if num_failed > 0 {
            info!("Marked {} SchedulerJob instances as failed", num_failed);
        }

        Ok::<(), anyhow::Error>(())
    };

    task.retry(&backoff)
        .await
        .context("Retries for adopting/resetting orphaned tasks failed")?;

    Ok(())
}

async fn check_triggers_timeout() -> Result<()> {
    use crate::schema::task_instance::dsl::*;
    use chrono::Utc;
    use diesel_async::RunQueryDsl;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::pooled_connection::deadpool::Pool;
    use serde_json::json;

    let config = global_config();
    let max_db_retries = config.max_db_retries;

    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(5))
        .with_jitter()
        .with_max_times(max_db_retries as usize);

    let task = || async {
        debug!("Calling SchedulerJob.check_trigger_timeouts method");

        let conn_str = convert_sqlalchemy_to_postgres(&config.sql_alchemy_conn, 5432)
            .context("Failed to convert SQLAlchemy URL to Postgres URL")?;
        let conn_config =
            AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&conn_str);
        let _pool = Pool::builder(conn_config)
            .build()
            .context("Failed to build database connection pool")?;
        let mut conn = _pool.get().await.context("Failed to get connection from pool")?;

        let now_utc = Utc::now().naive_utc();

        let changes = TaskInstance {
            state: Some("scheduled".to_string()),
            next_method: Some("__fail__".to_string()),
            next_kwargs: Some(Serialized(json!({ "error": "Trigger Timeout" }))),
            scheduled_dttm: Some(now_utc),
            trigger_id: Some(None),
            ..Default::default()
        };

        let query = diesel::update(task_instance)
            .filter(state.eq("deferred"))
            .filter(trigger_timeout.lt(now_utc))
            .set(&changes);

        debug!(
            "Executing query: {:?}",
            diesel::debug_query::<diesel::pg::Pg, _>(&query)
        );

        let num_timed_out_tasks = RunQueryDsl::execute(query, &mut conn)
            .await
            .context("Failed to execute database update query for trigger timeouts")?;

        if num_timed_out_tasks > 0 {
            info!(
                "Timed out {} deferred tasks without fired triggers",
                num_timed_out_tasks
            );
        }

        Ok::<(), anyhow::Error>(())
    };

    task.retry(&backoff)
        .await
        .context("Retries for checking trigger timeouts failed")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_handle_signals_first_signal() {
        let signal_count = Arc::new(AtomicUsize::new(0));
        let signals = stream::iter(vec![SIGINT]);

        register_signals(signals, Arc::clone(&signal_count)).await;

        assert_eq!(signal_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_handle_signals_second_signal() {
        let signal_count = Arc::new(AtomicUsize::new(0));
        let signals = stream::iter(vec![SIGINT, SIGINT]);

        register_signals(signals, Arc::clone(&signal_count)).await;

        assert_eq!(signal_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_convert_sqlalchemy_to_postgres_invalid_url() {
        let invalid_url = "invalid-url";
        let result = convert_sqlalchemy_to_postgres(invalid_url, 5432);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Failed to parse SQLAlchemy URL"));
    }
}
