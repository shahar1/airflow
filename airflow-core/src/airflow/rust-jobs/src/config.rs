use std::env;
use std::sync::OnceLock;

fn get_env_var<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|s| s.parse::<T>().ok())
        .unwrap_or(default)
}

fn get_env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|s| match s.to_lowercase().as_str() {
            "true" | "t" | "1" | "yes" | "y" => Some(true),
            "false" | "f" | "0" | "no" | "n" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SchedulerConfig {
    pub allowed_run_id_pattern: String,
    pub catchup_by_default: bool,
    pub create_cron_data_intervals: bool,
    pub create_delta_data_intervals: bool,
    pub dag_stale_not_seen_duration: i64,
    pub enable_health_check: bool,
    pub enable_tracemalloc: bool,
    pub ignore_first_depends_on_past_by_default: bool,
    pub job_heartbeat_sec: f64,
    pub max_db_retries: i64,
    pub max_dagruns_per_loop_to_schedule: i64,
    pub max_dagruns_to_create_per_loop: i64,
    pub max_tis_per_query: i64,
    pub num_runs: i64,
    pub parallelism: i64,
    pub orphaned_tasks_check_interval: f64,
    pub parsing_cleanup_interval: i64,
    pub pool_metrics_interval: f64,
    pub running_metrics_interval: f64,
    pub scheduler_health_check_server_host: String,
    pub scheduler_health_check_server_port: i64,
    pub scheduler_health_check_threshold: i64,
    pub scheduler_heartbeat_sec: i64,
    pub scheduler_idle_sleep_time: f64,
    pub sql_alchemy_conn: String,
    pub task_instance_heartbeat_sec: i64,
    pub task_instance_heartbeat_timeout: i64,
    pub task_instance_heartbeat_timeout_detection_interval: f64,
    pub task_queued_timeout: f64,
    pub task_queued_timeout_check_interval: f64,
    pub trigger_timeout_check_interval: f64,
    pub use_job_schedule: bool,
    pub use_row_level_locking: bool,
}

impl SchedulerConfig {
    pub fn from_env() -> Self {
        Self {
            // Non-scheduler specific configs
            parallelism: get_env_var("AIRFLOW__CORE__PARALLELISM", 32),
            max_db_retries: get_env_var("AIRFLOW__DATABASE__MAX_DB_RETRIES", 3),
            sql_alchemy_conn: get_env_var(
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
                "postgresql+psycopg2://postgres:airflow@localhost:25433/airflow".to_string(),
            ),

            // Scheduler specific configs
            allowed_run_id_pattern: get_env_var(
                "AIRFLOW__SCHEDULER__ALLOWED_RUN_ID_PATTERN",
                "^[A-Za-z0-9_.~:+-]+$".to_string(),
            ),
            catchup_by_default: get_env_bool("AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT", false),
            create_cron_data_intervals: get_env_bool(
                "AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS",
                false,
            ),
            create_delta_data_intervals: get_env_bool(
                "AIRFLOW__SCHEDULER__CREATE_DELTA_DATA_INTERVALS",
                false,
            ),
            dag_stale_not_seen_duration: get_env_var(
                "AIRFLOW__SCHEDULER__DAG_STALE_NOT_SEEN_DURATION",
                600,
            ),
            enable_health_check: get_env_bool("AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK", false),
            enable_tracemalloc: get_env_bool("AIRFLOW__SCHEDULER__ENABLE_TRACEMALLOC", false),
            ignore_first_depends_on_past_by_default: get_env_bool(
                "AIRFLOW__SCHEDULER__IGNORE_FIRST_DEPENDS_ON_PAST_BY_DEFAULT",
                true,
            ),
            job_heartbeat_sec: get_env_var("AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC", 5.0),
            max_dagruns_per_loop_to_schedule: get_env_var(
                "AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE",
                20,
            ),
            max_dagruns_to_create_per_loop: get_env_var(
                "AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP",
                10,
            ),
            max_tis_per_query: get_env_var("AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY", 16),
            num_runs: get_env_var("AIRFLOW__SCHEDULER__NUM_RUNS", -1),
            orphaned_tasks_check_interval: get_env_var(
                "AIRFLOW__SCHEDULER__ORPHANED_TASKS_CHECK_INTERVAL",
                300.0,
            ),
            parsing_cleanup_interval: get_env_var(
                "AIRFLOW__SCHEDULER__PARSING_CLEANUP_INTERVAL",
                60,
            ),
            pool_metrics_interval: get_env_var("AIRFLOW__SCHEDULER__POOL_METRICS_INTERVAL", 5.0),
            running_metrics_interval: get_env_var(
                "AIRFLOW__SCHEDULER__RUNNING_METRICS_INTERVAL",
                30.0,
            ),
            scheduler_health_check_server_host: get_env_var(
                "AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_HOST",
                "0.0.0.0".to_string(),
            ),
            scheduler_health_check_server_port: get_env_var(
                "AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_PORT",
                8974,
            ),
            scheduler_health_check_threshold: get_env_var(
                "AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD",
                30,
            ),
            scheduler_heartbeat_sec: get_env_var(
                "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC",
                5,
            ),
            scheduler_idle_sleep_time: get_env_var(
                "AIRFLOW__SCHEDULER__SCHEDULER_IDLE_SLEEP_TIME",
                1.0,
            ),
            task_instance_heartbeat_sec: get_env_var(
                "AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_SEC",
                0,
            ),
            task_instance_heartbeat_timeout: get_env_var(
                "AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT",
                300,
            ),
            task_instance_heartbeat_timeout_detection_interval: get_env_var(
                "AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL",
                10.0,
            ),
            task_queued_timeout: get_env_var("AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT", 600.0),
            task_queued_timeout_check_interval: get_env_var(
                "AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT_CHECK_INTERVAL",
                120.0,
            ),
            trigger_timeout_check_interval: get_env_var(
                "AIRFLOW__SCHEDULER__TRIGGER_TIMEOUT_CHECK_INTERVAL",
                15.0,
            ),
            use_job_schedule: get_env_bool("AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE", true),
            use_row_level_locking: get_env_bool("AIRFLOW__SCHEDULER__USE_ROW_LEVEL_LOCKING", true),
        }
    }
}

pub fn global_config<'a>() -> &'a SchedulerConfig {
    static CONFIG: OnceLock<SchedulerConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        SchedulerConfig::from_env()
    })
}