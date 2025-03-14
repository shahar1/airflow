pub mod models;
pub mod schema;
use crate::models::{DagModel, DagRun, DagVersion, SerializedDagModel};
use crate::schema::task_instance::dsl::task_instance;
use chrono::{DateTime, NaiveDateTime};
use clap::{Arg, ArgAction, Command};
use crossbeam_deque::{Injector, Steal};
use diesel::dsl::now;
use diesel::prelude::*;
use diesel::sql_types::Int4;
use log::{debug, info, error};
use signal_hook::consts::{SIGINT, SIGTERM, SIGUSR1, SIGUSR2};
use signal_hook::iterator::Signals;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fmt, thread};
use pyo3::prelude::*;
use pyo3::{PyResult, Python};
use pyo3::types::{PyDict, PyList};
use url::Url;
use uuid::Uuid;
use pythonize::{depythonize, pythonize};


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum DagRunState {
    QUEUED,
    RUNNING,
    SUCCESS,
    FAILED,
}

impl fmt::Display for DagRunState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

fn register_signals() -> Arc<AtomicBool> {
    let term = Arc::new(AtomicBool::new(false));
    // let term_clone = Arc::clone(&term);

    thread::spawn(move || {
        let mut signals = Signals::new(&[SIGINT, SIGTERM, SIGUSR2, SIGUSR1]).unwrap();
        for sig in signals.forever() {
            match sig {
                SIGINT | SIGTERM => {
                    println!("Exiting gracefully upon receiving signal {}", sig);
                    std::process::exit(0);
                }
                SIGUSR2 => {
                    // Handle SIGUSR2 (_debug_dump equivalent)
                }
                SIGUSR1 => {
                    // Handle SIGUSR1
                }
                _ => unreachable!(),
            }
        }
    });

    term
}

fn convert_sqlalchemy_to_postgres(sqlalchemy_url: &str, db_port: u16) -> String {
    let clean_url = sqlalchemy_url.replacen("+psycopg2", "", 1);
    let parsed_url = Url::parse(&clean_url).expect("Failed to parse URL");
    let host = parsed_url.host_str().unwrap_or("localhost");
    let port = parsed_url.port().unwrap_or(db_port as u16);
    let username = parsed_url.username();
    let password = parsed_url.password().unwrap_or("airflow");
    let path_segments: Vec<_> = parsed_url.path_segments().unwrap().collect();
    let db_name = path_segments.get(0).unwrap_or(&"");

    format!(
        "postgresql://{}:{}/{}?user={}&password={}",
        host, port, db_name, username, password
    )
}

impl DagRun {
    fn get_running_dag_runs_to_examine(
        conn: &mut PgConnection,
        max_dagruns_per_loop_to_schedule: i64,
    ) -> QueryResult<Vec<DagRun>> {
        use crate::schema::backfill_dag_run::dsl as backfill_dag_run_dsl;
        use crate::schema::backfill_dag_run::table as t_backfill_dag_run;
        use crate::schema::dag::dsl as dag_model_dsl;
        use crate::schema::dag::table as t_dag_model;
        use crate::schema::dag_run::dsl as dag_run_dsl;
        use crate::schema::dag_run::table as dag_run_table;

        // First, select the IDs of the dag_run rows with the lock applied.
        let locked_ids: Vec<i32> = dag_run_table
            .select(dag_run_dsl::id)
            .filter(dag_run_dsl::state.eq("running"))
            .filter(dag_run_dsl::run_after.le(now))
            .for_update()
            .skip_locked()
            .limit(max_dagruns_per_loop_to_schedule)
            .load(conn)?;

        // Then, join against the locked subquery result.
        let query = dag_run_table
            .inner_join(t_dag_model.on(dag_run_dsl::dag_id.eq(dag_model_dsl::dag_id)))
            .left_outer_join(
                t_backfill_dag_run
                    .on(backfill_dag_run_dsl::dag_run_id.eq(backfill_dag_run_dsl::dag_run_id)),
            )
            .select(DagRun::as_select())
            .filter(dag_run_dsl::id.eq_any(locked_ids))
            .order(backfill_dag_run_dsl::sort_ordinal.asc().nulls_first())
            .then_order_by(dag_run_dsl::last_scheduling_decision.asc().nulls_first())
            .then_order_by(dag_run_dsl::run_after)
            .load::<DagRun>(conn)?;

        println!("Running dag runs to examine: {:?}", query);

        Ok(query)
    }
}

struct DataInterval {
    start: NaiveDateTime,
    end: NaiveDateTime,
}

impl DataInterval {
    fn exact(at: NaiveDateTime) -> Self {
        Self { start: at, end: at }
    }
}

impl DagModel {

    fn dags_needing_dagruns(
        conn: &mut PgConnection,
        num_dags_per_dagrun_query: i64,
    ) -> QueryResult<Vec<DagModel>> {
        use crate::schema::dag::dsl::*;
        use crate::schema::dag::table as dag_table;
        use diesel::dsl::now;

        let query = dag_table
            .select(DagModel::as_select())
            .filter(is_paused.eq(false))
            .filter(is_active.eq(true))
            .filter(has_import_errors.eq(false))
            .filter(next_dagrun_create_after.le(now))
            .order(next_dagrun_create_after.asc())
            .limit(num_dags_per_dagrun_query)
            .for_update()
            .skip_locked()
            .load::<DagModel>(conn)?;

        Ok(query)
    }

    fn get_next_data_interval() {

    }
}

struct SerializedDag {

}

struct DagBag {
    dags: HashMap<String, SerializedDagModel>,
}

impl DagBag {
    fn new(conn: &mut PgConnection) -> Self {
        DagBag {
            dags: HashMap::new()
        }
    }

    fn _add_dag(&mut self, dag: SerializedDagModel) -> Option<()> {
        self.dags.insert(dag.dag_id.clone(), dag);
        None
    }


    fn _add_dag_from_db(&mut self, dag_id: &str, conn: &mut PgConnection) -> Option<SerializedDagModel> {

        use crate::schema::serialized_dag::dsl as serialized_dag_dsl;
        use crate::schema::serialized_dag::table as t_serialized_dag;
        let row: Option<SerializedDagModel> = t_serialized_dag
            .filter(serialized_dag_dsl::dag_id.eq(dag_id))
            .first::<SerializedDagModel>(conn)
            .optional()
            .expect("Error loading SerializedDag")
            ;

        if let Some(row) = row {
            // let mut dag = row.clone();
            // self.dags.insert(row.dag_id.clone(), row.clone());

            //
            let py_dict = Python::with_gil(|py| -> PyResult<String> {
                let serialized_objects = py.import("airflow.serialization.serialized_objects")?;
                let serialized_dag = serialized_objects.getattr("SerializedDAG")?;
                let dict = pythonize(py, &row.data.clone()).unwrap().into_py(py);
                let dag = serialized_dag.call_method1("from_dict", (dict, ))?;
                Ok(format!("{:?}", dag))
            });

            println!("Added DAG to the bag {:?}", py_dict);
            Some(row)
        }
        else {
            None
        }
    }

    fn get_dag(&mut self, dag_id: &str, conn: &mut PgConnection) -> Option<SerializedDagModel> {
        if !self.dags.contains_key(dag_id) {
            self._add_dag_from_db(dag_id, conn)
        }
        else {
            self.dags.get(dag_id).cloned()
        }
    }
}

impl DagVersion {
    fn get_latest_version(dag_id: &str, conn: &mut PgConnection) -> QueryResult<DagVersion> {
        use crate::schema::dag_version::dsl as dag_version_dsl;
        use crate::schema::dag_version::table as t_dag_version;
        t_dag_version
            .filter(dag_version_dsl::dag_id.eq(dag_id))
            .order(dag_version_dsl::created_at.desc())
            .first::<DagVersion>(conn)
    }
}

fn _create_dagruns_for_dags(
    conn: &mut PgConnection,
    num_dags_per_dagrun_query: i64,
    dag_bag: &mut DagBag,
) -> QueryResult<Vec<(String, Option<NaiveDateTime>)>> {
    use crate::schema::dag_run::dsl as dag_run_dsl;
    use crate::schema::dag_run::table as dag_run_table;
    let dag_models =
        DagModel::dags_needing_dagruns(conn, num_dags_per_dagrun_query).unwrap_or_default();

    println!("Dags needing dagruns are: {:?}", dag_models);


    let existing_dagruns: Vec<(String, Option<NaiveDateTime>)> = dag_run_table
        .select((dag_run_dsl::dag_id, dag_run_dsl::logical_date))
        .filter(
            (dag_run_dsl::dag_id)
                .eq_any(dag_models.iter().map(|dm| dm.dag_id.clone()))
                .and(
                    (dag_run_dsl::logical_date)
                        .eq_any(dag_models.iter().map(|dm| dm.next_dagrun_create_after)),
                ),
        )
        .distinct()
        .load(conn)?;

    // TODO: Backfill

    // Iterate over dag_models and print them
    for dag_model in dag_models {
        if let None = dag_bag.get_dag(&dag_model.dag_id, conn) {
            error!("DAG '%s' not found in serialized_dag table {:?}", dag_model.dag_id);
            continue;
        }

        let latest_dag_version = DagVersion::get_latest_version(&dag_model.dag_id, conn)?;
        println!("{:?}", latest_dag_version)
    }

    println!("existing dagruns: {:?}", existing_dagruns);

    Ok(existing_dagruns)
}

fn main() {
    env_logger::init();
    let matches = Command::new("RustScheduler") // requires `cargo` feature
    .arg(
        Arg::new("scheduler_idle_sleep_time")
            .long("scheduler_idle_sleep_time")
            .help("Time to sleep when the scheduler is idle")
            .action(ArgAction::Set)
            .default_value("1")
    )
    .arg(
        Arg::new("task_instance_heartbeat_timeout")
            .long("task_instance_heartbeat_timeout")
            .help(
                "Local task jobs periodically heartbeat to the DB. If the job has
                   not heartbeat in this many seconds, the scheduler will mark the
                   associated task instance as failed and will re-schedule the task.",
            )
            .action(ArgAction::Set)
            .default_value("300")
    )
    .arg(
        Arg::new("dag_stale_not_seen_duration")
            .long("dag_stale_not_seen_duration")
            .help("Duration for considering a DAG as stale if not seen")
            .action(ArgAction::Set)
            .default_value("600")
    )
    .arg(
        Arg::new("task_queued_timeout")
            .long("task_queued_timeout")
            .help("Timeout for tasks in the queue")
            .action(ArgAction::Set)
            .default_value("120")
    )
    .arg(
        Arg::new("num_stuck_in_queued_retries")
            .long("num_stuck_in_queued_retries")
            .help("Number of retries for stuck tasks in the queue")
            .action(ArgAction::Set)
            .default_value("2")
    )
    .arg(
        Arg::new("sql_alchemy_conn")
            .long("sql_alchemy_conn")
            .help("The SQL Alchemy connection to use (will be converted to postgres connection string)")
            .action(ArgAction::Set)
            .default_value("postgresql+psycopg2://postgres:airflow@localhost/airflow")
    )
    .arg(
        Arg::new("db_port")
            .long("db_port")
            .help("Database port")
            .action(ArgAction::Set)
            .value_parser(clap::value_parser!(u16))
            .default_value("25433")
    )
    .arg(
        Arg::new("max_dagruns_per_loop_to_schedule")
            .long("max_dagruns_per_loop_to_schedule")
            .help("The maximum number of DAG Runs to create within a single scheduler loop")
            .action(ArgAction::Set)
            .default_value("20")
    )
    .get_matches();

    // Configuration
    let sql_alchemy_conn = matches.get_one::<String>("sql_alchemy_conn").unwrap();
    fn _get_config_value(matches: &clap::ArgMatches, key: &str) -> u64 {
        matches.get_one::<String>(key).unwrap().parse().unwrap()
    }

    let scheduler_idle_sleep_time = _get_config_value(&matches, "scheduler_idle_sleep_time");
    let _task_instance_heartbeat_timeout =
        _get_config_value(&matches, "task_instance_heartbeat_timeout");
    let _dag_stale_not_seen_duration = _get_config_value(&matches, "dag_stale_not_seen_duration");
    let _task_queued_timeout = _get_config_value(&matches, "task_queued_timeout");
    let _num_stuck_in_queued_retries = _get_config_value(&matches, "num_stuck_in_queued_retries");
    let max_dagruns_per_loop_to_schedule: i64 =
        _get_config_value(&matches, "max_dagruns_per_loop_to_schedule") as i64;

    let num_dags_per_dagrun_query = 10; // TODO: Make this configurable

    let db_port: u16 = *matches.get_one("db_port").expect("required");

    info!("Starting the Rust scheduler");

    let postgres_url = convert_sqlalchemy_to_postgres(sql_alchemy_conn, db_port);

    let mut connection = PgConnection::establish(&postgres_url)
        .unwrap_or_else(|_| panic!("Error connecting to db {}", postgres_url));
    info!("Successfully connected to the database {}", postgres_url);

    debug!(
        "There are currently {} task instances",
        task_instance
            .count()
            .get_result::<i64>(&mut connection)
            .unwrap()
    );

    let term = register_signals();

    struct WorkloadTaskInstance {
        pub id: Uuid,
        pub task_id: String,
        pub dag_id: String,
        pub run_id: String,
        pub map_index: i32,
        pub try_number: i32,
        pub pool_slots: i32,
        pub queue: Option<String>,
        pub priority_weight: Option<i32>,
        pub executor_config: Option<Vec<u8>>,
    }

    #[derive(Debug)]
    enum TaskInstanceState {
        /// Task vanished from DAG before it ran
        REMOVED,
        SCHEDULED,
        QUEUED,
        RUNNING,
        SUCCESS,
        RESTARTING,
        FAILED,
        UP_FOR_RETRY,
        UP_FOR_RESCHEDULE,
        UPSTREAM_FAILED,
        SKIPPED,
        DEFERRED,
    }

    impl fmt::Display for TaskInstanceState {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", format!("{:?}", self).to_lowercase())
        }
    }

    struct ExecuteTask {
        pub task_instance: WorkloadTaskInstance,
    }

    struct TaskInstanceStateType {
        pub task_instance: WorkloadTaskInstance,
        pub state: TaskInstanceState,
    }

    let activity_queue = Injector::<ExecuteTask>::new();
    let result_queue = VecDeque::<TaskInstanceStateType>::new();
    let mut dag_bag = DagBag::new(&mut connection);

    _create_dagruns_for_dags(&mut connection, num_dags_per_dagrun_query, &mut dag_bag);

   // while !term.load(Ordering::Relaxed) {
   //      println!("Doing some work");
   //      _create_dagruns_for_dags(&mut connection, num_dags_per_dagrun_query, &mut dag_bag);
   //      let dag_run = DagRun::get_running_dag_runs_to_examine(
   //          &mut connection,
   //          max_dagruns_per_loop_to_schedule,
   //      );
   //
   //
   //
   //      thread::sleep(std::time::Duration::from_secs(scheduler_idle_sleep_time));
   //  }
}
