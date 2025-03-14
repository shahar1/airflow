use crate::schema::{dag_run, task_instance, dag, serialized_dag, dag_version};
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Queryable, Insertable, AsChangeset, Serialize, Deserialize, Debug)]
#[diesel(table_name = task_instance)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskInstance {
    pub id: Uuid,
    pub task_id: String,
    pub dag_id: String,
    pub run_id: String,
    pub map_index: i32,
    pub start_date: Option<NaiveDateTime>,
    pub end_date: Option<NaiveDateTime>,
    pub duration: Option<f64>,
    pub state: Option<String>,
    pub try_id: Uuid,
    pub try_number: i32,
    pub max_tries: i32,
    pub hostname: Option<String>,
    pub unixname: Option<String>,
    pub pool: String,
    pub pool_slots: i32,
    pub queue: Option<String>,
    pub priority_weight: Option<i32>,
    pub operator: Option<String>,
    pub custom_operator_name: Option<String>,
    pub queued_dttm: Option<NaiveDateTime>,
    pub scheduled_dttm: Option<NaiveDateTime>,
    pub queued_by_job_id: Option<i32>,
    pub last_heartbeat_at: Option<NaiveDateTime>,
    pub pid: Option<i32>,
    pub executor: Option<String>,
    pub executor_config: Option<Vec<u8>>,
    pub updated_at: Option<NaiveDateTime>,
    pub rendered_map_index: Option<String>,
    pub external_executor_id: Option<String>,
    pub trigger_id: Option<i32>,
    pub trigger_timeout: Option<NaiveDateTime>,
    pub next_method: Option<String>,
    pub next_kwargs: Option<serde_json::Value>,
    pub task_display_name: Option<String>,
    pub dag_version_id: Option<Uuid>,
}

#[derive(Selectable, Queryable, Identifiable, Serialize, Deserialize, Debug)]
#[diesel(table_name = dag)]
#[diesel(primary_key(dag_id))]
#[derive(Clone)]
pub struct DagModel {
    pub dag_id: String,
    pub is_paused: Option<bool>,
    pub is_active: Option<bool>,
    pub last_parsed_time: Option<NaiveDateTime>,
    pub last_expired: Option<NaiveDateTime>,
    pub fileloc: Option<String>,
    pub relative_fileloc: Option<String>,
    pub bundle_name: Option<String>,
    pub bundle_version: Option<String>,
    pub owners: Option<String>,
    pub dag_display_name: Option<String>,
    pub description: Option<String>,
    pub timetable_summary: Option<String>,
    pub timetable_description: Option<String>,
    pub asset_expression: Option<serde_json::Value>,
    pub max_active_tasks: i32,
    pub max_active_runs: Option<i32>,
    pub max_consecutive_failed_dag_runs: i32,
    pub has_task_concurrency_limits: bool,
    pub has_import_errors: Option<bool>,
    pub next_dagrun: Option<NaiveDateTime>,
    pub next_dagrun_data_interval_start: Option<NaiveDateTime>,
    pub next_dagrun_data_interval_end: Option<NaiveDateTime>,
    pub next_dagrun_create_after: Option<NaiveDateTime>,
}

#[derive(Selectable, Queryable, Insertable, AsChangeset, Serialize, Deserialize, Debug)]
#[diesel(table_name = dag_run)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DagRun {
    pub id: i32,
    pub dag_id: String,
    pub queued_at: Option<NaiveDateTime>,
    pub logical_date: Option<NaiveDateTime>,
    pub start_date: Option<NaiveDateTime>,
    pub end_date: Option<NaiveDateTime>,
    pub state: Option<String>,
    pub run_id: String,
    pub creating_job_id: Option<i32>,
    pub run_type: String,
    pub triggered_by: Option<String>,
    pub conf: Option<serde_json::Value>,
    pub data_interval_start: Option<NaiveDateTime>,
    pub data_interval_end: Option<NaiveDateTime>,
    pub run_after: NaiveDateTime,
    pub last_scheduling_decision: Option<NaiveDateTime>,
    pub log_template_id: Option<i32>,
    pub updated_at: Option<NaiveDateTime>,
    pub clear_number: i32,
    pub backfill_id: Option<i32>,
    pub bundle_version: Option<String>,
}

#[derive(Selectable, Queryable, Insertable, AsChangeset, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = serialized_dag)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct SerializedDagModel {
    pub id: Uuid,
    pub dag_id: String,
    pub data: Option<serde_json::Value>,
    pub data_compressed: Option<Vec<u8>>,
    pub created_at: NaiveDateTime,
    pub last_updated: NaiveDateTime,
    pub dag_hash: String,
    pub dag_version_id: Uuid,
}

#[derive(Selectable, Queryable, Insertable, AsChangeset, Serialize, Deserialize, Debug)]
#[diesel(table_name = dag_version)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DagVersion {
    pub id: Uuid,
    pub version_number: i32,
    pub dag_id: String,
    pub bundle_name: String,
    pub bundle_version: Option<String>,
    pub created_at: NaiveDateTime,
    pub last_updated: NaiveDateTime
}
