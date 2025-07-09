use crate::schema::dag as dags;
use crate::{EdgeInfoType};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct Dag {
    pub dag_id: String,
    pub description: Option<String>,
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    pub max_active_tasks: Option<u32>,
    pub fail_fast: Option<bool>,
    pub max_active_runs: Option<u32>,
    pub doc_md: Option<String>,
    pub fileloc: Option<String>,
    pub edge_info:Option<std::collections::HashMap<String, std::collections::HashMap<String, EdgeInfoType>>>,
    pub max_consecutive_failed_dag_runs: Option<u32>,
    pub render_template_as_native_obj: Option<bool>,
    pub owner_links: Option<std::collections::HashMap<String, String>>,
    pub tags: Option<Vec<String>>,
    pub is_paused_upon_creation: Option<bool>,
    pub default_args: Option<std::collections::HashMap<String, serde_json::Value>>,
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
    pub disable_bundle_versioning: bool,
    pub relative_fileloc: Option<String>,
    pub catchup: bool,
    pub dag_display_name: Option<String>,
    pub deadline: Option<std::collections::HashMap<String, serde_json::Value>>,
    pub timetable: Option<String>,
    pub timezone: Option<String>,
    pub access_control: Option<String>,
    pub dagrun_timeout: Option<String>,
    pub task_group: Option<String>,
}

#[derive(Queryable, Insertable, AsChangeset, Debug)]
#[diesel(table_name = dags)]
pub struct NewDag {
    pub dag_id: String,
    pub is_paused: Option<bool>,
    pub is_stale: Option<bool>,
    pub last_parsed_time: Option<DateTime<Utc>>,
    pub last_expired: Option<DateTime<Utc>>,
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
    pub deadline: Option<serde_json::Value>,
    pub max_active_tasks: i32,
    pub max_active_runs: Option<i32>,
    pub max_consecutive_failed_dag_runs: i32,
    pub has_task_concurrency_limits: bool,
    pub has_import_errors: Option<bool>,
    pub next_dagrun: Option<DateTime<Utc>>,
    pub next_dagrun_data_interval_start: Option<DateTime<Utc>>,
    pub next_dagrun_data_interval_end: Option<DateTime<Utc>>,
    pub next_dagrun_create_after: Option<DateTime<Utc>>,
}

pub fn get_connection_pool() -> Pool<ConnectionManager<PgConnection>> {
    dotenv().ok(); // Load .env file
    let url = env::var("DATABASE_URL")
        .context("DATABASE_URL must be set")
        .unwrap();
    let manager = ConnectionManager::<PgConnection>::new(url);
    Pool::builder()
        .test_on_check_out(true)
        .build(manager)
        .expect("Could not build connection pool")
}

pub fn save_dag(conn: &mut PgConnection, dag: &Dag) -> Result<()> {
    let new_dag = NewDag::from(dag);
    diesel::insert_into(dags::table)
        .values(&new_dag)
        .on_conflict(dags::dag_id)
        .do_update()
        .set(&new_dag)
        .execute(conn)
        .with_context(|| format!("Error saving DAG '{}'", new_dag.dag_id))?;

    Ok(())
}


impl From<&Dag> for NewDag {
    fn from(dag: &Dag) -> Self {
        let owners = dag.default_args
            .as_ref()
            .and_then(|args| args.get("owner").or_else(|| args.get("owners")))
            .and_then(|owner_val| owner_val.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "airflow".to_string());

        NewDag {
            dag_id: dag.dag_id.clone(),
            is_paused: Option::from(dag.is_paused_upon_creation.unwrap_or(true)),
            is_stale: Option::from(false),
            last_parsed_time: Some(Utc::now()),
            last_expired: None,
            fileloc: dag.fileloc.clone(),
            relative_fileloc: dag.relative_fileloc.clone(),
            owners: Some(owners),
            dag_display_name: dag.dag_display_name.clone(),
            description: dag.description.clone(),
            timetable_summary: dag.timetable.clone(),
            timetable_description: dag.timetable.clone(),
            deadline: dag.deadline.as_ref().and_then(|d| serde_json::to_value(d).ok()),
            max_active_tasks: dag.max_active_tasks.map(|v| v as i32).unwrap_or(0),
            max_active_runs: dag.max_active_runs.map(|v| v as i32),
            max_consecutive_failed_dag_runs: dag.max_consecutive_failed_dag_runs.map(|v| v as i32).unwrap_or(0),
            has_task_concurrency_limits: false,
            has_import_errors: Option::from(false),
            bundle_name: None,
            bundle_version: None,
            asset_expression: None,
            next_dagrun: None,
            next_dagrun_data_interval_start: None,
            next_dagrun_data_interval_end: None,
            next_dagrun_create_after: None,
        }
    }
}
