use crate::Dag;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use dotenvy::dotenv;
use std::env;
use crate::schema::dag as dags; // Use alias for clarity


// This struct maps directly to the `dag` table's columns.
// We derive `Insertable` to create new records and `AsChangeset` to update existing ones.
#[derive(Queryable, Insertable, AsChangeset, Debug)]
#[diesel(table_name = dags)]
pub struct NewDag {
    pub dag_id: String,
    pub is_paused: bool,
    pub is_stale: bool,
    pub last_parsed_time: Option<DateTime<Utc>>,
    pub last_expired: Option<DateTime<Utc>>,
    pub fileloc: Option<String>,
    pub relative_fileloc: Option<String>,
    pub owners: Option<String>,
    pub dag_display_name: Option<String>,
    pub description: Option<String>,
    pub timetable_summary: Option<String>,
    pub timetable_description: Option<String>,
    pub deadline: Option<serde_json::Value>,
    pub max_active_tasks: Option<i32>,
    pub max_active_runs: Option<i32>,
    pub max_consecutive_failed_dag_runs: Option<i32>,
    pub has_task_concurrency_limits: Option<bool>,
    pub has_import_errors: bool,
    // Fields not in our source `Dag` struct are set to None or a default.
    // We can omit them here if they are not part of an update.
    // For this example, we explicitly define all insertable fields.
    pub bundle_name: Option<String>,
    pub bundle_version: Option<String>,
    pub asset_expression: Option<serde_json::Value>,
    pub next_dagrun: Option<DateTime<Utc>>,
    pub next_dagrun_data_interval_start: Option<DateTime<Utc>>,
    pub next_dagrun_data_interval_end: Option<DateTime<Utc>>,
    pub next_dagrun_create_after: Option<DateTime<Utc>>,
}

/// Establishes a connection to the PostgreSQL database.
pub fn establish_connection() -> Result<PgConnection> {
    dotenv().ok(); // Load .env file
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    PgConnection::establish(&database_url)
        .with_context(|| format!("Error connecting to {}", database_url))
}

/// Saves a single DAG to the database.
/// If a DAG with the same `dag_id` already exists, it will be updated.
pub fn save_dag(conn: &mut PgConnection, dag: &Dag) -> Result<()> {
    let new_dag = NewDag::from(dag);

    // This is an "UPSERT" operation.
    // It will INSERT the `new_dag`. If that violates the primary key constraint (dag_id),
    // it will instead UPDATE the existing row, setting the columns from `new_dag`.
    diesel::insert_into(dags::table)
        .values(&new_dag)
        .on_conflict(dags::dag_id)
        .do_update()
        .set(&new_dag)
        .execute(conn)
        .with_context(|| format!("Error saving DAG '{}'", new_dag.dag_id))?;

    Ok(())
}


// This is the core logic for mapping our application's `Dag` struct
// to the database-specific `NewDag` struct.
impl From<&Dag> for NewDag {
    fn from(dag: &Dag) -> Self {
        // Improvise 'owners' from default_args if possible, otherwise use a default.
        let owners = dag.default_args
            .as_ref()
            .and_then(|args| args.get("owner").or_else(|| args.get("owners")))
            .and_then(|owner_val| owner_val.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "airflow".to_string());

        NewDag {
            dag_id: dag.dag_id.clone(),
            // A DAG is paused on creation by default in Airflow
            is_paused: dag.is_paused_upon_creation.unwrap_or(true),
            // A newly parsed DAG is not stale.
            is_stale: false,
            // We are parsing it right now.
            last_parsed_time: Some(Utc::now()),
            // Not in source, so we set to NULL.
            last_expired: None,
            fileloc: dag.fileloc.clone(),
            relative_fileloc: dag.relative_fileloc.clone(),
            owners: Some(owners),
            dag_display_name: dag.dag_display_name.clone(),
            description: dag.description.clone(),
            timetable_summary: dag.timetable.clone(),
            timetable_description: dag.timetable.clone(), // Use timetable for both
            deadline: dag.deadline.as_ref().and_then(|d| serde_json::to_value(d).ok()),
            // SQL INTEGER is i32 in Diesel
            max_active_tasks: dag.max_active_tasks.map(|v| v as i32),
            max_active_runs: dag.max_active_runs.map(|v| v as i32),
            max_consecutive_failed_dag_runs: dag.max_consecutive_failed_dag_runs.map(|v| v as i32),
            // Assume false unless we have information otherwise.
            has_task_concurrency_limits: Some(false),
            // If parsing succeeded, there are no import errors.
            has_import_errors: false,

            // These fields are managed by the Airflow scheduler, not the parser.
            // So we default them all to NULL.
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