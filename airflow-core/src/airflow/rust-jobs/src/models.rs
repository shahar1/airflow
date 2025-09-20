use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel::{AsChangeset, Associations, Identifiable, Queryable, Selectable};
use serde::ser::{Serialize, Serializer};
use crate::schema::{dag, dag_run, job, task_instance};
use crate::serialized_objects::Serialized;

#[derive(Queryable, Selectable, Identifiable, Associations, AsChangeset, Default)]
#[diesel(belongs_to(DagModel, foreign_key = dag_id))]
#[diesel(check_for_backend(diesel::pg::Pg))]
#[diesel(table_name = job)]
pub struct Job {
    pub id: i32,
    pub dag_id: Option<String>,
    pub state: Option<String>,
    pub job_type: Option<String>,
    pub start_date: Option<NaiveDateTime>,
    pub end_date: Option<NaiveDateTime>,
    pub latest_heartbeat: Option<NaiveDateTime>,
    pub executor_class: Option<String>,
    pub hostname: Option<String>,
    pub unixname: Option<String>,
}

#[derive(Queryable, Identifiable, Associations, AsChangeset, Default)]
#[diesel(primary_key(task_id, dag_id, run_id, map_index))]
#[diesel(belongs_to(Job, foreign_key = queued_by_job_id))]
#[diesel(table_name = task_instance)]
pub struct TaskInstance {
    pub task_id: String,
    pub dag_id: String,
    pub run_id: String,
    pub map_index: i32,
    pub state: Option<String>,
    pub trigger_timeout: Option<NaiveDateTime>,
    pub next_method: Option<String>,
    pub next_kwargs: Option<Serialized>,
    pub scheduled_dttm: Option<NaiveDateTime>,
    pub trigger_id: Option<Option<i32>>,
    pub queued_by_job_id: Option<i32>,
}

#[derive(Queryable, Identifiable, Associations)]
#[diesel(belongs_to(Job, foreign_key = creating_job_id))]
#[diesel(table_name = dag_run)]
pub struct DagRun {
    pub id: i32,
    pub creating_job_id: Option<i32>,
}

#[derive(Queryable, Identifiable)]
#[diesel(primary_key(dag_id))]
#[diesel(table_name = dag)]
pub struct DagModel {
    #[diesel(sql_type = Text)]
    pub dag_id: String,
}
