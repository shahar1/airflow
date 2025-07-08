// @generated automatically by Diesel CLI.

diesel::table! {
    dag (dag_id) {
        #[max_length = 250]
        dag_id -> Varchar,
        is_paused -> Bool,
        is_stale -> Bool,
        last_parsed_time -> Nullable<Timestamptz>,
        last_expired -> Nullable<Timestamptz>,
        #[max_length = 2000]
        fileloc -> Nullable<Varchar>,
        #[max_length = 2000]
        relative_fileloc -> Nullable<Varchar>,
        #[max_length = 250]
        bundle_name -> Nullable<Varchar>,
        #[max_length = 200]
        bundle_version -> Nullable<Varchar>,
        #[max_length = 2000]
        owners -> Nullable<Varchar>,
        #[max_length = 2000]
        dag_display_name -> Nullable<Varchar>,
        description -> Nullable<Text>,
        timetable_summary -> Nullable<Text>,
        #[max_length = 1000]
        timetable_description -> Nullable<Varchar>,
        asset_expression -> Nullable<Json>,
        deadline -> Nullable<Json>,
        max_active_tasks -> Nullable<Int4>,
        max_active_runs -> Nullable<Int4>,
        max_consecutive_failed_dag_runs -> Nullable<Int4>,
        has_task_concurrency_limits -> Nullable<Bool>,
        has_import_errors -> Bool,
        next_dagrun -> Nullable<Timestamptz>,
        next_dagrun_data_interval_start -> Nullable<Timestamptz>,
        next_dagrun_data_interval_end -> Nullable<Timestamptz>,
        next_dagrun_create_after -> Nullable<Timestamptz>,
    }
}

