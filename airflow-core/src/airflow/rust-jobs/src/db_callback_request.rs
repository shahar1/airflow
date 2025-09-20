use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde_json::Value;

table! {
    callback_request (id) {
        id -> Integer,
        created_at -> Timestamp,
        priority_weight -> Integer,
        callback_data -> Jsonb,
        callback_type -> Varchar,
    }
}

#[derive(Queryable)]
pub struct DbCallbackRequest {
    pub id: i32,
    pub created_at: NaiveDateTime,
    pub priority_weight: i32,
    pub callback_data: Value,
    pub callback_type: String,
}
