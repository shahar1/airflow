use std::io::Write;
use diesel::deserialize::FromSql;
use diesel::sql_types::Jsonb;
use diesel::pg::Pg;
use diesel::backend::Backend;
use diesel::{deserialize, serialize};
use serde_json::{Value, json};
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use diesel::serialize::{IsNull, Output, ToSql};
use serde::ser::SerializeStruct;

/// TODO: Generalize

#[derive(Debug, Clone, PartialEq)]
#[derive(diesel::deserialize::FromSqlRow, diesel::expression::AsExpression)]
#[diesel(sql_type = Jsonb)]
pub struct Serialized(pub Value);

impl Serialize for Serialized {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SerializedWrapper", 2)?;
        state.serialize_field("__var", &self.0)?;
        state.serialize_field("__type", "dict")?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Serialized {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapper = Value::deserialize(deserializer)?;
        if let Some(obj) = wrapper.as_object() {
            if let Some(var_value) = obj.get("__var") {
                return Ok(Serialized(var_value.clone()));
            }
        }
        Err(serde::de::Error::custom("Expected JSON object with '__var' field"))
    }
}


impl ToSql<Jsonb, Pg> for Serialized {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_all(&[1])?;
        serde_json::to_writer(out, self)
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

impl FromSql<Jsonb, Pg> for Serialized {
    fn from_sql(bytes: <Pg as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        let raw_json_value = <Value as FromSql<Jsonb, Pg>>::from_sql(bytes)?;
        serde_json::from_value(raw_json_value).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_serialize() {
        let inner_value = json!({"error": "Trigger Timeout"});
        let serialized_instance = Serialized(inner_value.clone());

        let expected_json = json!({
            "__var": { "error": "Trigger Timeout" },
            "__type": "dict"
        });

        let serialized_output = serde_json::to_value(&serialized_instance).unwrap();
        assert_eq!(serialized_output, expected_json);
    }

    #[test]
    fn test_deserialize() {
        let db_json = json!({
            "__var": { "error": "Trigger Timeout" },
            "__type": "dict"
        });

        let deserialized_instance: Serialized = serde_json::from_value(db_json.clone()).unwrap();
        assert_eq!(deserialized_instance.0, json!({"error": "Trigger Timeout"}));

        let malformed_json = json!({"__type": "dict"});
        let result: Result<Serialized, _> = serde_json::from_value(malformed_json);
        assert!(result.is_err());

        let non_object_json = json!("not an object");
        let result: Result<Serialized, _> = serde_json::from_value(non_object_json);
        assert!(result.is_err());
    }
}