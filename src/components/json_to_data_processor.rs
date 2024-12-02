use serde_json::Value;
use tracing::debug;

use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};

pub struct JsonToDataProcessor;

impl Component for JsonToDataProcessor {
    fn configure(_: Value) -> Result<Self, Error> {
        Ok(JsonToDataProcessor)
    }

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        debug!("JsonToDataProcessor {}: input={input:?}", context.node_id);

        let Data::Json(json) = input else {
            return Err(DAGError::ExecutionError {
                node_id: context.node_id,
                reason: "Expected JSON input".to_string()
            });
        };

        match json.get("type").and_then(Value::as_str) {
            #[allow(clippy::match_same_arms)]
            Some("null") => Ok(Data::Null),
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Some("integer") => Ok(Data::Integer(json["value"].as_i64().unwrap() as i32)),
            Some("float") => Ok(Data::Float(json["value"].as_f64().unwrap())),
            Some("text") => Ok(Data::Text(json["value"].as_str().unwrap().to_string())),
            Some("list") => {
                let values = json["values"].as_array().unwrap();
                let list = values
                    .iter()
                    .map(|v| match v["type"].as_str().unwrap() {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        "integer" => Data::Integer(v["value"].as_i64().unwrap() as i32),
                        "float" => Data::Float(v["value"].as_f64().unwrap()),
                        "text" => Data::Text(v["value"].as_str().unwrap().to_string()),
                        _ => Data::Null,
                    })
                    .collect();
                Ok(Data::List(list))
            }
            _ => Ok(Data::Null),
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Json
    }

    fn output_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Json,
            DataType::Integer,
            DataType::Float,
            DataType::Text,
            DataType::List(Box::new(DataType::Union(vec![
                DataType::Integer,
                DataType::Text,
            ]))),
        ])
    }
}