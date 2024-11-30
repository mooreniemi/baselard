use serde_json::{Value, Map};
use crate::component::{Component, Data, DataType, Error};
use crate::dag::DAGError;

pub struct JsonCombiner;

impl Component for JsonCombiner {
    fn configure(_: Value) -> Result<Self, Error> {
        Ok(JsonCombiner)
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("JsonCombiner input: {input:?}");

        match input {
            Data::List(items) => {
                let mut combined = Map::new();

                // Process each input with a numbered key
                for (i, item) in items.into_iter().enumerate() {
                    let Data::Json(json_value) = item else {
                        return Err(DAGError::ExecutionError {
                            node_id: "unknown".to_string(),
                            reason: format!("Input {i} must be JSON"),
                        })
                    };
                    combined.insert(format!("input_{i}"), json_value);
                }

                Ok(Data::Json(Value::Object(combined)))
            }
            _ => Err(DAGError::ExecutionError {
                node_id: "unknown".to_string(),
                reason: "JsonCombiner requires a List input of JSON values".to_string(),
            }),
        }
    }

    fn input_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Json))
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}