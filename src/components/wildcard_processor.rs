use std::collections::HashSet;

use serde_json::{json, Value};
use tracing::info;

use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};

pub struct WildcardProcessor {
    expected_input_keys: HashSet<String>,
    expected_output_keys: HashSet<String>,
}

impl Component for WildcardProcessor {
    fn configure(config: Value) -> Result<Self, Error> {
        let expected_input_keys = config["expected_input_keys"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let expected_output_keys = config["expected_output_keys"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        Ok(WildcardProcessor {
            expected_input_keys,
            expected_output_keys,
        })
    }

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        info!("WildcardProcessor {}: input={input:?}", context.node_id);
        match input {
            Data::Json(mut value) => {
                let mut fallback_map = serde_json::Map::new();

                let input_object = value.as_object_mut().unwrap_or(&mut fallback_map);

                for key in &self.expected_input_keys {
                    if !input_object.contains_key(key) {
                        return Err(DAGError::ExecutionError {
                            node_id: "unknown".to_string(),
                            reason: format!("Missing key: {key}"),
                        });
                    }
                }

                let mut output_object = serde_json::Map::new();

                for key in &self.expected_output_keys {
                    if let Some(v) = input_object.get(key) {
                        output_object.insert(key.clone(), v.clone());
                    } else {
                        output_object.insert(key.clone(), json!(null));
                    }
                }

                Ok(Data::Json(Value::Object(output_object)))
            }
            _ => Err(DAGError::ExecutionError {
                node_id: "unknown".to_string(),
                reason: "Invalid input type, expected JSON".to_string(),
            }),
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Json
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}
