use std::time::Instant;

use serde_json::Value;
use jq_rs::compile;
use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;

pub struct PayloadTransformer {
    expression: String,
}

impl Component for PayloadTransformer {
    fn configure(config: Value) -> Self {
        let start = Instant::now();
        let expression = config["transformation_expression"]
            .as_str()
            .unwrap_or(".")
            .to_string();

        compile(&expression)
            .expect("Failed to compile JQ expression");
        let duration = start.elapsed();
        println!("JQ compilation time: {duration:?}");

        PayloadTransformer {
            expression,
        }
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("PayloadTransformer input: {input:?}");

        match input {
            Data::Json(value) => {
                let input_str = serde_json::to_string(&value).map_err(|err| DAGError::ExecutionError {
                    node_id: "unknown".to_string(),
                    reason: format!("Failed to serialize input: {err}"),
                })?;

                let start = Instant::now();
                let mut transformation = compile(&self.expression)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason: format!("Failed to compile JQ expression: {err}"),
                    })?;
                let duration = start.elapsed();
                println!("JQ compilation time: {duration:?}");

                let start = Instant::now();
                let output_str = transformation.run(&input_str).map_err(|err| DAGError::ExecutionError {
                    node_id: "unknown".to_string(),
                    reason: format!("Failed to execute jq: {err}"),
                })?;
                let duration = start.elapsed();
                println!("JQ execution time: {duration:?}");

                let output_json: Value = serde_json::from_str(&output_str).map_err(|err| DAGError::ExecutionError {
                    node_id: "unknown".to_string(),
                    reason: format!("Failed to parse jq output: {err}"),
                })?;

                Ok(Data::Json(output_json))
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