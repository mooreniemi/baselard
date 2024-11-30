use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};
use ndarray::{Array, CowArray};
use ort::{Environment, GraphOptimizationLevel, SessionBuilder, Value as OrtValue};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use lazy_static::lazy_static;

lazy_static! {
    static ref ONNX_ENV: Arc<Environment> = Arc::new(
        Environment::builder()
            .with_name("GlobalONNXEnvironment")
            .with_log_level(ort::LoggingLevel::Warning)
            .build()
            .expect("Failed to create global ONNX environment")
    );
}

pub struct MLModel {
    remote_endpoint: Option<String>,
    session: Option<Arc<ort::Session>>,
}

impl Component for MLModel {
    fn configure(config: Value) -> Result<Self, Error> {
        let remote_endpoint = config["remote_endpoint"].as_str().map(String::from);
        let session = if let Some(model_path) = config["onnx_model_path"].as_str() {
            Some(Arc::new(
                SessionBuilder::new(&ONNX_ENV)
                    .map_err(|e| Error::ConfigurationError(format!("Failed to create session builder: {e}")))?
                    .with_optimization_level(GraphOptimizationLevel::Level1)
                    .map_err(|e| Error::ConfigurationError(format!("Failed to set optimization level: {e}")))?
                    .with_model_from_file(model_path)
                    .map_err(|e| Error::ConfigurationError(format!("Failed to load model: {e}")))?
            ))
        } else {
            None
        };

        Ok(MLModel {
            remote_endpoint,
            session,
        })
    }

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        println!("MLModel '{}' started processing", context.node_id);
        let start_time = Instant::now();

        let input_vec = match input {
            Data::List(items) => items
                .into_iter()
                .map(|item| match item {
                    Data::Float(f) => Ok(f),
                    Data::Integer(i) => Ok(f64::from(i)),
                    _ => Err(DAGError::ExecutionError {
                        node_id: context.node_id.clone(),
                        reason: "Input list items must be numbers".to_string(),
                    }),
                })
                .collect::<Result<Vec<f64>, DAGError>>()?,
            _ => {
                return Err(DAGError::ExecutionError {
                    node_id: context.node_id,
                    reason: "Input must be a list of numbers".to_string(),
                })
            }
        };

        let result = if let Some(endpoint) = &self.remote_endpoint {
            Self::handle_remote_prediction(&context.node_id, endpoint, &input_vec)?
        } else {
            self.handle_local_prediction(&context.node_id, &input_vec)?
        };

        println!(
            "MLModel '{}' completed processing in {:?}",
            context.node_id,
            start_time.elapsed()
        );

        Ok(Data::List(result.into_iter().map(Data::Float).collect()))
    }

    fn input_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Union(vec![
            DataType::Float,
            DataType::Integer,
        ])))
    }

    fn output_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Float))
    }
}

impl MLModel {
    fn handle_local_prediction(&self, node_id: &str, input: &[f64]) -> Result<Vec<f64>, DAGError> {
        let session = self.session.as_ref().ok_or_else(|| DAGError::ExecutionError {
            node_id: node_id.to_string(),
            reason: "No ONNX session available".to_string(),
        })?;

        #[allow(clippy::cast_possible_truncation)]
        let array = Array::from_shape_vec(
            (1, input.len()),
            input.iter().map(|&x| x as f32).collect(),
        ).map_err(|e| DAGError::ExecutionError {
            node_id: node_id.to_string(),
            reason: format!("Failed to create input array: {e}"),
        })?;

        let cow_array: CowArray<f32, _> = array.into_dyn().into();
        let input_tensor = OrtValue::from_array(session.allocator(), &cow_array)
            .map_err(|e| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("Failed to create input tensor: {e}"),
            })?;

        let outputs = session.run(vec![input_tensor])
            .map_err(|e| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("ONNX inference failed: {e}"),
            })?;

        let output = outputs.first().ok_or_else(|| DAGError::ExecutionError {
            node_id: node_id.to_string(),
            reason: "No output tensor produced".to_string(),
        })?;

        #[allow(clippy::cast_precision_loss)]
        let result = output
            .try_extract::<i64>()
            .map_err(|e| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("Failed to extract tensor data: {e}"),
            })?
            .view()
            .iter()
            .map(|&x| x as f64)
            .collect();

        println!("Local prediction (in {node_id}) result: {result:?}");
        Ok(result)
    }

    fn handle_remote_prediction(
        node_id: &str,
        endpoint: &str,
        input: &[f64],
    ) -> Result<Vec<f64>, DAGError> {
        let client = Client::new();
        let input_data = json!({ "features": input });
        println!("Sending payload: {input_data:?}");

        let response = client
            .post(endpoint)
            .json(&input_data)
            .send()
            .map_err(|e| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("Remote request failed: {e}"),
            })?;

        if !response.status().is_success() {
            return Err(DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("Remote endpoint error: {}", response.status()),
            });
        }

        let result: Vec<f64> = response
            .json::<Value>()
            .map_err(|e| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("Failed to parse response: {e}"),
            })?
            .get("processed_features")
            .and_then(|features| features.as_array())
            .map(|array| array.iter().filter_map(serde_json::Value::as_f64).collect())
            .ok_or_else(|| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: "Invalid 'processed_features' in response".to_string(),
            })?;

        Ok(result)
    }
}
