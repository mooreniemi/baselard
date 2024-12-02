use serde_json::{json, Value};
use tracing::debug;

use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};

pub struct DataToJsonProcessor;

impl Component for DataToJsonProcessor {
    fn configure(_: Value) -> Result<Self, Error> {
        Ok(DataToJsonProcessor)
    }

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        debug!("DataToJsonProcessor {}: input={input:?}", context.node_id);
        let json_input = match input {
            Data::Null => json!({ "type": "null" }),
            Data::Json(value) => {
                if let Some(num) = value.as_i64() {
                    json!({ "type": "integer", "value": num })
                } else if let Some(num) = value.as_f64() {
                    json!({ "type": "float", "value": num })
                } else {
                    json!({ "type": "json", "value": value })
                }
            }
            Data::Integer(i) => json!({ "type": "integer", "value": i }),
            Data::Float(f) => json!({ "type": "float", "value": f }),
            Data::Text(t) => json!({ "type": "text", "value": t }),
            Data::List(list) => {
                let json_list: Vec<_> = list
                    .into_iter()
                    .map(|item| match item {
                        Data::Integer(i) => json!({ "type": "integer", "value": i }),
                        Data::Text(t) => json!({ "type": "text", "value": t }),
                        _ => json!({ "type": "unknown" }),
                    })
                    .collect();
                json!({ "type": "list", "values": json_list })
            }
        };

        Ok(Data::Json(json_input))
    }

    fn input_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Json,
            DataType::Integer,
            DataType::Float,
            DataType::Text,
            DataType::List(Box::new(DataType::Union(vec![
                DataType::Integer,
                DataType::Text,
                DataType::Float,
            ]))),
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}
