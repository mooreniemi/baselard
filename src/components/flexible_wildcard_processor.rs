use serde_json::{json, Value};

use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;

pub struct FlexibleWildcardProcessor;

impl Component for FlexibleWildcardProcessor {
    fn configure(_: Value) -> Self {
        FlexibleWildcardProcessor
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("FlexibleWildcardProcessor input: {input:?}");
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
            },
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
            ]))),
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}