use serde_json::Value;
use tracing::info;

use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};

pub struct Adder {
    value: i32,
}

impl Component for Adder {
    fn configure(config: Value) -> Result<Self, Error> {
        Ok(Adder {
            value: i32::try_from(config["value"].as_i64().unwrap()).unwrap(),
        })
    }

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        info!("Adder {}: input={input:?}", context.node_id);
        let input_value = match input {
            Data::Integer(v) => v,
            Data::List(list) => list.into_iter().filter_map(|v| v.as_integer()).sum(),
            _ => 0,
        };

        Ok(Data::Integer(input_value + self.value))
    }

    fn input_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Integer,
            DataType::List(Box::new(DataType::Integer)),
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}
