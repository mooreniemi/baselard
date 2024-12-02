use serde_json::Value;
use tracing::info;

use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};

pub struct StringLengthCounter;

impl Component for StringLengthCounter {
    fn configure(_: Value) -> Result<Self, Error> {
        Ok(StringLengthCounter)
    }

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        info!("StringLengthCounter {}: input={input:?}", context.node_id);
        let len = input.as_text().unwrap_or("").len();
        Ok(Data::Integer(i32::try_from(len).unwrap()))
    }

    fn input_type(&self) -> DataType {
        DataType::Text
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}
