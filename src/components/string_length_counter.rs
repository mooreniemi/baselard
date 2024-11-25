use serde_json::Value;

use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;

pub struct StringLengthCounter;

impl Component for StringLengthCounter {
    fn configure(_: Value) -> Self {
        StringLengthCounter
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        let len = input.as_text().unwrap_or("").len();
        Ok(Data::Integer(len as i32))
    }

    fn input_type(&self) -> DataType {
        DataType::Text
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}