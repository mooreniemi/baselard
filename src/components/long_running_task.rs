use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{oneshot, Mutex};

use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;

pub struct LongRunningTask {
    sleep_duration_ms: u64,
}

impl Component for LongRunningTask {
    fn configure(config: Value) -> Self {
        LongRunningTask {
            sleep_duration_ms: config["sleep_duration_ms"].as_u64().unwrap_or(30),
        }
    }

    fn execute(&self, _input: Data) -> Result<Data, DAGError> {
        let (tx, rx) = oneshot::channel();

        let sleep_duration_ms = self.sleep_duration_ms;
        tokio::spawn(async move {
            println!("LongRunningTask: Sleeping for {}ms", sleep_duration_ms);
            tokio::time::sleep(tokio::time::Duration::from_millis(
                sleep_duration_ms,
            ))
            .await;

            let result = Data::Integer(42);
            let _ = tx.send(result);
        });

        Ok(Data::OneConsumerChannel(Arc::new(Mutex::new(Some(rx)))))
    }

    fn input_type(&self) -> DataType {
        DataType::Null
    }

    fn output_type(&self) -> DataType {
        DataType::OneConsumerChannel(Box::new(DataType::Integer))
    }
}