use std::time::Duration;

use serde_json::Value;
use tokio::time::timeout;

use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;


pub struct ChannelConsumer {
    timeout_secs: Option<u64>,
}

impl Component for ChannelConsumer {
    fn configure(config: Value) -> Self {
        let timeout_secs = config["timeout_secs"].as_u64();
        ChannelConsumer { timeout_secs }
    }

    fn input_type(&self) -> DataType {
        DataType::OneConsumerChannel(Box::new(DataType::Integer))
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("ChannelConsumer input: {:?}", input);
        if let Data::OneConsumerChannel(channel) = input {
            let receiver = channel.clone();
            let timeout_duration = self.timeout_secs.map(Duration::from_secs);

            let result = tokio::task::block_in_place(|| {
                let mut receiver = receiver.blocking_lock();
                if let Some(rx) = receiver.take() {
                    let fut = async move {
                        if let Some(duration) = timeout_duration {
                            timeout(duration, rx).await.map_err(|_| "Timeout exceeded")
                        } else {
                            Ok(rx.await)
                        }
                    };

                    tokio::runtime::Handle::current().block_on(fut)
                } else {
                    Err("Channel already consumed".into())
                }
            });

            match result {
                Ok(Ok(data)) => {
                    println!("ChannelConsumer output: {:?}", data);
                    Ok(data)
                }
                Ok(Err(_)) | Err(_) => {
                    eprintln!("ChannelConsumer: Timed out or failed to receive data from channel.");
                    Err(DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason:
                            "ChannelConsumer: Timed out or failed to receive data from channel."
                                .to_string(),
                    })
                }
            }
        } else {
            eprintln!("ChannelConsumer: Invalid input type.");
            Err(DAGError::ExecutionError {
                node_id: "unknown".to_string(),
                reason: "ChannelConsumer: Invalid input type.".to_string(),
            })
        }
    }

    /// ChannelConsumer is deferrable because it can be used to wait for a result from a long-running task.
    fn is_deferrable(&self) -> bool {
        true
    }
}