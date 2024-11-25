use std::time::Duration;

use serde_json::Value;
use tokio::time::timeout;

use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;

pub struct MultiChannelConsumer {
    timeout_secs: Option<u64>,
}

impl Component for MultiChannelConsumer {
    fn configure(config: Value) -> Self {
        let timeout_secs = config["timeout_secs"].as_u64();
        MultiChannelConsumer { timeout_secs }
    }

    fn input_type(&self) -> DataType {
        DataType::List(Box::new(DataType::OneConsumerChannel(Box::new(
            DataType::Integer,
        ))))
    }

    fn output_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Integer))
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("MultiChannelConsumer input: {:?}", input);

        if let Data::List(channels) = input {
            let timeout_duration = self.timeout_secs.map(Duration::from_secs);

            let results: Result<Vec<Data>, DAGError> = channels
                .into_iter()
                .map(|channel_data| {
                    if let Data::OneConsumerChannel(channel) = channel_data {
                        let receiver = channel.clone();

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
                                println!("MultiChannelConsumer output: {:?}", data);
                                Ok(data)
                            }
                            Ok(Err(_)) | Err(_) => {
                                eprintln!("MultiChannelConsumer: Timed out or failed to receive data from channel.");
                                Err(DAGError::ExecutionError {
                                    node_id: "unknown".to_string(),
                                    reason: "MultiChannelConsumer: Timed out or failed to receive data from channel."
                                        .to_string(),
                                })
                            }
                        }
                    } else {
                        eprintln!("MultiChannelConsumer: Invalid input type in list.");
                        Err(DAGError::ExecutionError {
                            node_id: "unknown".to_string(),
                            reason: "MultiChannelConsumer: Invalid input type in list.".to_string(),
                        })
                    }
                })
                .collect();

            results.map(Data::List)
        } else {
            eprintln!("MultiChannelConsumer: Invalid input type.");
            Err(DAGError::ExecutionError {
                node_id: "unknown".to_string(),
                reason: "MultiChannelConsumer: Invalid input type.".to_string(),
            })
        }
    }

    /// MultiChannelConsumer is deferrable because it can be used to wait for results from multiple long-running tasks.
    fn is_deferrable(&self) -> bool {
        true
    }
}
