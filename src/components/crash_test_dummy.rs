use serde_json::Value;
use spin_sleep::SpinSleeper;
use tracing::info;
use std::time::Instant;
use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};

/// A test component that can be configured to fail or sleep
pub struct CrashTestDummy {
    fail: bool,
    /// Duration to sleep in milliseconds (if Some)
    sleep_duration_ms: Option<f64>,
    /// Threshold in microseconds below which to spin instead of yielding to OS
    /// Higher values improve sleep precision but increase CPU usage
    spin_threshold_us: u32,
    sleeper: SpinSleeper,
}

impl Component for CrashTestDummy {
    fn configure(config: Value) -> Result<Self, Error> {
        let fail = config["fail"].as_bool().unwrap_or(false);
        let sleep_duration_ms = config["sleep_duration_ms"].as_f64();
        #[allow(clippy::cast_possible_truncation)]
        let spin_threshold_us = config["spin_threshold_us"]
            .as_u64()
            .map_or(50_000, |x| x as u32);

        let sleeper = SpinSleeper::new(spin_threshold_us);

        Ok(CrashTestDummy {
            fail,
            sleep_duration_ms,
            spin_threshold_us,
            sleeper,
        })
    }

    fn execute(&self, context: NodeExecutionContext, _input: Data) -> Result<Data, DAGError> {
        if let Some(duration) = self.sleep_duration_ms {
            let start_time = Instant::now();
            info!(
                "CrashTestDummy {}: Sleeping for {duration}ms (spin threshold: {}μs)",
                context.node_id,
                self.spin_threshold_us
            );
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            self.sleeper.sleep(std::time::Duration::from_millis(duration as u64));
            info!(
                "CrashTestDummy {}: Done sleeping after {:.3}s",
                context.node_id,
                start_time.elapsed().as_secs_f32()
            );
        }

        if self.fail {
            Err(DAGError::ExecutionError {
                node_id: context.node_id,
                reason: "Simulated failure as configured".to_string(),
            })
        } else {
            Ok(Data::Text("Success!".to_string()))
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Null,
            DataType::Text,
            DataType::Json,
            DataType::Integer,
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Text
    }
}
