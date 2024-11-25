use serde_json::Value;

use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;

pub struct CrashTestDummy {
    fail: bool,
    sleep_duration_ms: Option<u64>,
}

impl Component for CrashTestDummy {
    fn configure(config: Value) -> Self {
        let fail = config["fail"].as_bool().unwrap_or(false);
        let sleep_duration_ms = config["sleep_duration_ms"].as_u64();
        CrashTestDummy {
            fail,
            sleep_duration_ms,
        }
    }

    fn execute(&self, _input: Data) -> Result<Data, DAGError> {
        if let Some(duration) = self.sleep_duration_ms {
            println!("CrashTestDummy: Sleeping for {}ms", duration);
            std::thread::sleep(std::time::Duration::from_millis(duration));
        }

        if self.fail {
            Err(DAGError::ExecutionError {
                node_id: "CrashTestDummy".to_string(),
                reason: "Simulated failure as configured".to_string(),
            })
        } else {
            Ok(Data::Text("Success!".to_string()))
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Null
    }

    fn output_type(&self) -> DataType {
        DataType::Text
    }
}