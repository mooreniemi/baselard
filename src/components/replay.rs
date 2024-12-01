use serde_json::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek};
use std::sync::Mutex;

use crate::cache::DAGResult;
use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext, RequestId};
use moka::sync::Cache as MokaCache;

pub(crate) type RequestPosition = u64;

const MAX_POSITION_CACHE_SIZE: u64 = 10_000;

/// Replay a DAG from a history file. This essentially pre-populates the
/// inputs to your DAG. You can then filter to the specific nodes you want.
///
/// Instead of caching the history output itself, this demo Component caches
/// the positions of the lines in the file so that the next seek is instant.
///
/// This Component is not optimized for read performance.
pub struct Replay {
    reader: Mutex<BufReader<File>>,
    position_cache: MokaCache<RequestId, RequestPosition>,
}

impl Component for Replay {
    fn configure(config: Value) -> Result<Self, Error> {
        let history_path = config
            .get("history_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ConfigurationError("history_path is required".to_string()))?;

        let file = File::open(history_path).map_err(|e| {
            Error::ConfigurationError(format!("Failed to open history file: {e}"))
        })?;
        let reader = Mutex::new(BufReader::new(file));
        let position_cache = MokaCache::new(MAX_POSITION_CACHE_SIZE);

        Ok(Self { reader, position_cache })
    }

    fn input_type(&self) -> DataType {
        DataType::Json
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }

    fn execute(&self, ctx: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        let node_id = ctx.node_id.clone();

        let Data::Json(input) = input else {
            return Err(DAGError::ExecutionError {
                node_id: node_id.clone(),
                reason: "Input must be JSON".to_string(),
            });
        };

        let request_id = input
            .get("request_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DAGError::ExecutionError {
                node_id: node_id.clone(),
                reason: "request_id is required in input".to_string(),
            })?;

        let target_nodes: Option<HashSet<String>> = input
            .get("target_nodes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(String::from)
                    .collect()
            });
        println!("Replaying node {node_id} with request_id {request_id} and target_nodes {target_nodes:?}");

        let historical_result = self
            .get_historical_result(&request_id.to_string())
            .map_err(|e| DAGError::ExecutionError {
                node_id: node_id.clone(),
                reason: format!("Failed to read history file: {e}"),
            })?
            .ok_or_else(|| DAGError::HistoricalResultNotFound {
                request_id: request_id.to_string(),
            })?;

        let filtered_results = match target_nodes {
            Some(nodes) => historical_result
                .node_results
                .into_iter()
                .filter(|(id, _)| nodes.contains(id))
                .collect(),
            _ => historical_result.node_results,
        };

        Ok(Data::Json(serde_json::to_value(filtered_results).map_err(
            |e| DAGError::ExecutionError {
                node_id,
                reason: format!("Failed to serialize results: {e}"),
            },
        )?))
    }
}

impl Replay {
    fn get_historical_result(&self, request_id: &RequestId) -> Result<Option<DAGResult>, std::io::Error> {
        if let Some(pos) = self.position_cache.get(request_id) {
            let mut reader = self.reader.lock().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire lock")
            })?;
            reader.seek(std::io::SeekFrom::Start(pos))?;
            let mut line = String::new();
            reader.read_line(&mut line)?;
            if let Ok(result) = serde_json::from_str::<DAGResult>(&line) {
                return Ok(Some(result));
            }
        }

        let mut reader = self.reader.lock().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire lock")
        })?;
        reader.seek(std::io::SeekFrom::Start(0))?;

        let mut searched = 0;
        let mut current_pos = reader.stream_position()?;

        for line in reader.by_ref().lines() {
            let line = line?;
            searched += 1;

            if let Ok(result) = serde_json::from_str::<DAGResult>(&line) {
                if result.request_id == *request_id {
                    println!("Found matching line: {line}, and caching position");
                    self.position_cache.insert(request_id.to_string(), current_pos);
                    return Ok(Some(result));
                }
            }
            current_pos += line.len() as u64 + 1; // +1 for newline
        }

        println!("Searched {searched} lines in history file");
        Ok(None)
    }
}
