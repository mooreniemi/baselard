use indexmap::IndexMap;
use moka::sync::Cache;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::component::Data;

type RequestId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGResult {
    pub request_id: Option<RequestId>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub node_results: IndexMap<String, Data>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct DAGInputHash {
    ir_hash: u64,
    inputs_hash: u64,
}

/// We cache results in two ways:
/// - By DAG inputs and configuration, for memoized behavior
/// - By request ID (in memory and backed on a file) for specific replay
pub struct DAGCache {
    /// Cache keyed by DAG inputs configuration
    request_cache: Arc<Cache<DAGInputHash, DAGResult>>,
    /// Cache keyed by request ID for testing/lookup
    history_cache: Arc<Cache<RequestId, DAGResult>>,
    /// History file path
    history_file: Option<PathBuf>,
}

impl DAGCache {
    pub fn new(history_file: Option<impl Into<PathBuf>>, max_capacity: u64) -> Self {
        Self {
            request_cache: Arc::new(Cache::new(max_capacity)),
            history_cache: Arc::new(Cache::new(max_capacity)),
            history_file: history_file.map(Into::into),
        }
    }

    fn create_cache_key(ir_hash: u64, inputs: &HashMap<String, Data>) -> DAGInputHash {
        DAGInputHash {
            ir_hash,
            inputs_hash: Self::calculate_inputs_hash(inputs),
        }
    }

    fn calculate_inputs_hash(map: &HashMap<String, Data>) -> u64 {
        let mut hasher = DefaultHasher::new();
        for (key, value) in map.iter() {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    pub async fn store_result(
        &self,
        ir_hash: u64,
        inputs: &HashMap<String, Data>,
        results: IndexMap<String, Data>,
        request_id: Option<RequestId>,
    ) {
        let timestamp = chrono::Utc::now();

        let memory_result = DAGResult {
            request_id: request_id.clone(),
            timestamp,
            node_results: results.clone(),
        };

        let cache_key = Self::create_cache_key(ir_hash, inputs);
        self.request_cache.insert(cache_key, memory_result.clone());

        if let Some(request_id) = request_id.clone() {
            self.history_cache.insert(request_id, memory_result);
        }

        if let Some(file_path) = &self.history_file {
            let file_path = file_path.clone();

            let history_request_id =
                request_id.unwrap_or_else(|| format!("auto-{}", uuid::Uuid::new_v4()));

            let history_result = DAGResult {
                request_id: Some(history_request_id),
                timestamp,
                node_results: results,
            };

            tokio::spawn(async move {
                if let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_path)
                    .await
                {
                    if let Ok(json) = serde_json::to_string(&history_result) {
                        let _ = file.write_all(format!("{}\n", json).as_bytes()).await;
                    }
                }
            });
        }
    }

    pub fn get_cached_result(
        &self,
        ir_hash: u64,
        inputs: &HashMap<String, Data>,
    ) -> Option<DAGResult> {
        let cache_key = Self::create_cache_key(ir_hash, inputs);
        self.request_cache.get(&cache_key)
    }

    pub fn get_result_by_request_id(&self, request_id: &str) -> Option<DAGResult> {
        self.history_cache.get(request_id)
    }

    pub async fn get_historical_result(&self, request_id: &str) -> Option<DAGResult> {
        if let Some(result) = self.history_cache.get(request_id) {
            return Some(result);
        }

        let history_file = self.history_file.as_ref()?;

        let file = match tokio::fs::File::open(history_file).await {
            Ok(file) => file,
            Err(_) => return None,
        };

        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(result) = serde_json::from_str::<DAGResult>(&line) {
                if result.request_id.as_deref() == Some(request_id) {
                    self.history_cache
                        .insert(request_id.to_string(), result.clone());
                    return Some(result);
                }
            }
        }

        None
    }

    pub fn get_cached_node_result(
        &self,
        ir_hash: u64,
        inputs: &HashMap<String, Data>,
        node_id: &str,
    ) -> Option<Data> {
        self.get_cached_result(ir_hash, inputs)
            .and_then(|result| result.node_results.get(node_id).cloned())
    }
}
