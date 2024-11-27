use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::task;
use tokio::time::timeout;
use uuid::Uuid;

use crate::cache::DAGCache;
use crate::cache::DAGResult;
use crate::component::ComponentRegistry;
use crate::component::{Component, Data, DataType};

#[derive(Debug, Clone)]
struct NodeIR {
    id: String,
    namespace: Option<String>,
    component_type: String,
    config: Value,
    inputs: Option<Data>,
}

#[derive(Debug)]
pub struct DAGIR {
    nodes: Vec<NodeIR>,
    edges: HashMap<String, Vec<Edge>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Edge {
    source: String,
    target: String,
    target_input: String,
}

impl DAGIR {
    pub fn from_json(json_config: Value) -> Result<Self, String> {
        let start = Instant::now();
        let mut nodes = Vec::new();
        let mut edges: HashMap<String, Vec<Edge>> = HashMap::new();

        let array = json_config
            .as_array()
            .ok_or_else(|| "Root JSON must be an array".to_string())?;

        for node in array {
            let id = node
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Node ID must be a string".to_string())?
                .to_string();

            if id.is_empty() {
                return Err("Node ID cannot be empty".to_string());
            }

            let component_type = node
                .get("component_type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("Node {} missing component_type", id))?
                .to_string();

            let config = node
                .get("config")
                .ok_or_else(|| format!("Node {} missing config", id))?
                .clone();

            let namespace = node
                .get("namespace")
                .and_then(|v| v.as_str())
                .map(String::from);

            let inputs = node
                .get("inputs")
                .map(|v| match v {
                    Value::String(s) => Ok(Data::Text(s.clone())),
                    Value::Number(n) => n
                        .as_i64()
                        .map(|i| Data::Integer(i as i32))
                        .ok_or_else(|| "Unsupported number type in inputs".to_string()),
                    Value::Array(arr) => {
                        let data_list = arr
                            .iter()
                            .map(|item| match item {
                                Value::String(s) => Ok(Data::Text(s.clone())),
                                Value::Number(n) => {
                                    n.as_i64().map(|i| Data::Integer(i as i32)).ok_or_else(|| {
                                        "Unsupported number type in array input".to_string()
                                    })
                                }
                                _ => Err("Unsupported type in array input".to_string()),
                            })
                            .collect::<Result<Vec<_>, String>>()?;
                        Ok(Data::List(data_list))
                    }
                    Value::Object(_) => Ok(Data::Json(v.clone())),
                    _ => Err("Unsupported input type".to_string()),
                })
                .transpose()?;

            nodes.push(NodeIR {
                id: id.clone(),
                namespace,
                component_type,
                config,
                inputs,
            });

            if let Some(depends_on) = node.get("depends_on") {
                let deps = depends_on
                    .as_array()
                    .ok_or_else(|| format!("Node {} depends_on must be an array", id))?;

                let mut node_edges = Vec::new();
                for dep in deps {
                    let source = dep
                        .as_str()
                        .ok_or_else(|| format!("Node {} dependency must be a string", id))?
                        .to_string();

                    node_edges.push(Edge {
                        source,
                        target: id.clone(),
                        target_input: "".to_string(),
                    });
                }
                if !node_edges.is_empty() {
                    edges.insert(id.clone(), node_edges);
                }
            }
        }

        let duration = start.elapsed();
        println!("DAGIR from_json took {:?}", duration);
        Ok(DAGIR { nodes, edges })
    }

    pub fn calculate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        let mut sorted_nodes = self.nodes.clone();
        sorted_nodes.sort_by(|a, b| a.id.cmp(&b.id));

        for node in &sorted_nodes {
            node.id.hash(&mut hasher);
            node.namespace.hash(&mut hasher);
            node.component_type.hash(&mut hasher);
            node.config.to_string().hash(&mut hasher);
            node.inputs.hash(&mut hasher);
        }

        let mut sorted_edges: Vec<_> = self.edges.iter().collect();
        sorted_edges.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        for (target, edges) in sorted_edges {
            target.hash(&mut hasher);
            let mut sorted_edges = edges.clone();
            sorted_edges.sort_by(|a, b| a.source.cmp(&b.source));
            for edge in sorted_edges {
                edge.hash(&mut hasher);
            }
        }

        hasher.finish()
    }
}

#[derive(Debug)]
pub enum DAGError {
    /// Represents a type mismatch error.
    TypeMismatch {
        node_id: String,
        expected: DataType,
        actual: DataType,
    },
    /// Represents a missing dependency output.
    MissingDependency {
        node_id: String,
        dependency_id: String,
    },
    /// Represents a runtime error during component execution.
    ExecutionError { node_id: String, reason: String },
    /// Represents a node not found in the DAG.
    NodeNotFound { node: String },
    /// Represents invalid configuration or setup.
    InvalidConfiguration(String),
    /// Represents a cycle detected in the DAG.
    CycleDetected,
    /// Represents no valid inputs for a node.
    NoValidInputs { node_id: String, expected: DataType },
    /// Represents a historical result not found
    HistoricalResultNotFound { request_id: String },
    /// Represents a type system failure.
    TypeSystemFailure {
        component: String,
        expected: DataType,
        received: DataType,
    },
}

impl std::fmt::Display for DAGError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DAGError::TypeMismatch {
                node_id,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Node {}: Type mismatch. Expected {:?}, got {:?}",
                    node_id, expected, actual
                )
            }
            DAGError::MissingDependency {
                node_id,
                dependency_id,
            } => {
                write!(
                    f,
                    "Node {}: Missing output from dependency {}",
                    node_id, dependency_id
                )
            }
            DAGError::ExecutionError { node_id, reason } => {
                write!(f, "Node {}: Execution failed. Reason: {}", node_id, reason)
            }
            DAGError::InvalidConfiguration(reason) => {
                write!(f, "Invalid configuration: {}", reason)
            }
            DAGError::CycleDetected => write!(f, "Cycle detected in the DAG"),
            DAGError::NodeNotFound { node } => write!(f, "Node {} not found", node),
            DAGError::NoValidInputs { node_id, expected } => {
                write!(
                    f,
                    "Node {}: No valid inputs. Expected {:?}",
                    node_id, expected
                )
            }
            DAGError::HistoricalResultNotFound { request_id } => {
                write!(
                    f,
                    "No historical result found for request ID: {}",
                    request_id
                )
            }
            DAGError::TypeSystemFailure {
                component,
                expected,
                received,
            } => {
                write!(
                    f,
                    "Type system failure in component {}: Expected {:?}, received {:?}",
                    component, expected, received
                )
            }
        }
    }
}

impl std::error::Error for DAGError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGConfig {
    pub per_node_timeout_ms: Option<u64>,
    pub enable_memory_cache: bool,
    pub enable_history: bool,
}

impl DAGConfig {
    pub fn enable_memory_cache(&self) -> bool {
        self.enable_memory_cache
    }

    pub fn enable_history(&self) -> bool {
        self.enable_history
    }

    pub fn per_node_timeout_ms(&self) -> Option<u64> {
        self.per_node_timeout_ms
    }

    pub fn cache_off() -> Self {
        Self {
            per_node_timeout_ms: Some(200),
            enable_memory_cache: false,
            enable_history: false,
        }
    }
}

impl Default for DAGConfig {
    fn default() -> Self {
        DAGConfig {
            per_node_timeout_ms: Some(200),
            enable_memory_cache: true,
            enable_history: true,
        }
    }
}

pub struct DAG {
    nodes: Arc<HashMap<String, Box<dyn Component>>>,
    edges: Arc<HashMap<String, Vec<Edge>>>,
    initial_inputs: Arc<HashMap<String, Data>>,
    config: DAGConfig,
    cache: Option<Arc<DAGCache>>,
    ir_hash: u64,
}
impl DAG {
    pub fn from_ir(
        ir: DAGIR,
        registry: &ComponentRegistry,
        config: DAGConfig,
        cache: Option<Arc<DAGCache>>,
    ) -> Result<Self, String> {
        let ir_hash = ir.calculate_hash();
        let mut nodes = HashMap::new();
        let mut edges: HashMap<String, Vec<Edge>> = HashMap::new();
        let mut initial_inputs = HashMap::new();
        let mut node_ids = HashSet::new();

        println!("DAGConfig: {:?}", config);

        println!("Checking for duplicate node IDs");
        for node in &ir.nodes {
            if !node_ids.insert(node.id.clone()) {
                return Err(format!("Duplicate node ID found: {}", node.id));
            }
        }

        println!("Registry: {:?}", registry);
        println!("Creating components");
        for node in ir.nodes {
            let factory = registry
                .get(&node.component_type)
                .ok_or_else(|| format!("Unknown component type: {}", node.component_type))?;
            let component = factory(node.config);

            if let Some(input) = &node.inputs {
                if !Self::validate_data_type(input, &component.input_type()) {
                    return Err(format!(
                        "Node {} initial input type mismatch. Expected {:?}, got {:?}",
                        node.id,
                        component.input_type(),
                        input.get_type()
                    ));
                }
                initial_inputs.insert(node.id.clone(), input.clone());
            }

            println!("Checking dependencies for node {}", node.id);
            if let Some(deps) = ir.edges.get(&node.id) {
                for dep in deps {
                    if !node_ids.contains(&dep.source) {
                        return Err(format!(
                            "Node {} depends on non-existent node {}",
                            dep.target, dep.source
                        ));
                    }
                    edges
                        .entry(dep.target.clone())
                        .or_insert_with(Vec::new)
                        .push(dep.clone());
                }
            }

            nodes.insert(node.id.clone(), component);
        }

        Ok(Self {
            nodes: Arc::new(nodes),
            edges: Arc::new(edges),
            initial_inputs: Arc::new(initial_inputs),
            config,
            cache,
            ir_hash,
        })
    }

    fn validate_data_type(data: &Data, expected_type: &DataType) -> bool {
        match expected_type {
            DataType::Null => matches!(data, Data::Null),
            DataType::Integer => matches!(data, Data::Integer(_)),
            DataType::Float => matches!(data, Data::Float(_)),
            DataType::Text => matches!(data, Data::Text(_)),
            DataType::List(element_type) => {
                if let Data::List(items) = data {
                    items
                        .iter()
                        .all(|item| DAG::validate_data_type(item, element_type))
                } else {
                    false
                }
            }
            DataType::Json => matches!(data, Data::Json(_)),
            DataType::Union(types) => types.iter().any(|t| DAG::validate_data_type(data, t)),
        }
    }

    pub async fn execute(
        &self,
        request_id: Option<String>,
    ) -> Result<IndexMap<String, Data>, DAGError> {
        let start_time = Instant::now();
        let request_id = request_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        println!(
            "[{:.2}s] Starting DAG execution with request_id: {}",
            start_time.elapsed().as_secs_f32(),
            request_id
        );

        let sorted_nodes = self.compute_execution_order()?;

        let (notifiers, shared_results) = self.setup_execution_state();

        let final_results = self
            .execute_nodes(sorted_nodes, notifiers, shared_results, start_time)
            .await?;

        if let Some(cache) = &self.cache {
            self.handle_caching(cache, &final_results, &request_id);
        }

        println!(
            "[{:.2}s] DAG execution completed",
            start_time.elapsed().as_secs_f32()
        );
        Ok(final_results)
    }

    fn compute_execution_order(&self) -> Result<Vec<String>, DAGError> {
        println!("[0.00s] Starting topological sort");
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();

        for node_id in self.nodes.keys() {
            graph.entry(node_id.clone()).or_default();
            in_degree.entry(node_id.clone()).or_insert(0);
        }

        for edges in self.edges.values() {
            for edge in edges {
                *in_degree.entry(edge.target.clone()).or_default() += 1;
                graph
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
            }
        }

        println!("Initial in-degrees: {:?}", in_degree);

        let mut zero_degree_nodes: Vec<_> = in_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(node, _)| node.clone())
            .collect();
        zero_degree_nodes.sort();
        let mut queue: VecDeque<_> = zero_degree_nodes.into();

        let mut sorted_nodes = Vec::new();
        while let Some(node) = queue.pop_front() {
            sorted_nodes.push(node.clone());

            if let Some(children) = graph.get(&node) {
                for child in children {
                    if let Some(degree) = in_degree.get_mut(child) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(child.clone());
                        }
                    }
                }
            }
        }

        println!(
            "[0.00s] Topological sort complete. Execution order: {:?}",
            sorted_nodes
        );

        if sorted_nodes.len() != self.nodes.len() {
            return Err(DAGError::CycleDetected);
        }

        Ok(sorted_nodes)
    }

    fn setup_execution_state(
        &self,
    ) -> (
        Arc<Mutex<HashMap<String, watch::Sender<()>>>>,
        Arc<Mutex<IndexMap<String, Data>>>,
    ) {
        println!("[0.00s] Setting up notification channels");

        let mut results = IndexMap::new();
        results.extend((*self.initial_inputs).clone());
        println!(
            "[0.00s] Initialized with {} initial inputs",
            self.initial_inputs.len()
        );

        let notifiers = Arc::new(Mutex::new(HashMap::new()));
        let shared_results = Arc::new(Mutex::new(results));

        (notifiers, shared_results)
    }

    async fn execute_nodes(
        &self,
        sorted_nodes: Vec<String>,
        notifiers: Arc<Mutex<HashMap<String, watch::Sender<()>>>>,
        shared_results: Arc<Mutex<IndexMap<String, Data>>>,
        start_time: Instant,
    ) -> Result<IndexMap<String, Data>, DAGError> {
        for node_id in &sorted_nodes {
            let (tx, _) = watch::channel(());
            notifiers.lock().unwrap().insert(node_id.clone(), tx);
        }

        println!(
            "[{:.2}s] Spawning tasks for {} nodes",
            start_time.elapsed().as_secs_f32(),
            sorted_nodes.len()
        );

        let mut handles = Vec::new();
        for node_id in sorted_nodes {
            handles.push(self.spawn_node_task(
                node_id,
                Arc::clone(&notifiers),
                Arc::clone(&shared_results),
                start_time,
            ));
        }

        println!(
            "[{:.2}s] Waiting for all tasks to complete",
            start_time.elapsed().as_secs_f32()
        );

        for handle in handles {
            handle.await.map_err(|e| DAGError::ExecutionError {
                node_id: "unknown".to_string(),
                reason: format!("Task join error: {}", e),
            })??;
        }

        let final_results = (*shared_results.lock().unwrap()).clone();
        println!(
            "[{:.2}s] All tasks completed",
            start_time.elapsed().as_secs_f32()
        );
        println!("Final results: {:?}", final_results);

        Ok(final_results)
    }

    fn spawn_node_task(
        &self,
        node_id: String,
        notifiers: Arc<Mutex<HashMap<String, watch::Sender<()>>>>,
        shared_results: Arc<Mutex<IndexMap<String, Data>>>,
        task_start_time: Instant,
    ) -> tokio::task::JoinHandle<Result<(), DAGError>> {
        let mut receivers = HashMap::new();
        if let Some(edges) = self.edges.get(&node_id) {
            for edge in edges {
                if let Some(sender) = notifiers.lock().unwrap().get(&edge.source) {
                    receivers.insert(edge.source.clone(), sender.subscribe());
                }
            }
        }

        let nodes = Arc::clone(&self.nodes);
        let edges = Arc::clone(&self.edges);
        let initial_inputs = Arc::clone(&self.initial_inputs);
        let timeout_ms = self.config.per_node_timeout_ms();

        let node_id_for_async = node_id.clone();
        let shared_results_for_async = Arc::clone(&shared_results);
        let notifiers_for_async = Arc::clone(&notifiers);

        tokio::spawn(async move {
            println!(
                "[{:.2}s] Starting task for node {}",
                task_start_time.elapsed().as_secs_f32(),
                node_id_for_async
            );

            for receiver in receivers.values_mut() {
                if let Err(e) = receiver.changed().await {
                    return Err(DAGError::ExecutionError {
                        node_id: node_id_for_async.clone(),
                        reason: format!("Failed to receive dependency notification: {}", e),
                    });
                }
            }

            println!(
                "[{:.2}s] Node {} dependencies satisfied, executing",
                task_start_time.elapsed().as_secs_f32(),
                node_id_for_async
            );

            let node_id_for_blocking = node_id_for_async.clone();
            let shared_results_for_blocking = Arc::clone(&shared_results_for_async);

            let execution = task::spawn_blocking(move || {
                let input_data = {
                    let results_guard = shared_results_for_blocking.lock().unwrap();
                    Self::prepare_input_data(
                        &node_id_for_blocking,
                        edges
                            .get(&node_id_for_blocking)
                            .map(|e| e.as_slice())
                            .unwrap_or(&[]),
                        &results_guard,
                        &initial_inputs,
                        &nodes.get(&node_id_for_blocking).unwrap().input_type(),
                    )?
                };

                let component = nodes.get(&node_id_for_blocking).unwrap();
                let output = component.execute(input_data)?;
                Ok((node_id_for_blocking, output))
            });

            let node_id_for_error = node_id_for_async.clone();

            let result = if let Some(ms) = timeout_ms {
                match timeout(Duration::from_millis(ms), execution).await {
                    Ok(result) => result.map_err(|e| DAGError::ExecutionError {
                        node_id: node_id_for_error.clone(),
                        reason: format!("Task join error: {}", e),
                    })?,
                    Err(_) => Err(DAGError::ExecutionError {
                        node_id: node_id_for_error.clone(),
                        reason: format!("Node execution timed out after {}ms", ms),
                    }),
                }
            } else {
                execution.await.map_err(|e| DAGError::ExecutionError {
                    node_id: node_id_for_error.clone(),
                    reason: format!("Task join error: {}", e),
                })?
            };

            match result {
                Ok((id, output)) => {
                    println!(
                        "[{:.2}s] Node {} completed successfully",
                        task_start_time.elapsed().as_secs_f32(),
                        id
                    );
                    shared_results_for_async
                        .lock()
                        .unwrap()
                        .insert(id.clone(), output);
                    if let Some(sender) = notifiers_for_async.lock().unwrap().get(&id) {
                        let _ = sender.send(());
                    }
                    Ok(())
                }
                Err(e) => {
                    println!(
                        "[{:.2}s] Node {} failed: {:?}",
                        task_start_time.elapsed().as_secs_f32(),
                        node_id_for_error,
                        e
                    );
                    Err(e)
                }
            }
        })
    }

    fn prepare_input_data(
        node_id: &str,
        edges: &[Edge],
        results: &IndexMap<String, Data>,
        initial_inputs: &HashMap<String, Data>,
        expected_type: &DataType,
    ) -> Result<Data, DAGError> {
        println!("Preparing input data for node {}", node_id);

        if !edges.is_empty() {
            let mut valid_inputs = Vec::new();
            for edge in edges {
                if let Some(output) = results.get(&edge.source) {
                    valid_inputs.push(output.clone());
                } else {
                    return Err(DAGError::MissingDependency {
                        node_id: node_id.to_string(),
                        dependency_id: edge.source.clone(),
                    });
                }
            }

            if valid_inputs.len() == 1 {
                let input = valid_inputs.pop().unwrap();
                if !Self::validate_data_type(&input, expected_type) {
                    return Err(DAGError::TypeMismatch {
                        node_id: node_id.to_string(),
                        expected: expected_type.clone(),
                        actual: input.get_type(),
                    });
                }
                return Ok(input);
            }
            return Ok(Data::List(valid_inputs));
        }

        if let Some(input) = initial_inputs.get(node_id) {
            return Ok(input.clone());
        }

        Ok(Data::Null)
    }

    fn handle_caching(
        &self,
        cache: &Arc<DAGCache>,
        final_results: &IndexMap<String, Data>,
        request_id: &str,
    ) {
        if self.config.enable_history {
            println!("[0.00s] Caching results");
            let cache = Arc::clone(cache);
            let results_copy = final_results.clone();
            let inputs = self.initial_inputs.clone();
            let request_id = request_id.to_string();
            let ir_hash = self.ir_hash;

            tokio::spawn(async move {
                cache
                    .store_result(ir_hash, &inputs, results_copy, Some(request_id))
                    .await;
            });
        }
    }

    /// Replay a previous execution by request ID
    pub async fn replay(&self, request_id: &str) -> Result<IndexMap<String, Data>, DAGError> {
        if !self.config.enable_history {
            return Err(DAGError::InvalidConfiguration(
                "History replay is disabled".to_string(),
            ));
        }

        if let Some(cache) = &self.cache {
            if let Some(historical_result) = cache.get_historical_result(request_id).await {
                Ok(historical_result.node_results)
            } else {
                Err(DAGError::HistoricalResultNotFound {
                    request_id: request_id.to_string(),
                })
            }
        } else {
            Err(DAGError::InvalidConfiguration(
                "Cache not configured".to_string(),
            ))
        }
    }

    pub fn get_cached_result(&self) -> Option<DAGResult> {
        if !self.config.enable_memory_cache {
            println!("Memory cache is disabled");
            return None;
        }
        self.cache
            .as_ref()
            .and_then(|c| c.get_cached_result(self.ir_hash, &self.initial_inputs))
    }

    pub fn get_result_by_request_id(&self, request_id: &str) -> Option<DAGResult> {
        if !self.config.enable_memory_cache {
            return None;
        }
        self.cache
            .as_ref()
            .and_then(|c| c.get_result_by_request_id(request_id))
    }

    pub async fn get_cached_node_result(&self, node_id: &str) -> Option<Data> {
        if !self.config.enable_memory_cache {
            return None;
        }
        if let Some(cache) = &self.cache {
            cache.get_cached_node_result(self.ir_hash, &self.initial_inputs, node_id)
        } else {
            None
        }
    }

    pub async fn get_historical_result(&self, request_id: &str) -> Option<DAGResult> {
        if !self.config.enable_history {
            return None;
        }
        if let Some(cache) = &self.cache {
            cache.get_historical_result(request_id).await
        } else {
            None
        }
    }
}
