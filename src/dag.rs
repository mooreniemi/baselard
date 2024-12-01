use futures::StreamExt;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sorted_vec::SortedVec;
use std::collections::BTreeMap;
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

use crate::cache::Cache;
use crate::cache::DAGResult;
use crate::component::Registry;
use crate::component::{Component, Data, DataType};

type NodeID = String;

#[derive(Debug, Clone)]
pub(crate) struct NodeIR {
    pub(crate) id: NodeID,
    pub(crate) namespace: Option<String>,
    pub(crate) component_type: String,
    pub(crate) config: Value,
    pub(crate) inputs: Option<Data>,
}

impl Eq for NodeIR {}

impl PartialEq for NodeIR {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialOrd for NodeIR {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeIR {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Debug)]
pub struct DAGIR {
    pub(crate) nodes: SortedVec<NodeIR>,
    pub(crate) edges: BTreeMap<NodeID, Vec<Edge>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct Edge {
    pub(crate) source: NodeID,
    pub(crate) target: NodeID,
}
impl DAGIR {
    /// Creates a new DAGIR from a JSON configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The root JSON is not an array
    /// - Any node is missing required fields (`id`, `component_type`, `config`)
    /// - Any node ID is empty
    /// - Any input data is of an unsupported type
    ///
    /// # Panics
    ///
    /// Panics if integer conversion fails when processing numeric inputs
    pub fn from_json(json_config: &Value) -> Result<Self, String> {
        let start = Instant::now();
        let mut nodes = SortedVec::new();
        let mut edges: BTreeMap<NodeID, Vec<Edge>> = BTreeMap::new();

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
                .ok_or_else(|| format!("Node {id} missing component_type"))?
                .to_string();

            let config = node
                .get("config")
                .ok_or_else(|| format!("Node {id} missing config"))?
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
                        .map(|i| Data::Integer(i32::try_from(i).unwrap()))
                        .ok_or_else(|| "Unsupported number type in inputs".to_string()),
                    Value::Array(arr) => {
                        let data_list = arr
                            .iter()
                            .map(|item| match item {
                                Value::String(s) => Ok(Data::Text(s.clone())),
                                Value::Number(n) => n
                                    .as_i64()
                                    .map(|i| Data::Integer(i32::try_from(i).unwrap()))
                                    .ok_or_else(|| {
                                        "Unsupported number type in array input".to_string()
                                    }),
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
                    .ok_or_else(|| format!("Node {id} depends_on must be an array"))?;

                let mut node_edges = Vec::new();
                for dep in deps {
                    let source = dep
                        .as_str()
                        .ok_or_else(|| format!("Node {id} dependency must be a string"))?
                        .to_string();

                    node_edges.push(Edge {
                        source,
                        target: id.clone(),
                    });
                }
                if !node_edges.is_empty() {
                    edges.insert(id.clone(), node_edges);
                }
            }
        }

        let duration = start.elapsed();
        println!("DAGIR from_json took {duration:?}");
        Ok(DAGIR { nodes, edges })
    }

    #[must_use]
    pub fn calculate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        for node in self.nodes.iter() {
            node.id.hash(&mut hasher);
            node.namespace.hash(&mut hasher);
            node.component_type.hash(&mut hasher);
            node.config.to_string().hash(&mut hasher);
            node.inputs.hash(&mut hasher);
        }

        for (target, edges) in &self.edges {
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
        node_id: NodeID,
        expected: DataType,
        actual: DataType,
    },
    /// Represents a missing dependency output.
    MissingDependency {
        node_id: NodeID,
        dependency_id: NodeID,
    },
    /// Represents a runtime error during component execution.
    ExecutionError { node_id: NodeID, reason: String },
    /// Represents a node not found in the DAG.
    NodeNotFound { node: NodeID },
    /// Represents invalid configuration or setup.
    InvalidConfiguration(String),
    /// Represents a cycle detected in the DAG.
    CycleDetected,
    /// Represents no valid inputs for a node.
    NoValidInputs { node_id: NodeID, expected: DataType },
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
                    "Node {node_id}: Type mismatch. Expected {expected:?}, got {actual:?}"
                )
            }
            DAGError::MissingDependency {
                node_id,
                dependency_id,
            } => {
                write!(
                    f,
                    "Node {node_id}: Missing output from dependency {dependency_id}"
                )
            }
            DAGError::ExecutionError { node_id, reason } => {
                write!(f, "Node {node_id}: Execution failed. Reason: {reason}")
            }
            DAGError::InvalidConfiguration(reason) => {
                write!(f, "Invalid configuration: {reason}")
            }
            DAGError::CycleDetected => write!(f, "Cycle detected in the DAG"),
            DAGError::NodeNotFound { node } => write!(f, "Node {node} not found"),
            DAGError::NoValidInputs { node_id, expected } => {
                write!(f, "Node {node_id}: No valid inputs. Expected {expected:?}")
            }
            DAGError::HistoricalResultNotFound { request_id } => {
                write!(f, "No historical result found for request ID: {request_id}")
            }
            DAGError::TypeSystemFailure {
                component,
                expected,
                received,
            } => {
                write!(
                    f,
                    "Type system failure in component {component}: Expected {expected:?}, received {received:?}"
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
    #[must_use]
    pub fn enable_memory_cache(&self) -> bool {
        self.enable_memory_cache
    }

    #[must_use]
    pub fn enable_history(&self) -> bool {
        self.enable_history
    }

    #[must_use]
    pub fn per_node_timeout_ms(&self) -> Option<u64> {
        self.per_node_timeout_ms
    }

    #[must_use]
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

pub type Notifiers = Arc<Mutex<HashMap<NodeID, watch::Sender<()>>>>;
pub type SharedResults = Arc<Mutex<IndexMap<NodeID, Data>>>;

pub struct DAG {
    nodes: Arc<HashMap<NodeID, Arc<dyn Component>>>,
    edges: Arc<HashMap<NodeID, Vec<Edge>>>,
    initial_inputs: Arc<HashMap<NodeID, Data>>,
    config: DAGConfig,
    cache: Option<Arc<Cache>>,
    ir_hash: u64,
}

impl std::fmt::Debug for DAG {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DAG")
            .field("dag_config", &self.config)
            .field("nodes", &self.nodes.keys().collect::<Vec<_>>())
            .field("edge_count", &self.edges.len())
            .field("initial_inputs", &self.initial_inputs)
            .field("has_cache", &self.cache.is_some())
            .field("ir_hash", &self.ir_hash)
            .finish()
    }
}

impl DAG {
    /// Creates a new DAG from an IR representation
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - There are duplicate node IDs
    /// - A node depends on a non-existent node
    /// - Component creation fails
    /// - Initial input types don't match component input types
    pub fn from_ir(
        ir: &DAGIR,
        registry: &Registry,
        config: DAGConfig,
        cache: Option<Arc<Cache>>,
    ) -> Result<Self, String> {
        let start = Instant::now();
        println!("DAGConfig: {config:?}");

        let hash_start = Instant::now();
        let ir_hash = ir.calculate_hash();
        println!("Hash calculation took {:?}", hash_start.elapsed());

        let mut nodes = HashMap::new();
        let mut edges: HashMap<NodeID, Vec<Edge>> = HashMap::new();
        let mut initial_inputs = HashMap::new();
        let mut node_ids = HashSet::new();

        let dup_start = Instant::now();
        println!("Checking for duplicate node IDs");
        for node in ir.nodes.iter() {
            if !node_ids.insert(node.id.clone()) {
                return Err(format!("Duplicate node ID found: {}", node.id));
            }
        }
        println!("Duplicate check took {:?}", dup_start.elapsed());

        println!("Registry: {registry:?}");
        println!("Creating components");

        let comp_start = Instant::now();
        for node in ir.nodes.iter() {
            println!("Starting component creation for node {}", node.id);
            let component_start = Instant::now();

            println!("Getting component from registry for node {}", node.id);
            let component = registry
                .get_configured(&node.component_type, &node.config)
                .map_err(|e| format!("Failed to get component for node {}: {}", node.id, e))?;

            println!("Component creation successful for node {}", node.id);

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
                        .or_default()
                        .push(dep.clone());
                }
            }

            nodes.insert(node.id.clone(), component);
            println!("Node {} setup took {:?}", node.id, component_start.elapsed());
        }
        println!("Total component creation took {:?}", comp_start.elapsed());

        let dag = Self {
            nodes: Arc::new(nodes),
            edges: Arc::new(edges),
            initial_inputs: Arc::new(initial_inputs),
            config,
            cache,
            ir_hash,
        };

        println!("Total DAG setup took {:?}", start.elapsed());
        Ok(dag)
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

    /// Execute the DAG with the given request ID.
    ///
    /// # Errors
    ///
    /// Returns a `DAGError` if:
    /// - Any node execution fails
    /// - There are missing dependencies
    /// - There are type mismatches between node inputs/outputs
    /// - The DAG contains cycles
    pub async fn execute(
        &self,
        request_id: Option<String>,
    ) -> Result<IndexMap<NodeID, Data>, DAGError> {
        let start_time = Instant::now();
        let request_id = request_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        println!(
            "[{:.3}s] Starting DAG execution with request_id: {}",
            start_time.elapsed().as_secs_f32(),
            request_id
        );

        if self.config.enable_memory_cache() {
            if let Some(cache) = &self.cache {
                if let Some(cached_result) =
                    cache.get_cached_result(self.ir_hash, &self.initial_inputs)
                {
                    println!(
                        "[{:.3}s] Cache hit! Returning cached result",
                        start_time.elapsed().as_secs_f32()
                    );
                    return Ok(cached_result.node_results);
                }
            }
        }

        let sorted_nodes = self.compute_execution_order(start_time.elapsed().as_secs_f32())?;

        let (notifiers, shared_results) =
            self.setup_execution_state(start_time.elapsed().as_secs_f32());

        let final_results = self
            .execute_nodes(
                sorted_nodes,
                notifiers,
                shared_results,
                start_time,
                request_id.clone(),
            )
            .await?;

        if let Some(cache) = &self.cache {
            self.handle_caching(
                cache,
                &final_results,
                &request_id,
                start_time.elapsed().as_secs_f32(),
            );
        }

        println!(
            "[{:.3}s] DAG execution completed",
            start_time.elapsed().as_secs_f32()
        );
        Ok(final_results)
    }

    fn compute_execution_order(&self, elapsed_secs: f32) -> Result<Vec<NodeID>, DAGError> {
        println!("[{elapsed_secs:.3}s] Starting topological sort");
        let mut in_degree: HashMap<NodeID, usize> = HashMap::new();
        let mut graph: HashMap<NodeID, Vec<NodeID>> = HashMap::new();

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

        println!("Initial in-degrees: {in_degree:?}");

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
            "[{elapsed_secs:.3}s] Topological sort complete. Execution order: {sorted_nodes:?}"
        );

        if sorted_nodes.len() != self.nodes.len() {
            return Err(DAGError::CycleDetected);
        }

        Ok(sorted_nodes)
    }

    fn setup_execution_state(&self, elapsed_secs: f32) -> (Notifiers, SharedResults) {
        println!("[{elapsed_secs:.3}s] Setting up notification channels");

        let mut results = IndexMap::new();
        results.extend((*self.initial_inputs).clone());
        println!(
            "[{:.3}s] Initialized with {} initial inputs",
            elapsed_secs,
            self.initial_inputs.len()
        );

        let notifiers = Arc::new(Mutex::new(HashMap::new()));
        let shared_results = Arc::new(Mutex::new(results));

        (notifiers, shared_results)
    }

    async fn execute_nodes(
        &self,
        sorted_nodes: Vec<NodeID>,
        notifiers: Notifiers,
        shared_results: SharedResults,
        start_time: Instant,
        request_id: String,
    ) -> Result<IndexMap<NodeID, Data>, DAGError> {
        println!(
            "[{:.3}s] Spawning tasks for {} nodes",
            start_time.elapsed().as_secs_f32(),
            sorted_nodes.len()
        );

        let setup_start = Instant::now();
        for node_id in &sorted_nodes {
            let (tx, _) = watch::channel(());
            notifiers.lock().unwrap().insert(node_id.clone(), tx);
        }
        println!(
            "[{:.3}s] Notification channels setup took {:.3}s",
            start_time.elapsed().as_secs_f32(),
            setup_start.elapsed().as_secs_f32()
        );

        let spawn_start = Instant::now();
        let handles: Vec<_> = sorted_nodes
            .into_iter()
            .map(|node_id| {
                let handle = self.spawn_node_task(
                    &node_id,
                    &request_id,
                    &notifiers,
                    &shared_results,
                    start_time,
                );
                (node_id, handle)
            })
            .collect();
        println!(
            "[{:.3}s] Task spawning took {:.3}s",
            start_time.elapsed().as_secs_f32(),
            spawn_start.elapsed().as_secs_f32()
        );

        let futures_start = Instant::now();
        let mut futures = Vec::new();
        let mut abort_handles = Vec::new();

        for (id, handle) in handles {
            let id_for_future = id.clone();
            futures.push(async move {
                match handle.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err((id_for_future, e)),
                    Err(e) => Err((id_for_future.clone(), DAGError::ExecutionError {
                        node_id: id_for_future,
                        reason: format!("Task join error: {e}"),
                    })),
                }
            });
            abort_handles.push(id);
        }
        println!(
            "[{:.3}s] Future preparation took {:.3}s",
            start_time.elapsed().as_secs_f32(),
            futures_start.elapsed().as_secs_f32()
        );

        let mut all_tasks = futures::stream::FuturesUnordered::from_iter(futures);
        println!(
            "[{:.3}s] Starting parallel execution of {} tasks",
            start_time.elapsed().as_secs_f32(),
            abort_handles.len()
        );

        while let Some(result) = all_tasks.next().await {
            if let Err((failed_node, error)) = result {
                println!(
                    "[{:.3}s] Node {} failed, aborting remaining tasks",
                    start_time.elapsed().as_secs_f32(),
                    failed_node
                );
                return Err(error);
            }
        }

        let final_results = (*shared_results.lock().unwrap()).clone();
        println!(
            "[{:.3}s] All tasks completed successfully",
            start_time.elapsed().as_secs_f32(),
        );

        Ok(final_results)
    }

    fn spawn_node_task(
        &self,
        node_id: &NodeID,
        request_id: &str,
        notifiers: &Notifiers,
        shared_results: &SharedResults,
        start_time: Instant,
    ) -> tokio::task::JoinHandle<Result<(), DAGError>> {
        let node_id = node_id.to_string();
        let request_id = request_id.to_string();
        let notifiers = Arc::clone(notifiers);
        let shared_results = Arc::clone(shared_results);

        let mut receivers = Self::setup_dependency_receivers(&self.edges, &node_id, &notifiers);
        let nodes = Arc::clone(&self.nodes);
        let edges = Arc::clone(&self.edges);
        let initial_inputs = Arc::clone(&self.initial_inputs);
        let timeout_ms = self.config.per_node_timeout_ms();

        tokio::spawn(async move {
            println!(
                "[{:.3}s] Node {} waiting for dependencies",
                start_time.elapsed().as_secs_f32(),
                node_id
            );
            if !receivers.is_empty() {
                Self::wait_for_dependencies(
                    &mut receivers,
                    &node_id,
                    start_time,
                    &shared_results,
                    &edges,
                ).await?;
            }

            let execution = Self::prepare_node_execution(
                node_id.clone(),
                request_id,
                nodes,
                edges,
                initial_inputs,
                shared_results.clone(),
                start_time,
            );

            let result = Self::execute_with_timeout(execution, timeout_ms, &node_id, start_time).await?;

            Self::handle_execution_result(
                result,
                &shared_results,
                &notifiers,
                start_time,
            );

            Ok(())
        })
    }

    fn setup_dependency_receivers(
        edges: &HashMap<NodeID, Vec<Edge>>,
        node_id: &NodeID,
        notifiers: &Notifiers,
    ) -> HashMap<String, watch::Receiver<()>> {
        let mut receivers = HashMap::new();
        if let Some(edges) = edges.get(node_id) {
            for edge in edges {
                if let Some(sender) = notifiers.lock().unwrap().get(&edge.source) {
                    receivers.insert(edge.source.clone(), sender.subscribe());
                }
            }
        }
        receivers
    }

    async fn wait_for_dependencies(
        receivers: &mut HashMap<String, watch::Receiver<()>>,
        node_id: &str,
        start_time: Instant,
        shared_results: &SharedResults,
        edges: &HashMap<NodeID, Vec<Edge>>,
    ) -> Result<(), DAGError> {
        let wait_start = Instant::now();

        println!(
            "[{:.3}s] Node {} waiting for {} dependencies",
            start_time.elapsed().as_secs_f32(),
            node_id,
            receivers.len()
        );
        for receiver in receivers.values_mut() {
            if let Err(e) = receiver.changed().await {
                return Err(DAGError::ExecutionError {
                    node_id: node_id.to_string(),
                    reason: format!("Failed to receive dependency notification: {e}"),
                });
            }
        }

        println!(
            "[{:.3}s] Node {} verifying {} dependency results",
            start_time.elapsed().as_secs_f32(),
            node_id,
            edges.get(node_id).map_or(0, Vec::len)
        );
        if let Some(edges) = edges.get(node_id) {
            let results = shared_results.lock().unwrap();
            for edge in edges {
                if !results.contains_key(&edge.source) {
                    return Err(DAGError::MissingDependency {
                        node_id: node_id.to_string(),
                        dependency_id: edge.source.clone(),
                    });
                }
            }
        }

        println!(
            "[{:.3}s] Node {} waited {:.3}s for dependencies",
            start_time.elapsed().as_secs_f32(),
            node_id,
            wait_start.elapsed().as_secs_f32()
        );
        Ok(())
    }

    fn prepare_node_execution(
        node_id: NodeID,
        request_id: String,
        nodes: Arc<HashMap<NodeID, Arc<dyn Component>>>,
        edges: Arc<HashMap<NodeID, Vec<Edge>>>,
        initial_inputs: Arc<HashMap<NodeID, Data>>,
        shared_results: SharedResults,
        start_time: Instant,
    ) -> task::JoinHandle<Result<(NodeID, Data), DAGError>> {
        task::spawn_blocking(move || {
            let input_data = {
                let prep_start = Instant::now();
                let results_guard = shared_results.lock().unwrap();
                let result = Self::prepare_input_data(
                    &node_id,
                    edges.get(&node_id).map_or(&[], Vec::as_slice),
                    &results_guard,
                    &initial_inputs,
                    &nodes.get(&node_id).unwrap().input_type(),
                    start_time,
                )?;
                println!(
                    "[{:.3}s] Input preparation for node {} took {:.3}s",
                    start_time.elapsed().as_secs_f32(),
                    node_id,
                    prep_start.elapsed().as_secs_f32()
                );
                result
            };

            let execution_start = Instant::now();
            let execution_context = NodeExecutionContext::new(node_id.clone(), request_id);
            let component = nodes.get(&node_id).unwrap();
            let output = component.execute(execution_context, input_data)?;
            println!(
                "[{:.3}s] Node {} execution took {:.3}s",
                start_time.elapsed().as_secs_f32(),
                node_id,
                execution_start.elapsed().as_secs_f32()
            );
            Ok((node_id, output))
        })
    }

    async fn execute_with_timeout(
        execution: task::JoinHandle<Result<(NodeID, Data), DAGError>>,
        timeout_ms: Option<u64>,
        node_id: &NodeID,
        start_time: Instant,
    ) -> Result<(NodeID, Data), DAGError> {
        let execution_start = Instant::now();

        if let Some(ms) = timeout_ms {
            match timeout(Duration::from_millis(ms), execution).await {
                Ok(result) => {
                    println!(
                        "[{:.3}s] Node {} execution took {:.3}s (was within timeout)",
                        start_time.elapsed().as_secs_f32(),
                        node_id,
                        execution_start.elapsed().as_secs_f32()
                    );
                    result.map_err(|e| DAGError::ExecutionError {
                        node_id: node_id.to_string(),
                        reason: format!("Task join error: {e}"),
                    })?
                },
                Err(_) => Err(DAGError::ExecutionError {
                    node_id: node_id.to_string(),
                    reason: format!("Node execution timed out after {ms}ms"),
                }),
            }
        } else {
            execution.await.map_err(|e| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: format!("Task join error: {e}"),
            })?
        }
    }

    fn handle_execution_result(
        result: (NodeID, Data),
        shared_results: &SharedResults,
        notifiers: &Notifiers,
        start_time: Instant,
    ) {
        let (id, output) = result;
        let store_start = Instant::now();
        shared_results.lock().unwrap().insert(id.clone(), output);
        println!(
            "[{:.3}s] Node {} result storage took {:.3}s",
            start_time.elapsed().as_secs_f32(),
            id,
            store_start.elapsed().as_secs_f32()
        );

        let notify_start = Instant::now();
        if let Some(sender) = notifiers.lock().unwrap().get(&id) {
            println!(
                "[{:.3}s] Notifying dependents of node {} completion",
                start_time.elapsed().as_secs_f32(),
                id
            );
            let _ = sender.send(());
        }
        println!(
            "[{:.3}s] Node {} notification took {:.3}s",
            start_time.elapsed().as_secs_f32(),
            id,
            notify_start.elapsed().as_secs_f32()
        );
    }

    fn prepare_input_data(
        node_id: &NodeID,
        edges: &[Edge],
        results: &IndexMap<String, Data>,
        initial_inputs: &HashMap<String, Data>,
        expected_type: &DataType,
        start_time: Instant,
    ) -> Result<Data, DAGError> {
        println!(
            "[{:.3}s] Preparing input data for node {node_id}",
            start_time.elapsed().as_secs_f32()
        );

        if !edges.is_empty() {
            if edges.len() == 1 {
                let edge = &edges[0];
                if let Some(output) = results.get(&edge.source) {
                    if !Self::validate_data_type(output, expected_type) {
                        return Err(DAGError::TypeMismatch {
                            node_id: node_id.to_string(),
                            expected: expected_type.clone(),
                            actual: output.get_type(),
                        });
                    }
                    return Ok(output.clone());
                }
                return Err(DAGError::MissingDependency {
                    node_id: node_id.to_string(),
                    dependency_id: edge.source.clone(),
                });
            }

            let mut valid_inputs = Vec::with_capacity(edges.len());
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
            return Ok(Data::List(valid_inputs));
        }

        Ok(initial_inputs.get(node_id).cloned().unwrap_or(Data::Null))
    }

    fn handle_caching(
        &self,
        cache: &Arc<Cache>,
        final_results: &IndexMap<String, Data>,
        request_id: &str,
        elapsed_secs: f32,
    ) {
        println!("[{elapsed_secs:.3}s] Starting cache storage");
        let cache_start = Instant::now();
        let cache = Arc::clone(cache);
        let results_copy = final_results.clone();
        let inputs = self.initial_inputs.clone();
        let request_id = request_id.to_string();
        let ir_hash = self.ir_hash;

        tokio::spawn(async move {
            cache.store_result(ir_hash, &inputs, results_copy, Some(request_id));
            println!(
                "[{elapsed_secs:.3}s] Cache storage spawned, setup took {:.3}s",
                cache_start.elapsed().as_secs_f32()
            );
        });
    }

    /// Replay a previous execution by request ID
    ///
    /// # Errors
    ///
    /// Returns a `DAGError` if:
    /// - History replay is disabled
    /// - Cache is not configured
    /// - No historical result found for the given request ID
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

    #[must_use]
    pub fn get_cached_result(&self) -> Option<DAGResult> {
        if !self.config.enable_memory_cache {
            println!("Memory cache is disabled");
            return None;
        }
        self.cache
            .as_ref()
            .and_then(|c| c.get_cached_result(self.ir_hash, &self.initial_inputs))
    }

    #[must_use]
    pub fn get_result_by_request_id(&self, request_id: &str) -> Option<DAGResult> {
        if !self.config.enable_memory_cache {
            return None;
        }
        self.cache
            .as_ref()
            .and_then(|c| c.get_result_by_request_id(request_id))
    }

    #[must_use]
    pub fn get_cached_node_result(&self, node_id: &str) -> Option<Data> {
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

/// When a Component is used in a DAG, it is aliased with a `NodeID`;
/// the actual Component's "identity" is its type and configuration.
/// (Because Components can be reused across different DAGs, via the
/// Registry.)
///
/// The indirection of a `NodeID` as an alias might seem strange until
/// you want to use the same Component (even with the same configuration)
/// more than once in a DAG. In that case, you can give each instance
/// a different `NodeID` and understand exactly which one ran.
///
/// (You may want duplicated Components if variation is handled internally.)
///
/// Additionally, a Node runs as part of a specific request through
/// a DAG, so it is useful to have the request ID in the context.
#[derive(Debug, Clone)]
pub struct NodeExecutionContext {
    pub node_id: NodeID,
    pub request_id: String,
}

impl NodeExecutionContext {
    #[must_use]
    pub fn new(node_id: NodeID, request_id: String) -> Self {
        Self {
            node_id,
            request_id,
        }
    }
}
