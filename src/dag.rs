use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher, DefaultHasher};
use std::sync::Arc;
use std::time::Duration;
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

        Ok(DAGIR { nodes, edges })
    }

    pub fn calculate_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash nodes in a deterministic order
        let mut sorted_nodes = self.nodes.clone();
        sorted_nodes.sort_by(|a, b| a.id.cmp(&b.id));

        for node in &sorted_nodes {
            node.id.hash(&mut hasher);
            node.namespace.hash(&mut hasher);
            node.component_type.hash(&mut hasher);
            node.config.to_string().hash(&mut hasher);  // Convert Value to string for hashing
            node.inputs.hash(&mut hasher);
        }

        // Hash edges in a deterministic order
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
                write!(f, "No historical result found for request ID: {}", request_id)
            }
            DAGError::TypeSystemFailure { component, expected, received } => {
                write!(f, "Type system failure in component {}: Expected {:?}, received {:?}", component, expected, received)
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
            per_node_timeout_ms: Some(100),
            enable_memory_cache: false,
            enable_history: false,
        }
    }
}

impl Default for DAGConfig {
    fn default() -> Self {
        DAGConfig {
            per_node_timeout_ms: Some(100),
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
            DataType::OneConsumerChannel(_) => matches!(data, Data::OneConsumerChannel(_)),
            DataType::Union(types) => types.iter().any(|t| DAG::validate_data_type(data, t)),
        }
    }

    pub async fn execute(
        &self,
        request_id: Option<String>,
    ) -> Result<IndexMap<String, Data>, DAGError> {
        let request_id = request_id.unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut results: IndexMap<String, Data> = IndexMap::new();
        results.extend((*self.initial_inputs).clone());

        let levels = self.group_into_levels()?;
        let final_results = self.execute_levels(levels, results).await?;

        if let Some(cache) = &self.cache {
            if self.config.enable_history {
                let cache = Arc::clone(cache);
                let copy_of_final_results = final_results.clone();
                let inputs = self.initial_inputs.clone();
                let request_id = request_id.clone();
                let ir_hash = self.ir_hash;
                tokio::spawn(async move {
                    cache
                        .store_result(ir_hash, &inputs, copy_of_final_results, Some(request_id))
                        .await;
                });
            }
        }

        Ok(final_results)
    }

    pub fn get_cached_result(&self) -> Option<DAGResult> {
        if !self.config.enable_memory_cache {
            println!("Memory cache is disabled");
            return None;
        }
        self.cache.as_ref().and_then(|c|
            c.get_cached_result(self.ir_hash, &self.initial_inputs)
        )
    }

    pub fn get_result_by_request_id(&self, request_id: &str) -> Option<DAGResult> {
        if !self.config.enable_memory_cache {
            return None;
        }
        self.cache.as_ref().and_then(|c|
            c.get_result_by_request_id(request_id)
        )
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

    fn group_into_levels(&self) -> Result<Vec<Vec<String>>, DAGError> {
        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut remaining_nodes: HashSet<String> = self.nodes.keys().cloned().collect();
        let mut deferred_nodes: HashSet<String> = HashSet::new();

        for (node_id, component) in self.nodes.iter() {
            if component.is_deferrable() {
                deferred_nodes.insert(node_id.clone());
                self.mark_dependent_nodes_deferred(node_id, &mut deferred_nodes);
            }
        }
        println!("ZERO PASS: Deferred nodes: {:?}", deferred_nodes);

        let mut processed_nodes: HashSet<String> = HashSet::new();
        while remaining_nodes.iter().any(|n| !deferred_nodes.contains(n)) {
            println!("FIRST PASS: Remaining nodes: {:?}", remaining_nodes);
            let ready_nodes: Vec<String> = remaining_nodes
                .iter()
                .filter(|node| {
                    if deferred_nodes.contains(*node) {
                        return false;
                    }
                    let deps = self.edges.get(*node).map(|d| d.as_slice()).unwrap_or(&[]);
                    deps.iter().all(|dep| processed_nodes.contains(&dep.source))
                })
                .cloned()
                .collect();

            if ready_nodes.is_empty() && remaining_nodes.iter().any(|n| !deferred_nodes.contains(n))
            {
                return Err(DAGError::CycleDetected);
            }

            if !ready_nodes.is_empty() {
                levels.push(ready_nodes.clone());
                for node in ready_nodes {
                    processed_nodes.insert(node.clone());
                    remaining_nodes.remove(&node);
                }
            }
        }

        while !remaining_nodes.is_empty() {
            println!("SECOND PASS: Remaining nodes: {:?}", remaining_nodes);
            let ready_nodes: Vec<String> = remaining_nodes
                .iter()
                .filter(|node| {
                    let deps = self.edges.get(*node).map(|d| d.as_slice()).unwrap_or(&[]);
                    deps.iter()
                        .all(|dep| !remaining_nodes.contains(&dep.source))
                })
                .cloned()
                .collect();

            if ready_nodes.is_empty() {
                return Err(DAGError::CycleDetected);
            }

            let mut current_level = if !levels.is_empty() {
                levels.pop().unwrap()
            } else {
                Vec::new()
            };

            for node in ready_nodes {
                println!(
                    "SECOND PASS: Checking if {} can execute together with {:?}",
                    node, current_level
                );
                if current_level.iter().all(|other| {
                    self.can_execute_together(other, &node)
                        && !self.has_dependency_between(other, &node)
                        && !self.has_dependency_between(&node, other)
                }) {
                    current_level.push(node.clone());
                } else {
                    levels.push(current_level);
                    current_level = vec![node.clone()];
                }
                remaining_nodes.remove(&node);
            }

            if !current_level.is_empty() {
                levels.push(current_level);
            }
        }

        Ok(levels)
    }

    fn can_execute_together(&self, node1: &str, node2: &str) -> bool {
        println!("Checking if {} and {} can execute together", node1, node2);
        let deps1 = self
            .edges
            .get(node1)
            .map(|e| e.iter().map(|edge| &edge.source).collect::<HashSet<_>>())
            .unwrap_or_default();
        let deps2 = self
            .edges
            .get(node2)
            .map(|e| e.iter().map(|edge| &edge.source).collect::<HashSet<_>>())
            .unwrap_or_default();

        let result = deps1.is_disjoint(&deps2);
        println!("{} and {} can execute together: {}", node1, node2, result);
        result
    }

    fn has_dependency_between(&self, from: &str, to: &str) -> bool {
        self.edges
            .get(to)
            .map(|edges| edges.iter().any(|e| e.source == from))
            .unwrap_or(false)
    }

    fn mark_dependent_nodes_deferred(&self, node: &str, deferred_nodes: &mut HashSet<String>) {
        for (target, edges) in self.edges.iter() {
            if edges.iter().any(|edge| edge.source == node) {
                if deferred_nodes.insert(target.clone()) {
                    self.mark_dependent_nodes_deferred(target, deferred_nodes);
                }
            }
        }
    }

    async fn execute_levels(
        &self,
        levels: Vec<Vec<String>>,
        mut results: IndexMap<String, Data>,
    ) -> Result<IndexMap<String, Data>, DAGError> {
        for (level_idx, level) in levels.iter().enumerate() {
            println!("Executing level {}: {:?}", level_idx, level);

            let level_results = self.execute_level(level, &results).await?;
            results.extend(level_results);
        }

        println!("Final results: {:?}", results);
        Ok(results)
    }

    async fn execute_level(
        &self,
        level: &[String],
        results: &IndexMap<String, Data>,
    ) -> Result<Vec<(String, Data)>, DAGError> {
        let timeout_ms = self.config.per_node_timeout_ms.unwrap_or(100);
        let nodes = Arc::clone(&self.nodes);
        let edges = Arc::clone(&self.edges);
        let initial_inputs = Arc::clone(&self.initial_inputs);

        let level_results = futures::future::join_all(level.iter().map(|node_id| {
            let node_id = node_id.clone();
            let results = results.clone();
            let nodes = Arc::clone(&nodes);
            let edges = Arc::clone(&edges);
            let initial_inputs = Arc::clone(&initial_inputs);

            async move {
                let node_id_for_error = node_id.clone();

                println!("Executing node {} in spawn_blocking", node_id);
                let handle = tokio::task::spawn_blocking(move || {
                    Self::execute_node(&node_id, &results, &nodes, &edges, &initial_inputs)
                });

                match timeout(Duration::from_millis(timeout_ms), handle).await {
                    Ok(Ok(Ok(result))) => Ok(result),
                    Ok(Ok(Err(e))) => Err(e),
                    Ok(Err(join_error)) => Err(DAGError::ExecutionError {
                        node_id: node_id_for_error,
                        reason: format!("Task join error: {:?}", join_error),
                    }),
                    Err(_) => Err(DAGError::ExecutionError {
                        node_id: node_id_for_error,
                        reason: format!("Execution timed out after {}ms", timeout_ms),
                    }),
                }
            }
        }))
        .await;

        level_results
            .into_iter()
            .map(|res| res.map_err(|e| e))
            .collect()
    }

    fn execute_node(
        node_id: &str,
        results: &IndexMap<String, Data>,
        nodes: &HashMap<String, Box<dyn Component>>,
        edges: &HashMap<String, Vec<Edge>>,
        initial_inputs: &HashMap<String, Data>,
    ) -> Result<(String, Data), DAGError> {
        let component = nodes.get(node_id).ok_or_else(|| DAGError::ExecutionError {
            node_id: node_id.to_string(),
            reason: "Component not found".to_string(),
        })?;

        let expected_input_type = component.input_type();

        let input_data = if expected_input_type == DataType::Null {
            Data::Null
        } else {
            Self::prepare_input_data(
                node_id,
                edges.get(node_id).map(|e| e.as_slice()).unwrap_or(&[]),
                results,
                initial_inputs,
                &expected_input_type,
            )?
        };

        if expected_input_type != DataType::Null
            && !Self::validate_data_type(&input_data, &expected_input_type)
        {
            return Err(DAGError::TypeMismatch {
                node_id: node_id.to_string(),
                expected: expected_input_type,
                actual: input_data.get_type(),
            });
        }

        let output = component
            .execute(input_data)
            .map_err(|err| DAGError::ExecutionError {
                node_id: node_id.to_string(),
                reason: err.to_string(),
            })?;

        if !Self::validate_data_type(&output, &component.output_type()) {
            return Err(DAGError::TypeMismatch {
                node_id: node_id.to_string(),
                expected: component.output_type(),
                actual: output.get_type(),
            });
        }

        Ok((node_id.to_string(), output))
    }

    fn prepare_input_data(
        node_id: &str,
        deps: &[Edge],
        results: &IndexMap<String, Data>,
        initial_inputs: &HashMap<String, Data>,
        expected_input_type: &DataType,
    ) -> Result<Data, DAGError> {
        println!("Preparing input data for node {}", node_id);
        if deps.is_empty() {
            Ok(initial_inputs.get(node_id).cloned().unwrap_or(Data::Null))
        } else if deps.len() == 1 {
            let dep = &deps[0];
            let dep_output =
                results
                    .get(&dep.source)
                    .cloned()
                    .ok_or_else(|| DAGError::MissingDependency {
                        node_id: node_id.to_string(),
                        dependency_id: dep.source.clone(),
                    })?;
            if Self::validate_data_type(&dep_output, expected_input_type) {
                Ok(dep_output)
            } else {
                Err(DAGError::TypeMismatch {
                    node_id: node_id.to_string(),
                    expected: expected_input_type.clone(),
                    actual: dep_output.get_type(),
                })
            }
        } else {
            if let DataType::List(inner_type) = expected_input_type {
                let aggregated_results: Vec<_> = deps
                    .iter()
                    .filter_map(|dep| {
                        results.get(&dep.source).cloned().and_then(|data| {
                            if Self::validate_data_type(&data, inner_type) {
                                Some(data)
                            } else {
                                None
                            }
                        })
                    })
                    .collect();

                if aggregated_results.is_empty() {
                    Err(DAGError::NoValidInputs {
                        node_id: node_id.to_string(),
                        expected: expected_input_type.clone(),
                    })
                } else {
                    Ok(Data::List(aggregated_results))
                }
            } else {
                let aggregated_results: Vec<_> = deps
                    .iter()
                    .filter_map(|dep| {
                        results.get(&dep.source).cloned().and_then(|data| {
                            if Self::validate_data_type(&data, expected_input_type) {
                                Some(data)
                            } else {
                                None
                            }
                        })
                    })
                    .collect();

                if aggregated_results.is_empty() {
                    Err(DAGError::NoValidInputs {
                        node_id: node_id.to_string(),
                        expected: expected_input_type.clone(),
                    })
                } else {
                    Ok(Data::List(aggregated_results))
                }
            }
        }
    }

    /// Replay a previous execution by request ID
    pub async fn replay(&self, request_id: &str) -> Result<IndexMap<String, Data>, DAGError> {
        if !self.config.enable_history {
            return Err(DAGError::InvalidConfiguration(
                "History replay is disabled".to_string()
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
                "Cache not configured".to_string()
            ))
        }
    }
}
