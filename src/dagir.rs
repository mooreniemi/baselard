use std::collections::HashSet;
use std::{hash::DefaultHasher, time::Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use sorted_vec::SortedVec;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::hash::Hasher;

use crate::{component::Data, dag::NodeID};

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
    pub(crate) alias: String,
    pub(crate) nodes: SortedVec<NodeIR>,
    pub(crate) edges: BTreeMap<NodeID, Vec<Edge>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct Edge {
    pub(crate) source: NodeID,
    pub(crate) target: NodeID,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DAGConfig {
    alias: String,
    #[serde(default)]
    metadata: Option<Value>,
    nodes: Vec<NodeConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    id: String,
    component_type: String,
    config: Value,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    inputs: Option<Value>,
    #[serde(default)]
    depends_on: Vec<String>,
}

impl DAGIR {
    /// Creates a new DAGIR from a JSON configuration, which contains a
    /// `nodes` array, an `alias` string, and an optional `metadata` object.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The root JSON is not an object
    /// - The `alias` field is empty
    /// - Any node is missing required fields (`id`, `component_type`, `config`)
    pub fn from_json(json_config: &Value) -> Result<Self, String> {
        let start = Instant::now();

        let config: DAGConfig = serde_json::from_value(json_config.clone())
            .map_err(|e| format!("Invalid DAG configuration: {e}"))?;

        let result = Self::from_config(config.clone());

        let duration = start.elapsed();
        println!(
            "DAGIR ({}, {:?}) from_json took {duration:?}",
            config.alias, config.metadata
        );

        result
    }

    fn from_config(config: DAGConfig) -> Result<Self, String> {
        if config.alias.is_empty() {
            return Err("Alias cannot be empty".to_string());
        }

        let mut nodes = SortedVec::new();
        let mut edges = BTreeMap::new();

        for node in config.nodes {
            if node.id.is_empty() {
                return Err("Node ID cannot be empty".to_string());
            }

            let inputs = match node.inputs {
                Some(input_value) => Some(Self::parse_input_value(&input_value)?),
                _ => None,
            };

            nodes.push(NodeIR {
                id: node.id.clone(),
                namespace: node.namespace,
                component_type: node.component_type,
                config: node.config,
                inputs,
            });

            if !node.depends_on.is_empty() {
                let node_edges = node
                    .depends_on
                    .into_iter()
                    .map(|source| Edge {
                        source,
                        target: node.id.clone(),
                    })
                    .collect();
                edges.insert(node.id, node_edges);
            }
        }

        Ok(DAGIR {
            alias: config.alias,
            nodes,
            edges,
        })
    }

    fn parse_input_value(value: &Value) -> Result<Data, String> {
        match value {
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
                            .ok_or_else(|| "Unsupported number type in array input".to_string()),
                        _ => Err("Unsupported type in array input".to_string()),
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(Data::List(data_list))
            }
            Value::Object(_) => Ok(Data::Json(value.clone())),
            _ => Err("Unsupported input type".to_string()),
        }
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

impl DAGConfig {
    /// Creates a new DAG configuration with the given alias
    pub fn new(alias: impl Into<String>) -> Self {
        DAGConfig {
            alias: alias.into(),
            metadata: None,
            nodes: Vec::new(),
        }
    }

    /// Adds metadata to the DAG configuration
    #[must_use]
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Adds a node to the DAG configuration
    #[must_use]
    pub fn with_node(mut self, node: NodeConfig) -> Self {
        self.nodes.push(node);
        self
    }

    /// Adds multiple nodes to the DAG configuration
    #[must_use]
    pub fn with_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = NodeConfig>,
    {
        self.nodes.extend(nodes);
        self
    }

    /// Merges this DAG configuration with another, creating a new configuration that combines both.
    ///
    /// The merge process:
    /// 1. Preserves the base configuration while applying overrides
    /// 2. Tracks whether changes are structural (new nodes/edges) or config-only
    /// 3. Updates metadata including version bumps and merge history
    /// 4. Validates the resulting graph structure
    ///
    /// # Arguments
    ///
    /// * `other` - The overriding DAG configuration to merge into this one
    ///
    /// # Returns
    ///
    /// * `Ok(DAGConfig)` - A new DAG configuration combining both inputs
    /// * `Err(String)` - If the merge would create an invalid DAG
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * Attempting to change a node's component type
    /// * Creating a cyclic dependency
    /// * Creating disconnected subgraphs
    /// * Invalid version format in metadata
    ///
    /// # Example
    ///
    /// ```
    /// # use serde_json::json;
    /// # use baselard::dagir::{DAGConfig, NodeConfig};
    ///
    /// let base = DAGConfig::new("base")
    ///     .with_node(
    ///         NodeConfig::new("node1", "Transform")
    ///             .with_config(json!({"param": "original"}))
    ///     );
    ///
    /// let override_config = DAGConfig::new("override")
    ///     .with_node(
    ///         NodeConfig::new("node1", "Transform")
    ///             .with_config(json!({"param": "updated"}))
    ///     );
    ///
    /// let merged = base.merge(&override_config).unwrap();
    /// ```
    pub fn merge(&self, other: &DAGConfig) -> Result<DAGConfig, String> {
        let mut merged = self.clone();

        let is_structural_change = Self::is_structural_change(self, other);

        merged.metadata = Some(self.merge_metadata(other, is_structural_change)?);

        if other.alias != self.alias {
            merged.alias = format!("{}_merged_{}", self.alias, other.alias);
        }

        let mut node_map: BTreeMap<NodeID, NodeConfig> = self
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.clone()))
            .collect();

        for override_node in &other.nodes {
            match node_map.get_mut(&override_node.id) {
                Some(existing_node) => {
                    if override_node.component_type != existing_node.component_type {
                        return Err(format!(
                            "Cannot change component type for node {}: {} -> {}",
                            override_node.id,
                            existing_node.component_type,
                            override_node.component_type
                        ));
                    }

                    if let Some(existing_obj) = existing_node.config.as_object_mut() {
                        if let Some(override_obj) = override_node.config.as_object() {
                            existing_obj.extend(override_obj.clone());
                        }
                    } else {
                        existing_node.config = override_node.config.clone();
                    }

                    if override_node.inputs.is_some() {
                        existing_node.inputs.clone_from(&override_node.inputs);
                    }

                    let new_deps: Vec<String> = override_node
                        .depends_on
                        .iter()
                        .filter(|d| !existing_node.depends_on.contains(d))
                        .cloned()
                        .collect();
                    existing_node.depends_on.extend(new_deps);
                }

                _ => {
                    node_map.insert(override_node.id.clone(), override_node.clone());
                }
            }
        }

        merged.nodes = node_map.into_values().collect();
        Self::validate_graph_structure(&merged)?;

        Ok(merged)
    }

    fn is_structural_change(base: &DAGConfig, other: &DAGConfig) -> bool {
        let base_node_ids: HashSet<_> = base.nodes.iter().map(|n| &n.id).collect();
        let other_node_ids: HashSet<_> = other.nodes.iter().map(|n| &n.id).collect();

        if !other_node_ids.is_subset(&base_node_ids) {
            return true;
        }

        for node in &other.nodes {
            if let Some(base_node) = base.nodes.iter().find(|n| n.id == node.id) {
                if base_node.depends_on != node.depends_on {
                    return true;
                }
            }
        }

        false
    }

    fn merge_metadata(&self, other: &DAGConfig, is_structural: bool) -> Result<Value, String> {
        let mut merged = match &self.metadata {
            Some(base_meta) => base_meta
                .as_object()
                .ok_or("Base metadata must be an object")?
                .clone(),
            _ => serde_json::Map::new(),
        };

        if let Some(other_meta) = &other.metadata {
            let other_obj = other_meta
                .as_object()
                .ok_or("Override metadata must be an object")?;
            merged.extend(other_obj.clone());
        }

        let current_version = merged
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("1.0.0")
            .to_string();

        let new_version = Self::bump_version(&current_version)?;

        let history_entry = json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "base_alias": self.alias,
            "override_alias": other.alias,
            "change_type": if is_structural { "structural" } else { "config_only" },
            "previous_version": current_version,
            "new_version": new_version,
        });

        merged.insert("version".to_string(), json!(new_version));

        let mut history = merged
            .get("merge_history")
            .and_then(|h| h.as_array().cloned())
            .unwrap_or_default();
        history.push(history_entry);
        merged.insert("merge_history".to_string(), json!(history));

        Ok(Value::Object(merged))
    }

    fn bump_version(version: &str) -> Result<String, String> {
        let mut parts: Vec<u32> = version
            .split('.')
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| format!("Invalid version format: {version}"))?;

        if parts.len() != 3 {
            return Err(format!(
                "Version must be in semver format (x.y.z): {version}"
            ));
        }

        parts[2] += 1;
        Ok(format!("{}.{}.{}", parts[0], parts[1], parts[2]))
    }

    fn validate_graph_structure(&self) -> Result<(), String> {
        let mut adj_list: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for node in &self.nodes {
            adj_list.entry(&node.id).or_default();
            for dep in &node.depends_on {
                adj_list.entry(dep).or_default().push(&node.id);
            }
        }

        let mut visited = HashSet::new();
        let mut stack = HashSet::new();

        for node in &self.nodes {
            if Self::has_cycle(&adj_list, &node.id, &mut visited, &mut stack) {
                return Err(format!(
                    "Cycle detected in merged DAG starting from node {}",
                    node.id
                ));
            }
        }

        Self::check_connectivity(&adj_list)?;

        Ok(())
    }

    fn has_cycle(
        adj_list: &BTreeMap<&str, Vec<&str>>,
        node: &str,
        visited: &mut HashSet<NodeID>,
        stack: &mut HashSet<NodeID>,
    ) -> bool {
        if stack.contains(node) {
            return true;
        }
        if visited.contains(node) {
            return false;
        }

        visited.insert(node.to_string());
        stack.insert(node.to_string());

        if let Some(neighbors) = adj_list.get(node) {
            for &neighbor in neighbors {
                if Self::has_cycle(adj_list, neighbor, visited, stack) {
                    return true;
                }
            }
        }

        stack.remove(node);
        false
    }

    fn check_connectivity(adj_list: &BTreeMap<&str, Vec<&str>>) -> Result<(), String> {
        if adj_list.is_empty() {
            return Ok(());
        }

        let roots: Vec<&str> = adj_list
            .keys()
            .filter(|&&node| !adj_list.values().any(|edges| edges.contains(&node)))
            .copied()
            .collect();

        if roots.is_empty() {
            return Err("DAG has no root nodes (all nodes have dependencies)".to_string());
        }

        let mut reachable = HashSet::new();
        let mut queue = VecDeque::new();
        queue.extend(&roots);

        while let Some(node) = queue.pop_front() {
            if reachable.insert(node) {
                if let Some(neighbors) = adj_list.get(node) {
                    queue.extend(neighbors);
                }
            }
        }

        let unreachable: Vec<_> = adj_list
            .keys()
            .filter(|&&node| !reachable.contains(node))
            .collect();

        if !unreachable.is_empty() {
            return Err(format!(
                "DAG contains disconnected components. Unreachable nodes: {unreachable:?}"
            ));
        }

        Ok(())
    }
}

impl NodeConfig {
    /// Creates a new node configuration
    pub fn new(id: impl Into<String>, component_type: impl Into<String>) -> Self {
        NodeConfig {
            id: id.into(),
            component_type: component_type.into(),
            config: json!({}),
            namespace: None,
            inputs: None,
            depends_on: Vec::new(),
        }
    }

    /// Sets the node's configuration
    #[must_use]
    pub fn with_config(mut self, config: Value) -> Self {
        self.config = config;
        self
    }

    /// Sets the node's namespace
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sets the node's inputs
    #[must_use]
    pub fn with_inputs(mut self, inputs: Value) -> Self {
        self.inputs = Some(inputs);
        self
    }

    /// Adds dependencies for this node
    #[must_use]
    pub fn with_dependencies(mut self, deps: Vec<String>) -> Self {
        self.depends_on = deps;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_config_changes() {
        let base = DAGConfig {
            alias: "base".to_string(),
            metadata: Some(json!({
                "version": "1.0.0",
                "environment": "dev"
            })),
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({
                    "param1": "original",
                    "param2": 123
                }),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        let override_config = DAGConfig {
            alias: "override".to_string(),
            metadata: Some(json!({
                "environment": "prod",
                "region": "us-west"
            })),
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({
                    "param1": "updated",
                    "param3": true
                }),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        let merged = base.merge(&override_config).unwrap();

        assert_eq!(merged.alias, "base_merged_override");

        let metadata = merged.metadata.unwrap();
        assert_eq!(metadata["version"], "1.0.1");
        assert_eq!(metadata["environment"], "prod");
        assert_eq!(metadata["region"], "us-west");

        let node = &merged.nodes[0];
        let config = node.config.as_object().unwrap();
        assert_eq!(config["param1"], "updated");
        assert_eq!(config["param2"], 123);
        assert_eq!(config["param3"], true);
    }

    #[test]
    fn test_merge_structural_changes() {
        let base = DAGConfig {
            alias: "base".to_string(),
            metadata: None,
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({}),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        let override_config = DAGConfig {
            alias: "override".to_string(),
            metadata: None,
            nodes: vec![NodeConfig {
                id: "node2".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({}),
                namespace: None,
                inputs: None,
                depends_on: vec!["node1".to_string()],
            }],
        };

        let merged = base.merge(&override_config).unwrap();
        assert_eq!(merged.nodes.len(), 2);

        let node2 = merged.nodes.iter().find(|n| n.id == "node2").unwrap();
        assert_eq!(node2.depends_on, vec!["node1"]);
    }

    #[test]
    fn test_merge_invalid_component_change() {
        let base = DAGConfig {
            alias: "base".to_string(),
            metadata: None,
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({}),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        let override_config = DAGConfig {
            alias: "override".to_string(),
            metadata: None,
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "DifferentComponent".to_string(),
                config: json!({}),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        assert!(base.merge(&override_config).is_err());
    }

    #[test]
    fn test_merge_creates_cycle() {
        let base = DAGConfig {
            alias: "base".to_string(),
            metadata: None,
            nodes: vec![
                NodeConfig {
                    id: "node1".to_string(),
                    component_type: "PayloadTransformer".to_string(),
                    config: json!({}),
                    namespace: None,
                    inputs: None,
                    depends_on: vec!["node2".to_string()],
                },
                NodeConfig {
                    id: "node2".to_string(),
                    component_type: "PayloadTransformer".to_string(),
                    config: json!({}),
                    namespace: None,
                    inputs: None,
                    depends_on: vec![],
                },
            ],
        };

        let override_config = DAGConfig {
            alias: "override".to_string(),
            metadata: None,
            nodes: vec![NodeConfig {
                id: "node2".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({}),
                namespace: None,
                inputs: None,
                depends_on: vec!["node1".to_string()],
            }],
        };

        assert!(base.merge(&override_config).is_err());
    }

    #[test]
    fn test_merge_metadata_tracking() {
        let base = DAGConfig {
            alias: "base".to_string(),
            metadata: Some(json!({
                "version": "1.0.0",
                "environment": "dev"
            })),
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({}),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        let config_change = DAGConfig {
            alias: "config_override".to_string(),
            metadata: Some(json!({
                "environment": "prod"
            })),
            nodes: vec![NodeConfig {
                id: "node1".to_string(),
                component_type: "PayloadTransformer".to_string(),
                config: json!({"new_param": true}),
                namespace: None,
                inputs: None,
                depends_on: vec![],
            }],
        };

        let merged = base.merge(&config_change).unwrap();
        let metadata = merged.metadata.as_ref().unwrap();

        assert_eq!(metadata["version"], "1.0.1");
        assert_eq!(metadata["environment"], "prod");

        let history = metadata["merge_history"].as_array().unwrap();
        let last_merge = history.last().unwrap();
        assert_eq!(last_merge["change_type"], "config_only");
        assert_eq!(last_merge["previous_version"], "1.0.0");
        assert_eq!(last_merge["new_version"], "1.0.1");

        let structural_change = DAGConfig {
            alias: "structural_override".to_string(),
            metadata: None,
            nodes: vec![
                NodeConfig {
                    id: "node1".to_string(),
                    component_type: "PayloadTransformer".to_string(),
                    config: json!({}),
                    namespace: None,
                    inputs: None,
                    depends_on: vec![],
                },
                NodeConfig {
                    id: "node2".to_string(),
                    component_type: "PayloadTransformer".to_string(),
                    config: json!({}),
                    namespace: None,
                    inputs: None,
                    depends_on: vec!["node1".to_string()],
                },
            ],
        };

        let merged = merged.merge(&structural_change).unwrap();
        let metadata = merged.metadata.unwrap();

        assert_eq!(metadata["version"], "1.0.2");

        let history = metadata["merge_history"].as_array().unwrap();
        let last_merge = history.last().unwrap();
        assert_eq!(last_merge["change_type"], "structural");
        assert_eq!(last_merge["previous_version"], "1.0.1");
        assert_eq!(last_merge["new_version"], "1.0.2");
    }
}
