use std::{hash::DefaultHasher, time::Instant};

use serde_json::Value;
use sorted_vec::SortedVec;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::hash::Hasher;
use serde::Deserialize;

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
struct DAGConfig {
    alias: String,
    #[serde(default)]
    metadata: Option<Value>,
    nodes: Vec<NodeConfig>,
}

#[derive(Debug, Deserialize, Clone)]
struct NodeConfig {
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
        println!("DAGIR ({}, {:?}) from_json took {duration:?}", config.alias, config.metadata);

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
                let node_edges = node.depends_on.into_iter()
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
            Value::Number(n) => n.as_i64()
                .map(|i| Data::Integer(i32::try_from(i).unwrap()))
                .ok_or_else(|| "Unsupported number type in inputs".to_string()),
            Value::Array(arr) => {
                let data_list = arr.iter()
                    .map(|item| match item {
                        Value::String(s) => Ok(Data::Text(s.clone())),
                        Value::Number(n) => n.as_i64()
                            .map(|i| Data::Integer(i32::try_from(i).unwrap()))
                            .ok_or_else(|| "Unsupported number type in array input".to_string()),
                        _ => Err("Unsupported type in array input".to_string()),
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(Data::List(data_list))
            },
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
