use std::{hash::DefaultHasher, time::Instant};

use serde_json::Value;
use sorted_vec::SortedVec;
use std::collections::BTreeMap;
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
impl DAGIR {
    /// Creates a new DAGIR from a JSON configuration, which contains a
    /// `nodes` array, an `alias` string, and an optional `metadata` object.
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
    // FIXME: break up and refactor this function
    #[allow(clippy::too_many_lines)]
    pub fn from_json(json_config: &Value) -> Result<Self, String> {
        let start = Instant::now();
        let mut nodes = SortedVec::new();
        let mut edges: BTreeMap<NodeID, Vec<Edge>> = BTreeMap::new();

        let obj = json_config
            .as_object()
            .ok_or_else(|| "Root JSON must be an object".to_string())?;

        let alias = obj
            .get("alias")
            .ok_or_else(|| "Config must contain an 'alias' key".to_string())?;
        println!("Loading DAG with alias: {alias}");

        if let Some(metadata) = obj.get("metadata") {
            println!("Loading DAG with metadata: {metadata}");
        }

        let nodes_array = obj
            .get("nodes")
            .ok_or_else(|| "Config must contain a 'nodes' key".to_string())?
            .as_array()
            .ok_or_else(|| "'nodes' must be an array".to_string())?;

        for node in nodes_array {
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
        println!("DAGIR ({alias}) from_json took {duration:?}");
        Ok(DAGIR {
            alias: alias.to_string(),
            nodes,
            edges,
        })
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
