use std::collections::{HashMap, HashSet};

use ascii_tree::Tree;

use crate::dag::DAGIR;

#[derive(Debug, Clone, Copy)]
pub enum TreeView {
    /// top-down from roots
    Execution,
    /// bottom-up from leaves
    Dependency,
}

impl DAGIR {
    #[must_use]
    pub fn build_tree(&self, view: TreeView) -> Tree {
        match view {
            TreeView::Execution => {
                // Build a map of each node to its children
                let mut node_map: HashMap<&String, Vec<&String>> = HashMap::new();

                // For each node, look at its dependencies to build parent->child relationships
                for node in &self.nodes {
                    if let Some(edges) = self.edges.get(&node.id) {
                        node_map.entry(&node.id).or_default(); // Ensure every node has an entry
                        for edge in edges {
                            node_map.entry(&edge.source).or_default().push(&edge.target);
                        }
                    } else {
                        node_map.entry(&node.id).or_default(); // Handle nodes with no edges
                    }
                }

                // Find start nodes (roots for execution, leaves for dependency)
                let all_targets: HashSet<_> = self
                    .edges
                    .values()
                    .flat_map(|edges| edges.iter().map(|edge| &edge.target))
                    .collect();
                let start_nodes: Vec<_> = self
                    .nodes
                    .iter()
                    .map(|node| &node.id)
                    .filter(|id| !all_targets.contains(id))
                    .collect();

                Self::build_subtree(&format!("DAG:{view:?}"), &start_nodes, &node_map, view)
            }
            TreeView::Dependency => {
                // Build a map of each node to its parents
                let mut node_map: HashMap<&String, Vec<&String>> = HashMap::new();

                for node in &self.nodes {
                    node_map.entry(&node.id).or_default();
                    if let Some(edges) = self.edges.get(&node.id) {
                        for edge in edges {
                            node_map.entry(&edge.target).or_default().push(&edge.source);
                        }
                    }
                }

                // Find leaf nodes (nodes with no children)
                let all_sources: HashSet<_> = self
                    .edges
                    .values()
                    .flat_map(|edges| edges.iter().map(|edge| &edge.source))
                    .collect();
                let start_nodes: Vec<_> = self
                    .nodes
                    .iter()
                    .map(|node| &node.id)
                    .filter(|id| !all_sources.contains(id))
                    .collect();

                Self::build_subtree(&format!("DAG:{view:?}"), &start_nodes, &node_map, view)
            }
        }
    }

    #[allow(clippy::items_after_statements)]
    fn build_subtree<'a>(
        node_id: &str,
        start_nodes: &[&'a String],
        node_map: &HashMap<&'a String, Vec<&'a String>>,
        view: TreeView,
    ) -> Tree {
        if node_id == format!("DAG:{view:?}") {
            let children = start_nodes
                .iter()
                .map(|id| Self::build_subtree(id, start_nodes, node_map, view))
                .collect();
            return Tree::Node(node_id.to_string(), children);
        }

        let children = node_map
            .get(&node_id.to_string())
            .unwrap_or(&vec![])
            .iter()
            .map(|child_id| Self::build_subtree(child_id, start_nodes, node_map, view))
            .collect::<Vec<Tree>>();

        Tree::Node(node_id.to_string(), children)
    }
}
