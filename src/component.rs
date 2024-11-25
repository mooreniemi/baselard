use std::{collections::HashMap, sync::Arc};

use serde_json::Value;
use tokio::sync::{oneshot, Mutex};

use crate::dag::DAGError;

/// Runtime values that flow through the DAG
#[derive(Debug, Clone)]
pub enum Data {
    Null,
    Integer(i32),
    Text(String),
    List(Vec<Data>),
    Json(Value),
    /// A channel for single-consumer asynchronous results, wrapped in an `Arc<Mutex>` for safe sharing.
    OneConsumerChannel(Arc<Mutex<Option<oneshot::Receiver<Data>>>>),
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Data::Null, Data::Null) => true,
            (Data::Integer(a), Data::Integer(b)) => a == b,
            (Data::Text(a), Data::Text(b)) => a == b,
            (Data::List(a), Data::List(b)) => a == b,
            (Data::Json(a), Data::Json(b)) => a == b,
            // For channels, we'll consider them equal if they're both None
            (Data::OneConsumerChannel(a), Data::OneConsumerChannel(b)) => {
                // Compare if both are None
                matches!(
                    (a.try_lock().ok().as_ref().map(|g| g.is_none()),
                     b.try_lock().ok().as_ref().map(|g| g.is_none())),
                    (Some(true), Some(true))
                )
            },
            _ => false,
        }
    }
}

/// Type information for validation during DAG construction
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// Represents the absence of input for a component.
    Null,
    Integer,
    Text,
    List(Box<DataType>),
    Json,
    Union(Vec<DataType>),
    /// Represents a single-consumer channel carrying a specific data type.
    OneConsumerChannel(Box<DataType>),
}

impl DataType {
    /// Determines whether one `DataType` is compatible with another.
    ///
    /// This function checks if a value of the current `DataType` (`self`) can be
    /// safely used as input where the target `DataType` (`other`) is expected.
    /// It supports direct type equivalence, union compatibility, and nested list type compatibility.
    ///
    /// ### Compatibility Rules:
    /// - **Exact Match**: Two data types are directly compatible if they are equal.
    /// - **Union Compatibility**: A `DataType` is compatible with a `DataType::Union` if it is compatible
    ///   with at least one of the types in the union.
    /// - **List Compatibility**: Two `DataType::List` types are compatible if their element types are compatible.
    /// - **Otherwise**: The types are considered incompatible.
    ///
    /// ### Parameters:
    /// - `other`: The target `DataType` to check compatibility against.
    ///
    /// ### Returns:
    /// - `true` if `self` is compatible with `other`.
    /// - `false` otherwise.
    ///
    /// ### Examples:
    /// #### Example 1: Direct Compatibility
    /// ```rust
    /// use baselard::component::DataType;
    /// let a = DataType::Integer;
    /// let b = DataType::Integer;
    /// assert!(a.is_compatible_with(&b)); // true
    /// ```
    ///
    /// #### Example 2: Union Compatibility
    /// ```rust
    /// use baselard::component::DataType;
    /// let source = DataType::Text;
    /// let target = DataType::Union(vec![DataType::Integer, DataType::Text]);
    /// assert!(source.is_compatible_with(&target)); // true
    /// ```
    ///
    /// #### Example 3: List Compatibility
    /// ```rust
    /// use baselard::component::DataType;
    /// let source = DataType::List(Box::new(DataType::Integer));
    /// let target = DataType::List(Box::new(DataType::Integer));
    /// assert!(source.is_compatible_with(&target)); // true
    /// ```
    ///
    /// #### Example 4: Nested List Compatibility
    /// ```rust
    /// use baselard::component::DataType;
    /// let source = DataType::List(Box::new(DataType::List(Box::new(DataType::Text))));
    /// let target = DataType::List(Box::new(DataType::List(Box::new(DataType::Text))));
    /// assert!(source.is_compatible_with(&target)); // true
    /// ```
    ///
    /// #### Example 5: Incompatible Types
    /// ```rust
    /// use baselard::component::DataType;
    /// let source = DataType::Integer;
    /// let target = DataType::Text;
    /// assert!(!source.is_compatible_with(&target)); // false
    /// ```
    pub fn is_compatible_with(&self, other: &DataType) -> bool {
        match (self, other) {
            (a, b) if a == b => true,

            (source_type, DataType::Union(target_types)) => target_types
                .iter()
                .any(|t| source_type.is_compatible_with(t)),

            (DataType::List(a), DataType::List(b)) => a.is_compatible_with(b),

            _ => false,
        }
    }
}

impl Data {
    pub fn as_integer(&self) -> Option<i32> {
        if let Data::Integer(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        if let Data::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_list(&self) -> Option<&[Data]> {
        if let Data::List(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            Data::Null => DataType::Null,
            Data::Integer(_) => DataType::Integer,
            Data::Text(_) => DataType::Text,
            Data::List(items) => {
                if let Some(first) = items.first() {
                    DataType::List(Box::new(first.get_type()))
                } else {
                    DataType::List(Box::new(DataType::Integer))
                }
            }
            Data::Json(_) => DataType::Json,
            Data::OneConsumerChannel(_) => DataType::List(Box::new(DataType::Integer)),
        }
    }
}

pub type ComponentResult = Result<Data, DAGError>;

pub trait Component: Send + Sync + 'static {
    fn configure(config: Value) -> Self
    where
        Self: Sized;

    fn execute(&self, input: Data) -> ComponentResult;

    fn input_type(&self) -> DataType;

    fn output_type(&self) -> DataType;

    fn is_deferrable(&self) -> bool {
        false
    }
}

pub struct ComponentRegistry {
    components: HashMap<String, Arc<dyn Fn(Value) -> Box<dyn Component>>>,
}

impl ComponentRegistry {
    pub fn new() -> Self {
        Self {
            components: HashMap::new(),
        }
    }

    pub fn register<C: Component + 'static>(&mut self, name: &str) {
        self.components.insert(
            name.to_string(),
            Arc::new(|config| Box::new(C::configure(config)) as Box<dyn Component>),
        );
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn Fn(Value) -> Box<dyn Component>>> {
        self.components.get(name)
    }
}

impl std::fmt::Debug for ComponentRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentRegistry")
            .field("registered_components", &self.components.keys().collect::<Vec<_>>())
            .finish()
    }
}