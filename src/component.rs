use std::sync::RwLock;
use std::{
    collections::HashMap,
    hash::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use crate::dag::DAGError;
use crate::dag::NodeExecutionContext;

/// Runtime values that flow through the DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Data {
    Null,
    Integer(i32),
    Float(f64),
    Text(String),
    List(Vec<Data>),
    Json(Value),
}

impl Hash for Data {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Data::Null => {
                "Null".hash(state);
            }
            Data::Integer(value) => {
                "Integer".hash(state);
                value.hash(state);
            }
            Data::Float(value) => {
                "Float".hash(state);
                value.to_bits().hash(state);
            }
            Data::Text(value) => {
                "Text".hash(state);
                value.hash(state);
            }
            Data::List(values) => {
                "List".hash(state);
                for value in values {
                    value.hash(state);
                }
            }
            Data::Json(value) => {
                "Json".hash(state);
                value.to_string().hash(state);
            }
        }
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Data::Null, Data::Null) => true,
            (Data::Integer(a), Data::Integer(b)) => a == b,
            (Data::Float(a), Data::Float(b)) => a == b,
            (Data::Text(a), Data::Text(b)) => a == b,
            (Data::List(a), Data::List(b)) => a == b,
            (Data::Json(a), Data::Json(b)) => a == b,
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
    Float,
    Text,
    List(Box<DataType>),
    Json,
    Union(Vec<DataType>),
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
    #[must_use]
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
    #[must_use]
    pub fn as_integer(&self) -> Option<i32> {
        if let Data::Integer(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        if let Data::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[must_use]
    pub fn as_list(&self) -> Option<&[Data]> {
        if let Data::List(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[must_use]
    pub fn get_type(&self) -> DataType {
        match self {
            Data::Null => DataType::Null,
            Data::Integer(_) => DataType::Integer,
            Data::Float(_) => DataType::Float,
            Data::Text(_) => DataType::Text,
            Data::List(items) => {
                if let Some(first) = items.first() {
                    DataType::List(Box::new(first.get_type()))
                } else {
                    DataType::List(Box::new(DataType::Integer))
                }
            }
            Data::Json(_) => DataType::Json,
        }
    }

    /// Validates if this Data instance matches the expected `DataType`.
    ///
    /// This function checks if the current `Data` instance matches the expected `DataType`.
    /// It supports direct type equivalence, union compatibility, and nested list type compatibility.
    #[must_use]
    pub fn validate_type(&self, expected_type: &DataType) -> bool {
        match expected_type {
            DataType::Null => matches!(self, Data::Null),
            DataType::Integer => matches!(self, Data::Integer(_)),
            DataType::Float => matches!(self, Data::Float(_)),
            DataType::Text => matches!(self, Data::Text(_)),
            DataType::List(element_type) => {
                if let Data::List(items) = self {
                    items.iter().all(|item| item.validate_type(element_type))
                } else {
                    false
                }
            }
            DataType::Json => matches!(self, Data::Json(_)),
            DataType::Union(types) => types.iter().any(|t| self.validate_type(t)),
        }
    }
}

pub trait Component: Send + Sync + 'static {
    /// Configure a new component instance from the provided configuration
    ///
    /// # Errors
    /// Returns `component::Error::ConfigurationError` if the configuration is invalid.
    fn configure(config: Value) -> Result<Self, Error>
    where
        Self: Sized;

    /// Execute the component with the given execution context and input data
    ///
    /// # Errors
    /// Returns `DAGError` if the component execution fails.
    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError>;

    fn input_type(&self) -> DataType;

    fn output_type(&self) -> DataType;
}

type ComponentType = String;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ComponentKey {
    component_type: ComponentType,
    config_hash: u64,
}

type RegisteredComponentFactory = Arc<dyn Fn(Value) -> Result<Arc<dyn Component>, Error> + Send + Sync>;

pub struct Registry {
    unconfigured_component_factories: HashMap<ComponentType, RegisteredComponentFactory>,
    configured_component_cache: Arc<RwLock<HashMap<ComponentKey, Arc<dyn Component>>>>,
    configured_count: AtomicUsize,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            unconfigured_component_factories: HashMap::new(),
            configured_component_cache: Arc::new(RwLock::new(HashMap::new())),
            configured_count: AtomicUsize::new(0),
        }
    }

    /// Registers a new component type with the registry.
    pub fn register<C: Component + 'static>(&mut self, name: &str) {
        self.unconfigured_component_factories.insert(
            name.to_string(),
            Arc::new(|config| -> Result<Arc<dyn Component>, Error> {
                C::configure(config)
                    .map(|component| Arc::new(component) as Arc<dyn Component>)
                    .map_err(|e| Error::ConfigurationError(e.to_string()))
            }),
        );
    }

    /// Gets a configured component instance, using cache if available.
    /// This is the preferred method for getting components during DAG execution.
    ///
    /// # Errors
    /// Returns `component::Error::NotRegistered` if the component type is not registered.
    /// Returns `component::Error::CacheError` if there's an error accessing the cache.
    pub fn get_configured(&self, name: &str, config: &Value) -> Result<Arc<dyn Component>, Error> {
        let config_hash = Self::calculate_config_hash(config);
        let key = ComponentKey {
            component_type: name.to_string(),
            config_hash,
        };

        if let Ok(cache) = self.configured_component_cache.read() {
            if let Some(component) = cache.get(&key) {
                debug!("Configured component cache hit for {name}");
                return Ok(Arc::clone(component));
            }
        } else {
            return Err(Error::CacheError("Failed to acquire read lock".to_string()));
        }

        let factory = self
            .unconfigured_component_factories
            .get(name)
            .ok_or_else(|| Error::NotRegistered(name.to_string()))?;

        debug!("Configured component cache miss for {name}");
        let component = factory(config.clone())?;

        if let Ok(mut cache) = self.configured_component_cache.write() {
            cache.insert(key, Arc::clone(&component));
            self.configured_count.fetch_add(1, Ordering::Relaxed);
            Ok(component)
        } else {
            Err(Error::CacheError("Failed to acquire write lock".to_string()))
        }
    }

    /// Gets the raw component factory. This is primarily for internal use
    /// or advanced cases where you need to manage component configuration yourself.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RegisteredComponentFactory> {
        self.unconfigured_component_factories.get(name)
    }

    // Add this helper method
    fn calculate_config_hash(config: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        config.to_string().hash(&mut hasher);
        hasher.finish()
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field(
                "unconfigured_component_factories",
                &self.unconfigured_component_factories.keys().collect::<Vec<_>>(),
            )
            .field(
                "configured_component_cache",
                &self.configured_count.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum Error {
    NotRegistered(String),
    CacheError(String),
    ConfigurationError(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotRegistered(name) => {
                write!(f, "Component type '{name}' not registered")
            }
            Error::CacheError(msg) => write!(f, "Component cache error: {msg}"),
            Error::ConfigurationError(err) => write!(f, "Component configuration error: {err}"),
        }
    }
}
