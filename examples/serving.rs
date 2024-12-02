use axum::{
    extract::{Json, Path, Query, State},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};

use baselard::{
    cache::Cache,
    components::{
        crash_test_dummy::CrashTestDummy, data_to_json_processor::DataToJsonProcessor,
        json_combiner::JsonCombiner, json_to_data_processor::JsonToDataProcessor,
        ml_model::MLModel, replay::Replay, string_length_counter::StringLengthCounter,
    },
};
use baselard::{dag_visualizer::TreeView, dagir::DAGConfig};

use baselard::{
    component::{Component, Data, DataType, Error, Registry},
    components::{adder::Adder, payload_transformer::PayloadTransformer},
    dag::{DAGError, DAGSettings, NodeExecutionContext, DAG},
    dagir::DAGIR,
};
use core::time::Duration;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;
use std::{sync::Arc, thread};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Multiplier {
    value: f64,
}

impl Component for Multiplier {
    fn configure(config: serde_json::Value) -> Result<Self, Error> {
        let multiplier = config["multiplier"].as_f64().unwrap_or(0.0);
        Ok(Self { value: multiplier })
    }

    #[allow(clippy::cast_possible_truncation)]
    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        let input_value = match input {
            Data::Null => 0.0,
            Data::Integer(n) => f64::from(n),
            Data::List(list) => list
                .iter()
                .filter_map(baselard::component::Data::as_integer)
                .map(f64::from)
                .sum(),
            Data::Json(value) => {
                info!(
                    "Multiplier {}: received JSON value: {value:?}",
                    context.node_id
                );
                #[allow(clippy::cast_precision_loss)]
                if let Some(num) = value.as_i64() {
                    num as f64
                } else if let Some(num) = value.as_f64() {
                    num
                } else if let Some(list) = value.as_array() {
                    list.iter().filter_map(serde_json::Value::as_f64).sum()
                } else if let Some(text) = value.as_str() {
                    text.parse::<f64>().unwrap_or(0.0)
                } else {
                    info!("Multiplier received unparseable JSON value: {value:?}");
                    0.0
                }
            }
            _ => {
                return Err(DAGError::TypeSystemFailure {
                    component: "Multiplier".to_string(),
                    expected: self.input_type(),
                    received: input.get_type(),
                })
            }
        };

        Ok(Data::Integer((input_value * self.value) as i32))
    }

    fn input_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Null,
            DataType::Integer,
            DataType::List(Box::new(DataType::Integer)),
            DataType::Json, // This allows for payload transformations
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}

struct AppState {
    registry: Arc<Registry>,
    cache: Arc<Cache>,
    dag_configs: Arc<RwLock<HashMap<String, DAGConfig>>>,
}

#[derive(Deserialize)]
struct AliasExecuteRequest {
    overrides: Option<DAGConfig>,
}

/// Convenience endpoint to view the DAG in a tree format, incidentally validates
async fn view_dag(
    State(_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Json(dag_config_json): Json<Value>,
) -> impl IntoResponse {
    let view_type =
        params
            .get("mode")
            .map_or(TreeView::Dependency, |v| match v.to_lowercase().as_str() {
                "execution" => TreeView::Execution,
                _ => TreeView::Dependency,
            });

    let ir = match DAGIR::from_json(&dag_config_json) {
        Ok(ir) => ir,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("Invalid DAG configuration: {e}"),
            )
                .into_response()
        }
    };

    let tree = ir.build_tree(view_type);
    let mut output = String::new();

    if let Err(e) = ascii_tree::write_tree(&mut output, &tree) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to generate tree visualization: {e}"),
        )
            .into_response();
    }

    match Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(output)
    {
        Ok(response) => response.into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to construct response: {e}"),
        )
            .into_response(),
    }
}

async fn execute_dag(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(config): Json<DAGConfig>,
) -> Response {
    let start = Instant::now();

    let use_cache = !headers
        .get(axum::http::header::CACHE_CONTROL)
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.to_lowercase().contains("no-cache"));

    {
        let mut configs = state.dag_configs.write().unwrap();
        configs.insert(config.alias.clone(), config.clone());
    }

    let result = match DAGIR::from_config(config) {
        Ok(ir) => {
            let mut dag_config = DAGSettings::default();
            if !use_cache {
                dag_config.enable_memory_cache = false;
            }

            match DAG::from_ir(
                &ir,
                &state.registry,
                dag_config,
                Some(Arc::clone(&state.cache)),
            ) {
                Ok(dag) => dag.execute(None).await,
                Err(e) => Err(DAGError::InvalidConfiguration(e.to_string())),
            }
        }
        Err(e) => Err(DAGError::InvalidConfiguration(e)),
    };

    let elapsed = start.elapsed().as_millis();
    info!("DAG execution took {elapsed}ms");

    match result {
        Ok(outputs) => Json(json!({
            "success": true,
            "results": outputs,
            "took_ms": elapsed,
            "cache_enabled": !headers
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|h| h.to_str().ok())
                .is_some_and(|s| s.to_lowercase().contains("no-cache"))
        }))
        .into_response(),
        Err(err) => Json(json!({
            "success": false,
            "error": format!("{:?}", err),
            "took_ms": elapsed,
            "cache_enabled": !headers
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|h| h.to_str().ok())
                .is_some_and(|s| s.to_lowercase().contains("no-cache"))
        }))
        .into_response(),
    }
}

async fn execute_by_alias(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<AliasExecuteRequest>,
) -> Response {
    let start = Instant::now();

    let base_config = {
        let configs = state.dag_configs.read().unwrap();
        match configs.get(&alias) {
            Some(config) => config.clone(),
            _ => {
                return (
                    axum::http::StatusCode::NOT_FOUND,
                    format!("No DAG configuration found for alias: {alias}"),
                )
                    .into_response();
            }
        }
    };

    let config = if let Some(overrides) = request.overrides {
        match base_config.merge(&overrides) {
            Ok(merged) => merged,
            Err(e) => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("Failed to merge override configuration: {e}"),
                )
                    .into_response();
            }
        }
    } else {
        base_config
    };

    let use_cache = !headers
        .get(axum::http::header::CACHE_CONTROL)
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.to_lowercase().contains("no-cache"));

    let result = match DAGIR::from_config(config.clone()) {
        Ok(ir) => {
            let mut dag_config = DAGSettings::default();
            if !use_cache {
                dag_config.enable_memory_cache = false;
            }

            match DAG::from_ir(
                &ir,
                &state.registry,
                dag_config,
                Some(Arc::clone(&state.cache)),
            ) {
                Ok(dag) => dag.execute(None).await,
                Err(e) => Err(DAGError::InvalidConfiguration(e.to_string())),
            }
        }
        Err(e) => Err(DAGError::InvalidConfiguration(e)),
    };

    let elapsed = start.elapsed().as_millis();
    let config_alias = config.alias.clone();

    match result {
        Ok(outputs) => Json(json!({
            "success": true,
            "results": outputs,
            "took_ms": elapsed,
            "cache_enabled": use_cache,
            "base_alias": alias,
            "config_alias": config_alias
        }))
        .into_response(),
        Err(err) => Json(json!({
            "success": false,
            "error": format!("{:?}", err),
            "took_ms": elapsed,
            "cache_enabled": use_cache,
            "base_alias": alias,
            "config_alias": config_alias
        }))
        .into_response(),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        .init();

    thread::spawn(move || loop {
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if !deadlocks.is_empty() {
            error!("{} deadlock(s) detected!", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                error!("Deadlock #{i} involves the following threads:");
                for thread in threads {
                    error!(" - Thread ID: {:?}", thread.thread_id());
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
    });

    let mut registry = Registry::new();
    registry.register::<Adder>("Adder");
    registry.register::<Multiplier>("Multiplier");
    registry.register::<StringLengthCounter>("StringLengthCounter");
    registry.register::<CrashTestDummy>("CrashTestDummy");
    registry.register::<PayloadTransformer>("PayloadTransformer");
    registry.register::<DataToJsonProcessor>("DataToJsonProcessor");
    registry.register::<JsonToDataProcessor>("JsonToDataProcessor");
    registry.register::<JsonCombiner>("JsonCombiner");
    registry.register::<MLModel>("MLModel");
    registry.register::<Replay>("Replay");

    let cache = Cache::new(Some("/tmp/axum_dag_history.jsonl"), 10_000);

    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        cache: Arc::new(cache),
        dag_configs: Arc::new(RwLock::new(HashMap::new())),
    });

    let app = Router::new()
        .route("/execute", post(execute_dag))
        .route("/execute/:alias", post(execute_by_alias))
        .route("/view", post(view_dag))
        .with_state(state);

    info!("Server running on http://localhost:3000");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}
