use baselard::component::ComponentRegistry;
use baselard::component::Data;
use baselard::components::*;
use baselard::dag::{DAGConfig, DAG, DAGIR};
use indexmap::IndexMap;
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Initialize and return a shared `ComponentRegistry`
fn setup_registry() -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    registry.register::<adder::Adder>("Adder");
    registry.register::<string_length_counter::StringLengthCounter>("StringLengthCounter");
    registry.register::<wildcard_processor::WildcardProcessor>("WildcardProcessor");
    registry.register::<flexible_wildcard_processor::FlexibleWildcardProcessor>(
        "FlexibleWildcardProcessor",
    );
    registry.register::<long_running_task::LongRunningTask>("LongRunningTask");
    registry.register::<channel_consumer::ChannelConsumer>("ChannelConsumer");
    registry.register::<multi_channel_consumer::MultiChannelConsumer>("MultiChannelConsumer");
    registry.register::<crash_test_dummy::CrashTestDummy>("CrashTestDummy");
    registry
}

#[tokio::test]
async fn test_dag_execution() {
    let json_config = json!([
        {
            "id": "string_counter_1",
            "component_type": "StringLengthCounter",
            "config": {},
            "depends_on": [],
            "inputs": "Hello, world!"
        },
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": ["string_counter_1"]
        },
        {
            "id": "adder_2",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": ["adder_1"]
        },
        {
            "id": "adder_3",
            "component_type": "Adder",
            "config": { "value": 15 },
            "depends_on": ["adder_1", "adder_2"]
        },
        {
            "id": "wildcard_1",
            "component_type": "WildcardProcessor",
            "config": {
                "expected_input_keys": ["key1", "key2"],
                "expected_output_keys": ["key2", "key3"]
            },
            "depends_on": [],
            "inputs": { "key1": "value1", "key2": 42 }
        },
        {
            "id": "wildcard_2",
            "component_type": "WildcardProcessor",
            "config": {
                "expected_input_keys": ["key2", "key3"],
                "expected_output_keys": ["key1"]
            },
            "depends_on": ["wildcard_1"]
        },
        {
            "id": "flexible_wildcard_1",
            "component_type": "FlexibleWildcardProcessor",
            "config": {},
            "depends_on": ["adder_3"],
            "inputs": { "key1": "value1", "key2": 42 }
        },
        {
            "id": "long_task",
            "component_type": "LongRunningTask",
            "config": {},
            "depends_on": []
        },
        {
            "id": "consumer",
            "component_type": "ChannelConsumer",
            "config": {},
            "depends_on": ["long_task"]
        },
        {
            "id": "crash_dummy_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 20
            },
            "depends_on": ["consumer"]
        }
    ]);

    let registry = setup_registry();

    let dag_ir = DAGIR::from_json(json_config).expect("Failed to parse valid JSON config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    let mut expected_outputs: IndexMap<String, Data> = IndexMap::new();
    expected_outputs.insert(
        "flexible_wildcard_1".to_string(),
        Data::Json(json!({
            "type": "integer",
            "value": 61
        })),
    );
    expected_outputs.insert("string_counter_1".to_string(), Data::Integer(13));
    expected_outputs.insert(
        "wildcard_1".to_string(),
        Data::Json(json!({
            "key2": 42,
            "key3": null
        })),
    );
    expected_outputs.insert(
        "long_task".to_string(),
        Data::OneConsumerChannel(Arc::new(Mutex::new(None))),
    );
    expected_outputs.insert(
        "wildcard_2".to_string(),
        Data::Json(json!({
            "key1": null
        })),
    );
    expected_outputs.insert("adder_1".to_string(), Data::Integer(18));
    expected_outputs.insert("adder_2".to_string(), Data::Integer(28));
    expected_outputs.insert("adder_3".to_string(), Data::Integer(61));
    expected_outputs.insert(
        "crash_dummy_1".to_string(),
        Data::Text("Success!".to_string()),
    );
    expected_outputs.insert("consumer".to_string(), Data::Integer(42));

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => match dag.execute().await {
            Ok(results) => {
                let results_vec: Vec<_> = results.into_iter().collect();

                let last_two: Vec<_> = results_vec
                    .iter()
                    .rev()
                    .take(2)
                    .map(|(k, _)| k.as_str())
                    .collect();

                assert_eq!(
                    last_two,
                    vec!["crash_dummy_1", "consumer"],
                    "crash_dummy_1 should be last and consumer should be second-to-last"
                );

                for (key, actual_value) in &results_vec {
                    let expected_value =
                        expected_outputs.get(key).expect("Missing expected output");
                    match (actual_value, expected_value) {
                        (Data::OneConsumerChannel(_), Data::OneConsumerChannel(_)) => {
                            continue;
                        }
                        _ => assert_eq!(actual_value, expected_value, "Mismatch for key {}", key),
                    }
                }
            }
            Err(err) => panic!("Execution error: {}", err),
        },
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_execution_with_errors() {
    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "crash_dummy_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": true,
                "sleep_duration_ms": 20
            },
            "depends_on": ["adder_1"]
        }
    ]);

    let registry = setup_registry();

    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            if let Err(err) = dag.execute().await {
                println!("Execution error: {}", err);
                assert!(err.to_string().contains("Simulated failure as configured"));
            } else {
                panic!("Execution should have failed");
            }
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_execution_with_timeout() {
    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "crash_dummy_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 200
            },
            "depends_on": ["adder_1"]
        }
    ]);

    let registry = setup_registry();

    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            if let Err(err) = dag.execute().await {
                println!("Execution error: {}", err);
                assert!(err.to_string().contains("Execution timed out after 100ms"));
            } else {
                panic!("Execution should have failed");
            }
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_deferred_execution() {
    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "long_task",
            "component_type": "LongRunningTask",
            "config": {},
            "depends_on": []
        },
        {
            "id": "consumer",
            "component_type": "ChannelConsumer",
            "config": {},
            "depends_on": ["long_task"]
        }
    ]);

    let registry = setup_registry();

    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            if let Err(err) = dag.execute().await {
                panic!("Execution error: {}", err);
            }
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_cycle_detection() {
    let json_config = json!([
        {
            "id": "node_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": ["node_2"],
            "inputs": 42
        },
        {
            "id": "node_2",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": ["node_3"]
        },
        {
            "id": "node_3",
            "component_type": "Adder",
            "config": { "value": 15 },
            "depends_on": ["node_1"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => match dag.execute().await {
            Ok(_) => panic!("Expected cycle detection error"),
            Err(err) => {
                assert!(
                    err.to_string().contains("Cycle"),
                    "Error should mention cycle, got: {}",
                    err
                );
            }
        },
        Err(err) => {
            assert!(
                err.to_string().contains("Cycle"),
                "Error should mention cycle, got: {}",
                err
            );
        }
    }
}

#[tokio::test]
async fn test_dag_empty_graph() {
    let json_config = json!([]);
    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            let result = dag.execute().await;
            assert!(result.is_ok(), "Empty DAG should execute successfully");
            assert!(
                result.unwrap().is_empty(),
                "Empty DAG should return empty results"
            );
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_invalid_component_type() {
    let json_config = json!([{
        "id": "invalid_node",
        "component_type": "NonExistentComponent",
        "config": {},
        "depends_on": []
    }]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    if let Ok(_) = DAG::from_ir(dag_ir, &registry, dag_config) {
        panic!("Expected error for invalid component type");
    }
}

#[tokio::test]
async fn test_dag_invalid_dependency() {
    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": ["non_existent_node"],
            "inputs": 42
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    if let Ok(_) = DAG::from_ir(dag_ir, &registry, dag_config) {
        panic!("Expected error for invalid dependency");
    }
}

#[tokio::test]
async fn test_dag_duplicate_node_ids() {
    let json_config = json!([
        {
            "id": "node_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "node_1",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": []
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(100),
    };

    if let Ok(_) = DAG::from_ir(dag_ir, &registry, dag_config) {
        panic!("Expected error for duplicate node IDs");
    }
}

#[tokio::test]
async fn test_dag_parallel_execution() {
    let json_config = json!([
        {
            "id": "long_task_1",
            "component_type": "LongRunningTask",
            "config": {},
            "depends_on": []
        },
        {
            "id": "long_task_2",
            "component_type": "LongRunningTask",
            "config": {},
            "depends_on": []
        },
        {
            "id": "consumer",
            "component_type": "MultiChannelConsumer",
            "config": {},
            "depends_on": ["long_task_1", "long_task_2"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(200),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            let start = std::time::Instant::now();
            let result = dag.execute().await;
            let duration = start.elapsed();
            println!("Result: {:?}", result);

            assert!(result.is_ok(), "DAG execution failed");

            assert!(
                duration.as_millis() < 300,
                "Tasks did not execute in parallel"
            );
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_invalid_json_config() {
    let invalid_configs = vec![
        json!([{
            "id": "node_1",

            "config": {}
        }]),
        json!([{
            "id": 123,
            "component_type": "Adder",
            "config": {}
        }]),
        json!([{
            "id": "",
            "component_type": "Adder",
            "config": {}
        }]),
    ];

    for config in invalid_configs {
        match DAGIR::from_json(config) {
            Ok(_) => panic!("Expected JSON parsing to fail"),
            Err(err) => {
                println!("Got expected error: {}", err);
                assert!(!err.is_empty(), "Error message should not be empty");
            }
        }
    }
}

#[tokio::test]
async fn test_dag_large_parallel_execution() {
    let mut config_array = Vec::new();
    let num_parallel_nodes = 50;

    for i in 0..num_parallel_nodes {
        config_array.push(json!({
            "id": format!("adder_{}", i),
            "component_type": "Adder",
            "config": { "value": i },
            "depends_on": [],
            "inputs": 1
        }));
    }

    let mut depends_on = Vec::new();
    for i in 0..num_parallel_nodes {
        depends_on.push(format!("adder_{}", i));
    }

    config_array.push(json!({
        "id": "final_node",
        "component_type": "Adder",
        "config": { "value": 0 },
        "depends_on": depends_on
    }));

    let json_config = Value::Array(config_array);
    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(1000),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            let start = std::time::Instant::now();
            let result = dag.execute().await;
            let duration = start.elapsed();

            assert!(result.is_ok(), "Large parallel DAG execution failed");

            assert!(
                duration.as_millis() < 500,
                "Large parallel DAG took too long to execute: {:?}ms",
                duration.as_millis()
            );
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}

#[tokio::test]
async fn test_dag_cleanup_on_failure() {
    let json_config = json!([
        {
            "id": "long_task",
            "component_type": "LongRunningTask",
            "config": {},
            "depends_on": []
        },
        {
            "id": "crash_dummy",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": true,
                "sleep_duration_ms": 50
            },
            "depends_on": ["long_task"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(json_config).expect("Valid config");
    let dag_config = DAGConfig {
        per_node_timeout_ms: Some(200),
    };

    match DAG::from_ir(dag_ir, &registry, dag_config) {
        Ok(dag) => {
            let result = dag.execute().await;
            assert!(result.is_err(), "Expected execution to fail");
        }
        Err(err) => panic!("DAG construction error: {}", err),
    }
}
