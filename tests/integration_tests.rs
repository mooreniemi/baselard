use baselard::cache::Cache;
use baselard::component::Data;
use baselard::component::Registry;
use baselard::components::*;
use baselard::dag::DAGError;
use baselard::dag::{DAGConfig, DAG, DAGIR};
use indexmap::IndexMap;
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;

/// Initialize and return a shared `ComponentRegistry`
fn setup_registry() -> Registry {
    let mut registry = Registry::new();
    registry.register::<adder::Adder>("Adder");
    registry.register::<string_length_counter::StringLengthCounter>("StringLengthCounter");
    registry.register::<wildcard_processor::WildcardProcessor>("WildcardProcessor");
    registry.register::<data_to_json_processor::DataToJsonProcessor>(
        "FlexibleWildcardProcessor",
    );
    registry.register::<crash_test_dummy::CrashTestDummy>("CrashTestDummy");
    registry
}

#[tokio::test]
async fn test_optimal_makespan() {
    let json_config = json!([
        {
            "id": "fast_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 50
            },
            "depends_on": []
        },
        {
            "id": "fast_2",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 50
            },
            "depends_on": ["fast_1"]
        },
        {
            "id": "slow_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 100
            },
            "depends_on": []
        },
        {
            "id": "final",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 10
            },
            "depends_on": ["slow_1", "fast_2"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    let start = std::time::Instant::now();
    let dag = DAG::from_ir(&dag_ir, &registry, dag_config, None).expect("Valid DAG");
    let results = dag.execute(None).await.expect("Execution success");
    let duration = start.elapsed();
    assert!(
        duration.as_millis() < 125,
        "Execution should be ideal makespan of 110ms (and slight buffer), got {}",
        duration.as_millis()
    );
    assert_eq!(results.len(), 4);
}

#[tokio::test]
async fn test_optimal_failspan() {
    let json_config = json!([
        {
            "id": "fast_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 50
            },
            "depends_on": []
        },
        {
            "id": "fast_2",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": true,  // This will fail after 20ms
                "sleep_duration_ms": 20
            },
            "depends_on": ["fast_1"]
        },
        {
            "id": "slow_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 100  // This should get cancelled
            },
            "depends_on": []
        },
        {
            "id": "final",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 10
            },
            "depends_on": ["slow_1", "fast_2"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    let start = std::time::Instant::now();
    let dag = DAG::from_ir(&dag_ir, &registry, dag_config, None).expect("Valid DAG");
    let result = dag.execute(None).await;
    let duration = start.elapsed();

    assert!(result.is_err(), "Execution should fail");
    assert!(
        duration.as_millis() < 85,  // 50ms + 20ms + small buffer < 100ms slow_1
        "Execution should fail fast, got {}ms",
        duration.as_millis()
    );
}

#[tokio::test]
async fn test_simple_seq_adds_up() {
    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "adder_2",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": ["adder_1"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();
    let dag = DAG::from_ir(&dag_ir, &registry, dag_config, None).expect("Valid DAG");
    let results = dag.execute(None).await.expect("Execution success");
    assert_eq!(results.len(), 2);
    assert_eq!(
        results.get("adder_2").unwrap(),
        &Data::Integer(57),
        "adder_2 should have accumulated to 57"
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_complex_dag_execution() {
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
            "id": "crash_dummy_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 20
            },
            "depends_on": ["wildcard_2"]
        }
    ]);

    let registry = setup_registry();

    let dag_ir = DAGIR::from_json(&json_config).expect("Failed to parse valid JSON config");
    let dag_config = DAGConfig::cache_off();

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

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => match dag.execute(None).await {
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
                    vec!["crash_dummy_1", "adder_3"],
                    "crash_dummy_1 and adder_3 should be last"
                );

                for (key, actual_value) in &results_vec {
                    let expected_value =
                        expected_outputs.get(key).expect("Missing expected output");
                    assert_eq!(actual_value, expected_value, "Mismatch for key {key}");
                }
            }
            Err(err) => panic!("Execution error: {err}"),
        },
        Err(err) => panic!("DAG construction error: {err}"),
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

    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => {
            if let Err(err) = dag.execute(None).await {
                println!("Execution error: {err}");
                assert!(err.to_string().contains("Simulated failure as configured"));
            } else {
                panic!("Execution should have failed");
            }
        }
        Err(err) => panic!("DAG construction error: {err}"),
    }
}

#[tokio::test]
async fn test_dag_execution_with_timeout() {
    let dag_config = DAGConfig::cache_off();
    let timeout_ms = dag_config.per_node_timeout_ms;

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
                "sleep_duration_ms": timeout_ms.unwrap() + 100
            },
            "depends_on": ["adder_1"]
        }
    ]);

    let registry = setup_registry();

    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => {
            if let Err(err) = dag.execute(None).await {
                println!("Execution error: {err}");
                assert!(err.to_string().contains(&format!(
                    "Node execution timed out after {}",
                    timeout_ms.unwrap()
                )));
            } else {
                panic!("Execution should have failed");
            }
        }
        Err(err) => panic!("DAG construction error: {err}"),
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
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => match dag.execute(None).await {
            Ok(_) => panic!("Expected cycle detection error"),
            Err(err) => {
                assert!(
                    err.to_string().contains("Cycle"),
                    "Error should mention cycle, got: {err}"
                );
            }
        },
        Err(err) => {
            assert!(
                err.to_string().contains("Cycle"),
                "Error should mention cycle, got: {err}"
            );
        }
    }
}

#[tokio::test]
async fn test_dag_empty_graph() {
    let json_config = json!([]);
    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => {
            let result = dag.execute(None).await;
            assert!(result.is_ok(), "Empty DAG should execute successfully");
            assert!(
                result.unwrap().is_empty(),
                "Empty DAG should return empty results"
            );
        }
        Err(err) => panic!("DAG construction error: {err}"),
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
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    assert!(
        DAG::from_ir(&dag_ir, &registry, dag_config, None).is_err(),
        "Expected error for invalid component type"
    );
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
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    assert!(
        DAG::from_ir(&dag_ir, &registry, dag_config, None).is_err(),
        "Expected error for invalid dependency"
    );
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
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    assert!(
        DAG::from_ir(&dag_ir, &registry, dag_config, None).is_err(),
        "Expected error for duplicate node IDs"
    );
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
        match DAGIR::from_json(&config) {
            Ok(_) => panic!("Expected JSON parsing to fail"),
            Err(err) => {
                println!("Got expected error: {err}");
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
        depends_on.push(format!("adder_{i}"));
    }

    config_array.push(json!({
        "id": "final_node",
        "component_type": "Adder",
        "config": { "value": 0 },
        "depends_on": depends_on
    }));

    let json_config = Value::Array(config_array);
    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => {
            let start = std::time::Instant::now();
            let result = dag.execute(None).await;
            let duration = start.elapsed();

            assert!(result.is_ok(), "Large parallel DAG execution failed");

            assert!(
                duration.as_millis() < 500,
                "Large parallel DAG took too long to execute: {:?}ms",
                duration.as_millis()
            );
        }
        Err(err) => panic!("DAG construction error: {err}"),
    }
}

#[tokio::test]
async fn test_dag_cleanup_on_failure() {
    let json_config = json!([
        {
            "id": "crash_dummy_1",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": false,
                "sleep_duration_ms": 50
            },
            "depends_on": []
        },
        {
            "id": "crash_dummy",
            "component_type": "CrashTestDummy",
            "config": {
                "fail": true,
                "sleep_duration_ms": 50
            },
            "depends_on": ["crash_dummy_1"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();

    match DAG::from_ir(&dag_ir, &registry, dag_config, None) {
        Ok(dag) => {
            let result = dag.execute(None).await;
            assert!(result.is_err(), "Expected execution to fail");
        }
        Err(err) => panic!("DAG construction error: {err}"),
    }
}

#[tokio::test]
async fn test_dag_with_caching() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let history_file = temp_dir.path().join("dag_history.jsonl");
    let cache = Arc::new(Cache::new(
        Some(history_file.to_str().unwrap().to_string()),
        10_000,
    ));

    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "adder_2",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": ["adder_1"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::default();

    let dag =
        DAG::from_ir(&dag_ir, &registry, dag_config, Some(Arc::clone(&cache))).expect("Valid DAG");

    let request_id = "test-run-1".to_string();
    let results = dag
        .execute(Some(request_id.clone()))
        .await
        .expect("Execution success");

    assert_eq!(results.get("adder_1"), Some(&Data::Integer(47)));
    assert_eq!(results.get("adder_2"), Some(&Data::Integer(57)));

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let cached_results = dag
        .get_result_by_request_id(&request_id)
        .expect("Cached results exist");
    assert_eq!(results, cached_results.node_results);

    let node_result = dag.get_cached_node_result("adder_1");
    assert_eq!(node_result, Some(Data::Integer(47)));

    let file_contents = tokio::fs::read_to_string(history_file)
        .await
        .expect("Failed to read history file");
    println!("File contents: {file_contents}");

    assert!(file_contents.contains("\"request_id\":\"test-run-1\""));

    let parsed: serde_json::Value =
        serde_json::from_str(&file_contents).expect("File should contain valid JSON");

    assert_eq!(parsed["request_id"], "test-run-1");
    assert_eq!(parsed["node_results"]["adder_1"]["integer"], 47);
    assert_eq!(parsed["node_results"]["adder_2"]["integer"], 57);
}

#[tokio::test]
async fn test_dag_replay() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let history_file = temp_dir.path().join("replay_history.jsonl");
    let cache = Arc::new(Cache::new(Some(history_file), 10_000));

    let json_config = json!([
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": [],
            "inputs": 42
        },
        {
            "id": "adder_2",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": ["adder_1"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::default(); // This enables history

    let dag =
        DAG::from_ir(&dag_ir, &registry, dag_config, Some(Arc::clone(&cache))).expect("Valid DAG");

    // First execution
    let request_id = "replay-test-1".to_string();
    let original_results = dag
        .execute(Some(request_id.clone()))
        .await
        .expect("Execution success");

    // Small delay to ensure file is written
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Replay execution
    let replayed_results = dag.replay(&request_id).await.expect("Replay success");

    // Verify results match
    assert_eq!(original_results, replayed_results);
    assert_eq!(replayed_results.get("adder_1"), Some(&Data::Integer(47)));
    assert_eq!(replayed_results.get("adder_2"), Some(&Data::Integer(57)));

    // Test replay of non-existent request ID
    let err = dag.replay("non-existent-id").await.unwrap_err();
    assert!(matches!(err, DAGError::HistoricalResultNotFound { .. }));
}

#[tokio::test]
async fn test_dag_identical_components() {
    let json_config = json!([
        {
            "id": "first_adder",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": [],
            "inputs": 10
        },
        {
            "id": "second_adder",
            "component_type": "Adder",
            "config": { "value": 5 },  // Same configuration as first_adder
            "depends_on": ["first_adder"]
        },
        {
            "id": "third_adder",
            "component_type": "Adder",
            "config": { "value": 5 },  // Same configuration as others
            "depends_on": ["second_adder"]
        }
    ]);

    let registry = setup_registry();
    let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
    let dag_config = DAGConfig::cache_off();
    let dag = DAG::from_ir(&dag_ir, &registry, dag_config, None).expect("Valid DAG");

    let results = dag.execute(None).await.expect("Execution success");

    assert_eq!(results.get("first_adder"), Some(&Data::Integer(15)));   // 10 + 5
    assert_eq!(results.get("second_adder"), Some(&Data::Integer(20)));  // 15 + 5
    assert_eq!(results.get("third_adder"), Some(&Data::Integer(25)));   // 20 + 5
}
