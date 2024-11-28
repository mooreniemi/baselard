use baselard::component::ComponentRegistry;
use baselard::component::Data;
use baselard::components::payload_transformer::PayloadTransformer;
use baselard::dag::{DAGConfig, DAG, DAGIR};
use serde_json::json;

fn setup_test_registry() -> ComponentRegistry {
    let mut registry = ComponentRegistry::new();
    registry.register::<PayloadTransformer>("PayloadTransformer");
    registry
}

#[tokio::test]
async fn test_basic_transformation() {
    let registry = setup_test_registry();
    let json_config = json!([{
        "id": "transform1",
        "component_type": "PayloadTransformer",
        "config": {
            "transformation_expression": ".message"
        },
        "inputs": {
            "message": "Hello, World!",
            "extra": "ignored"
        }
    }]);

    let dag = DAGIR::from_json(json_config)
        .and_then(|ir| DAG::from_ir(ir, &registry, DAGConfig::default(), None))
        .expect("Valid DAG");

    let results = dag.execute(None).await.expect("Execution success");
    assert_eq!(
        results.get("transform1"),
        Some(&Data::Json(json!("Hello, World!")))
    );
}

#[ignore]
#[tokio::test]
async fn test_invalid_jq_expression() {
    let registry = setup_test_registry();
    let json_config = json!([{
        "id": "transform1",
        "component_type": "PayloadTransformer",
        "config": {
            "transformation_expression": "invalid[expression"
        },
        "inputs": {"test": "data"}
    }]);

    let result = DAGIR::from_json(json_config)
        .and_then(|ir| DAG::from_ir(ir, &registry, DAGConfig::default(), None));
    
    assert!(result.is_err(), "Invalid JQ expression should fail at configuration");
    assert!(
        matches!(
            result,
            Err(e) if e.to_string().contains("InvalidProgram")
        ),
        "Error should mention invalid JQ program"
    );
}

#[tokio::test]
async fn test_chained_transformations() {
    let registry = setup_test_registry();
    let json_config = json!([
        {
            "id": "transform1",
            "component_type": "PayloadTransformer",
            "config": {
                "transformation_expression": "{message: .message, count: (.count + 1)}"
            },
            "inputs": {
                "message": "Hello",
                "count": 0
            }
        },
        {
            "id": "transform2",
            "component_type": "PayloadTransformer",
            "config": {
                "transformation_expression": ".message + \" World!\""
            },
            "depends_on": ["transform1"]
        }
    ]);

    let dag = DAGIR::from_json(json_config)
        .and_then(|ir| DAG::from_ir(ir, &registry, DAGConfig::default(), None))
        .expect("Valid DAG");

    let results = dag.execute(None).await.expect("Execution success");
    
    assert_eq!(
        results.get("transform1"),
        Some(&Data::Json(json!({"message": "Hello", "count": 1})))
    );
    
    assert_eq!(
        results.get("transform2"),
        Some(&Data::Json(json!("Hello World!")))
    );
}

#[tokio::test]
async fn test_non_json_input() {
    let registry = setup_test_registry();
    let json_config = json!([{
        "id": "transform1",
        "component_type": "PayloadTransformer",
        "config": {
            "transformation_expression": "."
        },
        "inputs": 42  // Integer instead of JSON
    }]);

    let result = DAGIR::from_json(json_config)
        .and_then(|ir| DAG::from_ir(ir, &registry, DAGConfig::default(), None));

    assert!(result.is_err(), "Non-JSON input should fail DAG validation");
    assert!(
        matches!(
            result,
            Err(e) if e.to_string().contains("Expected Json, got Integer")
        ),
        "Error should mention type mismatch"
    );
}

#[tokio::test]
async fn test_default_identity_transform() {
    let registry = setup_test_registry();
    let json_config = json!([{
        "id": "transform1",
        "component_type": "PayloadTransformer",
        "config": {},  // No expression provided, should default to "."
        "inputs": {"test": "data"}
    }]);

    let dag = DAGIR::from_json(json_config)
        .and_then(|ir| DAG::from_ir(ir, &registry, DAGConfig::default(), None))
        .expect("Valid DAG");

    let results = dag.execute(None).await.expect("Execution success");
    assert_eq!(
        results.get("transform1"),
        Some(&Data::Json(json!({"test": "data"})))
    );
}