#[allow(clippy::module_inception)]
mod tests {
    use crate::component::Registry;
    use crate::components::*;
    use crate::dag::DAGSettings;
    use crate::dag::Data;
    use crate::dag::DAG;
    use crate::dagir::DAGIR;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_concurrent_dag_execution() {
        let mut registry = Registry::new();
        registry.register::<adder::Adder>("Adder");

        let json_config = json!({
            "alias": "concurrent_test",
            "nodes": [
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
            ]
        });

        let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
        let dag_config = DAGSettings::cache_off();
        let dag = Arc::new(DAG::from_ir(&dag_ir, &registry, dag_config, None).expect("Valid DAG"));

        let num_iterations = 100;
        let num_concurrent: usize = 5;

        for iteration in 0..num_iterations {
            println!("Starting iteration {iteration}");
            let mut join_set = JoinSet::new();

            for i in 0..num_concurrent {
                let dag = Arc::clone(&dag);
                let request_id = format!("iteration-{iteration}-concurrent-{i}");

                join_set.spawn(async move { dag.execute(Some(request_id)).await });
            }

            let mut results = Vec::new();
            while let Some(result) = join_set.join_next().await {
                let execution_result = result
                    .expect("Task panicked")
                    .expect("DAG execution failed");
                results.push(execution_result);
            }

            assert_eq!(
                results.len(),
                num_concurrent,
                "All executions should complete in iteration {iteration}"
            );

            let first_result = &results[0];
            for (idx, result) in results[1..].iter().enumerate() {
                assert_eq!(
                    result, first_result,
                    "All concurrent executions should produce identical results (iteration {iteration}, result {idx})"
                );

                assert_eq!(
                    result.get("adder_1"),
                    Some(&Data::Integer(47)),
                    "adder_1 should be 47 (iteration {iteration}, result {idx})"
                );
                assert_eq!(
                    result.get("adder_2"),
                    Some(&Data::Integer(57)),
                    "adder_2 should be 57 (iteration {iteration}, result {idx})"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_dag_execution_with_jq() {
        let mut registry = Registry::new();
        registry.register::<payload_transformer::PayloadTransformer>("PayloadTransformer");
        registry.register::<json_combiner::JsonCombiner>("JsonCombiner");

        let json_config = json!({
            "alias": "concurrent_jq_test",
            "nodes": [
                {
                    "id": "transform_1",
                    "component_type": "PayloadTransformer",
                    "config": {
                        "transformation_expression": ".input + 5",
                        "validation_data": {
                            "input": { "input": 10 },
                            "expected_output": 15
                        }
                    },
                    "depends_on": [],
                    "inputs": { "input": 42 }
                },
                {
                    "id": "transform_2",
                    "component_type": "PayloadTransformer",
                    "config": {
                        "transformation_expression": ".input * 2",
                        "validation_data": {
                            "input": { "input": 10 },
                            "expected_output": 20
                        }
                    },
                    "depends_on": [],
                    "inputs": { "input": 10 }
                },
                {
                    "id": "combiner",
                    "component_type": "JsonCombiner",
                    "config": {},
                    "depends_on": ["transform_1", "transform_2"]
                }
            ]
        });

        let dag_ir = DAGIR::from_json(&json_config).expect("Valid config");
        let dag_config = DAGSettings::cache_off();
        let dag = Arc::new(DAG::from_ir(&dag_ir, &registry, dag_config, None).expect("Valid DAG"));

        let num_iterations = 100;
        let num_concurrent: usize = 5;

        for iteration in 0..num_iterations {
            println!("Starting iteration {iteration}");
            let mut join_set = JoinSet::new();

            for i in 0..num_concurrent {
                let dag = Arc::clone(&dag);
                let request_id = format!("iteration-{iteration}-concurrent-{i}");

                join_set.spawn(async move { dag.execute(Some(request_id)).await });
            }

            let mut results = Vec::new();
            while let Some(result) = join_set.join_next().await {
                let execution_result = result
                    .expect("Task panicked")
                    .expect("DAG execution failed");
                results.push(execution_result);
            }

            assert_eq!(
                results.len(),
                num_concurrent,
                "All executions should complete in iteration {iteration}"
            );

            let first_result = &results[0];
            for (idx, result) in results[1..].iter().enumerate() {
                assert_eq!(
                    result, first_result,
                    "All concurrent executions should produce identical results (iteration {iteration}, result {idx})"
                );

                assert_eq!(
                    result.get("transform_1"),
                    Some(&Data::Json(json!(47))),
                    "transform_1 should output 47 (iteration {iteration}, result {idx})"
                );

                assert_eq!(
                    result.get("transform_2"),
                    Some(&Data::Json(json!(20))),
                    "transform_2 should output 20 (iteration {iteration}, result {idx})"
                );

                assert_eq!(
                    result.get("combiner"),
                    Some(&Data::Json(json!({
                        "input_0": 47,
                        "input_1": 20
                    }))),
                    "combiner should merge both transforms (iteration {iteration}, result {idx})"
                );
            }
        }
    }
}
