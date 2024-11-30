use crate::component::{Component, Data, DataType, Error};
use crate::dag::DAGError;
use indexmap::IndexMap;
use jq_rs::compile;
use serde_json::Value;
use std::cell::RefCell;
use std::process::Command;
use std::time::Instant;

/// The maximum number of programs to keep in the per-thread cache.
const MAX_PROGRAMS_PER_THREAD: usize = 100;
/// The timeout for validating a program.
const VALIDATION_TIMEOUT: f32 = 0.1;

/// ## What is a Payload Transformer?
///
/// A component that transforms the payload of a JSON object using a JQ expression.
/// This component is intended for "gluing" together two Components that can do JSON
/// input/output but don't yet share a common concrete Rust type.
///
/// The intention is not to use this method in "Production" 24/7/365 workloads but instead allow
/// you to quickly experiment without needing to do deployments just to compose two Components.
///
/// ### An example use case
///
/// Say you have a Component that emits nested JSON records and "sub-records," and another Component
/// that requires a flat list of the "sub-records." You can use a `PayloadTransformer` to glue them
/// together by flattening the nested records in between.
///
/// This requires both Components to operate on JSON: your payload source must have `Data::Json`
/// as a supported output type, and your payload destination must have `Data::Json` as a supported
/// input type.
///
/// ### Your workflow
///
/// It is expected that you write your JQ program on your local machine, testing it on sample input/output
/// data until it works correctly. Once you're confident it works, then you execute it in a DAG using
/// the `PayloadTransformer` Component.
///
/// You should be able to paste the expression into the `transformation_expression` field and paste the
/// validation data into the `validation_data` field. (See `tests/resources` for many examples.)
///
/// ### Safety
///
/// We compile and run the JQ programs **directly in the Rust process**.
///
/// JQ has no way to access network or filesystem, but it is Turing complete and therefore possible
/// to write an infinite loop. To prevent this, we require **validation data** and run the JQ program with
/// a timeout once before storing it. We need to run validation as a separate process because killing
/// things with proper cleanup across the FFI boundary is difficult.
///
/// ### Feature Engineering
///
/// Because JQ is a very expressive transformation language, you can also do some basic "feature
/// engineering" like one-hot encoding, calculating basic statistics, and so on. If you're changing
/// data this way you're not doing payload transformation so much as actual ETL. That really belongs
/// in with your remote endpoint Component's host code or as a new local Component in pure Rust.
pub struct PayloadTransformer {
    expression: String,
    max_programs_per_thread: usize,
}

thread_local! {
    static COMPILED_PROGRAMS: RefCell<IndexMap<String, jq_rs::JqProgram>> = RefCell::new(IndexMap::new());
}

impl PayloadTransformer {
    /// Validates a JQ program with test data to ensure:
    /// 1. It doesn't contain infinite loops (using timeout)
    /// 2. It produces the expected output structure
    /// 3. The output values match expectations (unless `structure_only` is true)
    fn validate_expression(expression: &str, validation_data: &Value) -> Result<(), String> {
        let validation_input = validation_data.get("input")
            .ok_or("validation_data.input is required")?;
        let expected_output = validation_data.get("expected_output")
            .ok_or("validation_data.expected_output is required")?;
        let structure_only = validation_data.get("structure_only")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let validation_json = serde_json::to_string(validation_input)
            .map_err(|e| format!("Failed to serialize validation input: {e}"))?;

        println!("Validating expression: {expression}");
        let mut child = Command::new("gtimeout")
            .arg(VALIDATION_TIMEOUT.to_string())
            .arg("jq")
            .arg(expression)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .or_else(|_| {
                Command::new("timeout")
                    .arg(VALIDATION_TIMEOUT.to_string())
                    .arg("jq")
                    .arg(expression)
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .spawn()
            })
            .map_err(|e| format!("Failed to spawn command: {e}"))?;

        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin.write_all(validation_json.as_bytes())
                .map_err(|e| format!("Failed to write to stdin: {e}"))?;
            drop(stdin);
        }

        let output = child.wait_with_output()
            .map_err(|e| format!("Failed to read command output: {e}"))?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            let timeout_error = output.status.code() == Some(124);
            return Err(format!("JQ program validation failed: {}",
                if timeout_error {
                    "Program timed out (possible infinite loop)"
                } else {
                    &error
                }
            ));
        }

        let actual_output: Value = serde_json::from_str(&String::from_utf8_lossy(&output.stdout))
            .map_err(|e| format!("Failed to parse JQ output as JSON: {e}"))?;

        if structure_only {
            Self::validate_structure(expected_output, &actual_output)?;
        } else if actual_output != *expected_output {
            return Err(format!(
                "Validation failed: output doesn't match.\nExpected: {expected_output}\nActual: {actual_output}"
            ));
        }

        println!("Validation passed");
        Ok(())
    }

    /// Helper function to validate that two values have the same structure
    fn validate_structure(expected: &Value, actual: &Value) -> Result<(), String> {
        // Check that the types match, handling all JSON value types
        if actual.is_array() != expected.is_array()
            || actual.is_object() != expected.is_object()
            || actual.is_string() != expected.is_string()
            || actual.is_number() != expected.is_number()
            || actual.is_boolean() != expected.is_boolean()
            || actual.is_null() != expected.is_null()
        {
            return Err(format!(
                "Wrong type.\nExpected type: {}\nActual type: {}",
                if expected.is_array() { "array" }
                else if expected.is_object() { "object" }
                else if expected.is_string() { "string" }
                else if expected.is_number() { "number" }
                else if expected.is_boolean() { "boolean" }
                else if expected.is_null() { "null" }
                else { "unknown" },
                if actual.is_array() { "array" }
                else if actual.is_object() { "object" }
                else if actual.is_string() { "string" }
                else if actual.is_number() { "number" }
                else if actual.is_boolean() { "boolean" }
                else if actual.is_null() { "null" }
                else { "unknown" }
            ));
        }
        Ok(())
    }

    /// Executes a JQ program on the given input, handling program compilation caching
    /// and thread-local storage. Returns the JSON output as a string.
    fn execute_jq_program(&self, input_str: &str) -> Result<String, String> {
        let start = Instant::now();
        COMPILED_PROGRAMS.with(|programs| {
            let mut programs = programs.borrow_mut();
            if !programs.contains_key(&self.expression) {
                // If we're at capacity, remove the oldest entry
                if programs.len() >= self.max_programs_per_thread {
                    let start_eviction = Instant::now();
                    println!(
                        "Thread {:?}: Removing oldest JQ program",
                        std::thread::current().id()
                    );
                    programs.shift_remove_index(0); // Removes and returns the first (oldest) entry
                    println!(
                        "Thread {:?}: Removed oldest JQ program in {:?}",
                        std::thread::current().id(),
                        start_eviction.elapsed()
                    );
                }

                let start_compilation = Instant::now();
                let program = match compile(&self.expression) {
                    Ok(p) => p,
                    Err(e) => return Err(format!("Failed to compile JQ program: {e}")),
                };
                programs.insert(self.expression.clone(), program);
                println!(
                    "Thread {:?}: Compiled new JQ program in {:?}",
                    std::thread::current().id(),
                    start_compilation.elapsed()
                );
            }
            Ok(())
        })?;

        println!(
            "Thread {:?}: Program access took {:?}",
            std::thread::current().id(),
            start.elapsed()
        );

        let output_str = COMPILED_PROGRAMS
            .with(|programs| {
                let mut programs = programs.borrow_mut();
                let program = programs.get_mut(&self.expression).unwrap();
                program.run(input_str)
            })
            .map_err(|e| format!("Failed to execute jq: {e}"))?;

        let start = Instant::now();
        println!(
            "Thread {:?}: JQ execution took {:?}",
            std::thread::current().id(),
            start.elapsed()
        );

        Ok(output_str)
    }
}

impl Component for PayloadTransformer {
    /// The FFI boundary for libjq means we don't have `Send` or `Sync`, so to keep pre-compiled programs
    /// (since compilation is expensive) we use a `thread_local` to keep them in. We bound this to
    /// `MAX_PROGRAMS_PER_THREAD` to limit memory usage. When a new program is compiled, it either
    /// goes in the per-thread cache, or evicts the oldest program in the cache.
    ///
    /// Additionally, jq is Turing complete, and so it is possible for someone to write an infinite loop.
    /// To validate that the program is not infinite, we run it with a timeout once before compiling.
    /// We need to run validation as a separate process because you can't kill the thread and clean up the
    /// C resources that were allocated. Validation cost is paid only once: when configuration happens.
    fn configure(config: Value) -> Result<Self, Error> {
        println!("PayloadTransformer config: {config:?}");

        let expression = config["transformation_expression"]
            .as_str()
            .map_or_else(|| ".".to_string(), String::from);

        let max_programs_per_thread = config["max_programs_per_thread"]
            .as_u64()
            .unwrap_or(MAX_PROGRAMS_PER_THREAD as u64)
            .try_into()
            .map_err(|e| Error::ConfigurationError(format!("Invalid max_programs_per_thread: {e}")))?;

        let validation_data = config.get("validation_data")
            .ok_or_else(|| Error::ConfigurationError("validation_data is required".to_string()))?;

        Self::validate_expression(&expression, validation_data)
            .map_err(|e| Error::ConfigurationError(e.to_string()))?;

        Ok(PayloadTransformer {
            expression,
            max_programs_per_thread,
        })
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("PayloadTransformer input: {input:?}");

        match input {
            Data::Json(value) => {
                let input_str = serde_json::to_string(&value)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason: format!("Failed to serialize input: {err}"),
                    })?;

                let output_str = self.execute_jq_program(&input_str)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason: err,
                    })?;

                let output_json: Value = serde_json::from_str(&output_str)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason: format!("Failed to parse jq output: {err}"),
                    })?;

                Ok(Data::Json(output_json))
            }
            _ => Err(DAGError::ExecutionError {
                node_id: "unknown".to_string(),
                reason: "Invalid input type, expected JSON".to_string(),
            }),
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Json
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}
