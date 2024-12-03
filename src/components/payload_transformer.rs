use crate::component::{Component, Data, DataType, Error};
use crate::dag::{DAGError, NodeExecutionContext};
use indexmap::IndexMap;
use jq_rs::compile;
use serde_json::Value;
use tracing::debug;
use std::cell::RefCell;
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use dashmap::DashSet;
use std::io::Read;

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

lazy_static::lazy_static! {
    static ref INVALID_EXPRESSIONS: DashSet<u64> = DashSet::new();
}

impl PayloadTransformer {
    fn get_expression_hash(expression: &str, validation_data: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        expression.hash(&mut hasher);
        validation_data.to_string().hash(&mut hasher);
        hasher.finish()
    }

    /// Validates a JQ program with test data to ensure:
    /// 1. It doesn't contain infinite loops (using timeout)
    /// 2. It produces the expected output structure
    /// 3. The output values match expectations (unless `structure_only` is true)
    fn validate_expression(expression: &str, validation_data: &Value) -> Result<(), String> {
        let start = Instant::now();
        let program_hash = Self::get_expression_hash(expression, validation_data);

        if INVALID_EXPRESSIONS.contains(&program_hash) {
            return Err("Program previously failed validation".to_string());
        }

        let (validation_input, expected_output, structure_only) = Self::parse_validation_data(validation_data)?;
        let validation_json = Self::serialize_validation_input(validation_input)?;

        debug!("Validating expression: {expression}");

        // Run validation in a separate process so we can kill it if it hangs
        let mut child = Self::spawn_validation_process(expression)?;
        Self::write_validation_input(&mut child, &validation_json)?;
        Self::wait_for_process_completion(&mut child, program_hash)?;

        // Now we pass ownership of the child process
        let actual_output = Self::get_process_output(child, program_hash)?;
        Self::validate_output(&actual_output, expected_output, structure_only, program_hash)?;

        // Only compile and cache after successful validation
        COMPILED_PROGRAMS.with(|programs| {
            debug!("Thread {:?}: Compiling JQ program because validation passed", std::thread::current().id());
            let mut programs = programs.borrow_mut();
            if !programs.contains_key(expression) {
                if programs.len() >= MAX_PROGRAMS_PER_THREAD {
                    debug!("Thread {:?}: Removing oldest JQ program because we hit the limit", std::thread::current().id());
                    programs.shift_remove_index(0);
                }
                match compile(expression) {
                    Ok(program) => {
                        programs.insert(expression.to_string(), program);
                    }
                    Err(e) => {
                        INVALID_EXPRESSIONS.insert(program_hash);
                        return Err(format!("Failed to compile JQ program: {e}"));
                    }
                }
            }
            Ok(())
        })?;

        debug!("Validation completed in {:?}", start.elapsed());
        Ok(())
    }

    fn parse_validation_data(validation_data: &Value) -> Result<(&Value, &Value, bool), String> {
        let validation_input = validation_data.get("input")
            .ok_or("validation_data.input is required")?;
        let expected_output = validation_data.get("expected_output")
            .ok_or("validation_data.expected_output is required")?;
        let structure_only = validation_data.get("structure_only")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        Ok((validation_input, expected_output, structure_only))
    }

    fn serialize_validation_input(validation_input: &Value) -> Result<String, String> {
        serde_json::to_string(validation_input)
            .map_err(|e| format!("Failed to serialize validation input: {e}"))
    }

    fn spawn_validation_process(expression: &str) -> Result<Child, String> {
        Command::new("gtimeout")
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
            .map_err(|e| format!("Failed to spawn command: {e}"))
    }

    fn write_validation_input(child: &mut Child, validation_json: &str) -> Result<(), String> {
        if let Some(mut stdin) = child.stdin.take() {
            let validation_json = validation_json.to_owned();
            let write_handle = std::thread::spawn(move || {
                use std::io::Write;
                stdin.write_all(validation_json.as_bytes())
            });

            let start = Instant::now();
            let timeout = Duration::from_secs_f32(VALIDATION_TIMEOUT);

            while start.elapsed() < timeout {
                if write_handle.is_finished() {
                    return match write_handle.join() {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(format!("Failed to write to stdin: {e}")),
                        Err(_) => Err("Write thread panicked".to_string()),
                    };
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            Err("Timeout while writing to stdin".to_string())
        } else {
            Ok(())
        }
    }

    fn wait_for_process_completion(child: &mut Child, program_hash: u64) -> Result<(), String> {
        let timeout = Duration::from_secs_f32(VALIDATION_TIMEOUT + 0.5);
        let start_wait = Instant::now();

        while start_wait.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(status)) => {
                    debug!("Process completed in {:?}", start_wait.elapsed());
                    return Self::handle_process_status(child, status, program_hash);
                }
                Ok(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(format!("Error waiting for process: {e}")),
            }
        }

        let _ = child.kill();
        Err("Process timed out".to_string())
    }

    fn handle_process_status(child: &mut Child, status: ExitStatus, program_hash: u64) -> Result<(), String> {
        if !status.success() {
            let error = if let Some(mut stderr) = child.stderr.take() {
                let mut error_str = String::new();
                stderr.read_to_string(&mut error_str)
                    .map_err(|e| format!("Failed to read stderr: {e}"))?;
                error_str
            } else {
                "No error output available".to_string()
            };

            let timeout_error = status.code() == Some(124);
            INVALID_EXPRESSIONS.insert(program_hash);
            return Err(format!("JQ program validation failed: {}",
                if timeout_error {
                    "Program timed out (possible infinite loop)"
                } else {
                    &error
                }
            ));
        }
        Ok(())
    }

    fn get_process_output(child: Child, program_hash: u64) -> Result<Value, String> {
        let output = child.wait_with_output()
            .map_err(|e| format!("Failed to read command output: {e}"))?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            let timeout_error = output.status.code() == Some(124);
            INVALID_EXPRESSIONS.insert(program_hash);
            return Err(format!("JQ program validation failed: {}",
                if timeout_error {
                    "Program timed out (possible infinite loop)"
                } else {
                    &error
                }
            ));
        }

        serde_json::from_str(&String::from_utf8_lossy(&output.stdout))
            .map_err(|e| {
                INVALID_EXPRESSIONS.insert(program_hash);
                format!("Failed to parse JQ output as JSON: {e}")
            })
    }

    fn validate_output(
        actual_output: &Value,
        expected_output: &Value,
        structure_only: bool,
        program_hash: u64,
    ) -> Result<(), String> {
        if structure_only {
            Self::validate_structure(expected_output, actual_output)
        } else if actual_output != expected_output {
            INVALID_EXPRESSIONS.insert(program_hash);
            Err(format!(
                "Validation failed: output doesn't match.\nExpected: {expected_output}\nActual: {actual_output}"
            ))
        } else {
            Ok(())
        }
    }

    /// Helper function to validate that two values have the same structure
    fn validate_structure(expected: &Value, actual: &Value) -> Result<(), String> {
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
    ///
    /// Compiled programs are cached and may be evicted in this block.
    ///
    /// This block locks.
    fn execute_jq_program(&self, input_str: &str) -> Result<String, String> {
        let start = Instant::now();

        let output_str = COMPILED_PROGRAMS.with(|programs| {
            let mut programs = programs.borrow_mut();
            if !programs.contains_key(&self.expression) {
                if programs.len() >= self.max_programs_per_thread {
                    let start_eviction = Instant::now();
                    debug!(
                        "Thread {:?}: Removing oldest JQ program",
                        std::thread::current().id()
                    );
                    programs.shift_remove_index(0);
                    debug!(
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
                debug!(
                    "Thread {:?}: Compiled new JQ program in {:?}",
                    std::thread::current().id(),
                    start_compilation.elapsed()
                );
            }

            // Execute the program while holding the lock
            let program = programs.get_mut(&self.expression)
                .ok_or_else(|| "Program not found after insertion".to_string())?;
            program.run(input_str)
                .map_err(|e| format!("Failed to execute jq: {e}"))
        })?;

        let execution_time = start.elapsed();
        debug!(
            "Thread {:?}: JQ execution took {:?}",
            std::thread::current().id(),
            execution_time
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
        debug!("PayloadTransformer config: {config:?}");

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

    fn execute(&self, context: NodeExecutionContext, input: Data) -> Result<Data, DAGError> {
        debug!("PayloadTransformer {}: input={input:?}", context.node_id);
        let node_id = context.node_id;

        match input {
            Data::Json(value) => {
                // FIXME: node_id should be known to the instance ultimately
                let input_str = serde_json::to_string(&value)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: node_id.clone(),
                        reason: format!("Failed to serialize input: {err}"),
                    })?;

                let output_str = self.execute_jq_program(&input_str)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: node_id.clone(),
                        reason: err,
                    })?;

                let output_json: Value = serde_json::from_str(&output_str)
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: node_id.clone(),
                        reason: format!("Failed to parse jq output: {err}"),
                    })?;

                Ok(Data::Json(output_json))
            }
            _ => Err(DAGError::ExecutionError {
                node_id: node_id.clone(),
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
