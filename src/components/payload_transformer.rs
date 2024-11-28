use crate::component::{Component, Data, DataType};
use crate::dag::DAGError;
use indexmap::IndexMap;
use jq_rs::compile;
use serde_json::Value;
use std::cell::RefCell;
use std::time::Instant;

const MAX_PROGRAMS_PER_THREAD: usize = 100;

pub struct PayloadTransformer {
    expression: String,
    max_programs_per_thread: usize,
}

thread_local! {
    static COMPILED_PROGRAMS: RefCell<IndexMap<String, jq_rs::JqProgram>> = RefCell::new(IndexMap::new());
}

impl Component for PayloadTransformer {
    /// The FFI boundary for libjq means we don't have `Send` or `Sync`, so to keep pre-compiled programs
    /// (since compilation is expensive) we use a `thread_local` to keep them in. We bound this to
    /// `MAX_PROGRAMS_PER_THREAD` to limit memory usage. When a new program is compiled, it either
    /// goes in the per-thread cache, or evicts the oldest program in the cache.
    fn configure(config: Value) -> Self {
        let expression = config["transformation_expression"]
            .as_str()
            .unwrap_or(".")
            .to_string();
        let max_programs_per_thread = usize::try_from(
            config["max_programs_per_thread"]
                .as_u64()
                .unwrap_or(MAX_PROGRAMS_PER_THREAD as u64),
        )
        .expect("Failed to parse max_programs_per_thread");

        let start = Instant::now();
        compile(&expression).expect("Failed to compile JQ expression");
        println!(
            "Thread {:?}: JQ compilation took {:?}",
            std::thread::current().id(),
            start.elapsed()
        );

        PayloadTransformer {
            expression,
            max_programs_per_thread,
        }
    }

    fn execute(&self, input: Data) -> Result<Data, DAGError> {
        println!("PayloadTransformer input: {input:?}");

        match input {
            Data::Json(value) => {
                let input_str =
                    serde_json::to_string(&value).map_err(|err| DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason: format!("Failed to serialize input: {err}"),
                    })?;

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
                        let program = compile(&self.expression).unwrap();
                        programs.insert(self.expression.clone(), program);
                        println!(
                            "Thread {:?}: Compiled new JQ program in {:?}",
                            std::thread::current().id(),
                            start_compilation.elapsed()
                        );
                    }
                });
                println!(
                    "Thread {:?}: Program access took {:?}",
                    std::thread::current().id(),
                    start.elapsed()
                );

                let output_str = COMPILED_PROGRAMS
                    .with(|programs| {
                        let mut programs = programs.borrow_mut();
                        let program = programs.get_mut(&self.expression).unwrap();
                        program.run(&input_str)
                    })
                    .map_err(|err| DAGError::ExecutionError {
                        node_id: "unknown".to_string(),
                        reason: format!("Failed to execute jq: {err}"),
                    })?;

                let start = Instant::now();
                println!(
                    "Thread {:?}: JQ execution took {:?}",
                    std::thread::current().id(),
                    start.elapsed()
                );

                let output_json: Value =
                    serde_json::from_str(&output_str).map_err(|err| DAGError::ExecutionError {
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
