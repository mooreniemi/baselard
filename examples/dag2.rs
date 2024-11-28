use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::{sync::watch, task};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Component {
    name: String,
    dependencies: Vec<String>,
    execution_time: u64,
}

fn topological_sort(components: &[Component]) -> Vec<Component> {
    let mut in_degree = HashMap::new();
    let mut graph = HashMap::new();

    for component in components {
        graph.entry(component.name.clone()).or_insert(vec![]);
        in_degree.entry(component.name.clone()).or_insert(0);

        for dep in &component.dependencies {
            graph
                .entry(dep.clone())
                .or_insert(vec![])
                .push(component.name.clone());
            *in_degree.entry(component.name.clone()).or_insert(0) += 1;
        }
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &degree)| degree == 0)
        .map(|(name, _)| name.clone())
        .collect();

    let mut sorted = Vec::new();
    while let Some(node) = queue.pop_front() {
        sorted.push(node.clone());
        if let Some(children) = graph.get(&node) {
            for child in children {
                if let Some(degree) = in_degree.get_mut(child) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
    }

    sorted
        .iter()
        .filter_map(|name| components.iter().find(|c| &c.name == name).cloned())
        .collect()
}

fn execute_component_blocking(
    name: String,
    dependencies: Vec<String>,
    execution_time: u64,
    start_time: Instant,
    notifiers: Arc<Mutex<HashMap<String, watch::Sender<()>>>>,
    receivers: HashMap<String, watch::Receiver<()>>,
    outputs: Arc<Mutex<HashMap<String, String>>>,
) {
    let rt_handle = tokio::runtime::Handle::current();

    let mut received_outputs = HashMap::new();
    for dep in dependencies {
        if let Some(receiver) = receivers.get(&dep) {
            let mut cloned_receiver = receiver.clone();
            rt_handle.block_on(cloned_receiver.changed()).ok();

            if let Some(output) = outputs.lock().unwrap().get(&dep) {
                received_outputs.insert(dep.clone(), output.clone());
            }
        }
    }

    let elapsed = start_time.elapsed().as_secs_f32();
    println!(
        "[{elapsed:.2}s] Executing component: {name} with inputs: {received_outputs:?} (takes {execution_time}s)"
    );

    std::thread::sleep(std::time::Duration::from_secs(execution_time));

    let output = format!("Output of {name}");
    outputs.lock().unwrap().insert(name.clone(), output);

    if let Some(sender) = notifiers.lock().unwrap().get(&name) {
        let _ = sender.send(());
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let components = vec![
        Component {
            name: "A".to_string(),
            dependencies: vec![],
            execution_time: 3,
        },
        Component {
            name: "B".to_string(),
            dependencies: vec!["A".to_string()],
            execution_time: 1,
        },
        Component {
            name: "C".to_string(),
            dependencies: vec!["A".to_string()],
            execution_time: 1,
        },
        Component {
            name: "D".to_string(),
            dependencies: vec![],
            execution_time: 4,
        },
        Component {
            name: "E".to_string(),
            dependencies: vec!["B".to_string(), "C".to_string(), "D".to_string()],
            execution_time: 1,
        },
    ];

    let start_time = Instant::now();
    let sorted_components = topological_sort(&components);
    let elapsed = start_time.elapsed().as_secs_f32();
    println!(
        "[{elapsed:.2}s] Topological sort: {sorted_components:?}"
    );

    let notifiers: Arc<Mutex<HashMap<String, watch::Sender<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let outputs: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut handles = Vec::new();

    for component in &sorted_components {
        let (tx, _rx) = watch::channel(());
        notifiers.lock().unwrap().insert(component.name.clone(), tx);

        let mut receivers = HashMap::new();
        for dep in &component.dependencies {
            if let Some(sender) = notifiers.lock().unwrap().get(dep) {
                receivers.insert(dep.clone(), sender.subscribe());
            }
        }

        let name = component.name.clone();
        let dependencies = component.dependencies.clone();
        let execution_time = component.execution_time;
        let notifiers = Arc::clone(&notifiers);
        let outputs = Arc::clone(&outputs);

        let handle = task::spawn_blocking(move || {
            execute_component_blocking(
                name,
                dependencies,
                execution_time,
                start_time,
                notifiers,
                receivers,
                outputs,
            );
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    println!("Final outputs: {:?}", outputs.lock().unwrap());
    println!(
        "Total workflow execution time: {:.2?}",
        start_time.elapsed()
    );
}
