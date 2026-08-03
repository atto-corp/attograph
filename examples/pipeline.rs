use std::sync::Arc;

use attograph::Engine;
use attograph::model::{ExecutionStatus, GraphDef};
use attograph::storage::MemoryStorage;
use bullmq::options::RedisConnectionOptions;
use serde_json::{Value, json};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), attograph::Error> {
    let storage = Arc::new(MemoryStorage::new());
    let engine = Engine::new(RedisConnectionOptions::default(), storage)
        .concurrency(4)
        .node_attempts(2);

    engine.register_fn_versioned("load", "1.0.0", |ctx: attograph::NodeContext| async move {
        let seed = ctx.graph_input["seed"].as_i64().unwrap_or(0);
        Ok(json!({ "payload": seed }))
    })?;
    engine.register_fn_versioned(
        "transform",
        "2.1.3",
        |ctx: attograph::NodeContext| async move {
            let payload = ctx.state["load"]["payload"].as_i64().unwrap_or(0);
            Ok(json!({ "scaled": payload * 2 }))
        },
    )?;
    engine.register_fn(
        "validate",
        |_ctx| async move { Ok(json!({ "valid": true })) },
    )?;
    engine.register_fn_versioned(
        "publish",
        "0.9.0",
        |ctx: attograph::NodeContext| async move {
            let scaled = ctx.state["transform"]["scaled"].as_i64().unwrap_or(0);
            let valid = ctx.state["validate"]["valid"].as_bool().unwrap_or(false);
            Ok(json!({ "result": scaled, "valid": valid }))
        },
    )?;

    let mut graph = GraphDef::new("pipeline");
    for n in ["load", "transform", "validate", "publish"] {
        graph.add_node(n);
    }
    graph.add_edge("load", "transform");
    graph.add_edge("load", "validate");
    graph.add_edge("transform", "publish");
    graph.add_edge("validate", "publish");

    let version = engine.save_graph(&graph).await?;
    engine
        .set_alias("pipeline", "myrelease:1.0.0", &version)
        .await?;
    println!("saved graph version: {version}");

    let exec_id = engine
        .enqueue("pipeline", "myrelease:1.0.0", json!({ "seed": 21 }))
        .await?;
    println!("enqueued execution: {exec_id}");

    let mut result: Option<Value> = None;
    for _ in 0..50 {
        let exec = engine.get_execution(&exec_id).await?.expect("execution");
        match exec.status {
            ExecutionStatus::Completed => {
                result = exec.output;
                break;
            }
            ExecutionStatus::Failed => {
                println!(
                    "execution failed: {}",
                    exec.error.as_deref().unwrap_or("unknown")
                );
                break;
            }
            _ => sleep(Duration::from_millis(100)).await,
        }
    }

    match result {
        Some(out) => println!("output: {out}"),
        None => println!("timed out waiting for execution"),
    }

    let node_execs = engine.list_node_executions(&exec_id).await?;
    for ne in &node_execs {
        println!(
            "  node '{}' status={:?} attempts={}",
            ne.node, ne.status, ne.attempts
        );
    }

    let exec = engine.get_execution(&exec_id).await?.expect("execution");
    println!("node code versions: {:?}", exec.node_versions);

    engine.shutdown().await;
    Ok(())
}
