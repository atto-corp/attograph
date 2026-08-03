use std::sync::{Arc, Mutex};
use std::time::Duration;

use attograph::model::{ExecutionStatus, GraphDef, NodeExecutionStatus};
use attograph::{Engine, MemoryStorage, NodeError};
use bullmq::options::RedisConnectionOptions;
use serde_json::{Value, json};

fn diamond() -> GraphDef {
    let mut g = GraphDef::new("diamond");
    for n in ["a", "b", "c", "d"] {
        g.add_node(n);
    }
    g.add_edge("a", "b");
    g.add_edge("a", "c");
    g.add_edge("b", "d");
    g.add_edge("c", "d");
    g
}

#[tokio::test]
async fn runs_in_topological_order_and_merges_output() {
    let storage = Arc::new(MemoryStorage::new());
    let engine = Engine::new(RedisConnectionOptions::default(), storage);

    let order_log = Arc::new(Mutex::new(Vec::<String>::new()));

    let log = order_log.clone();
    engine
        .register_fn("a", move |ctx: attograph::NodeContext| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push("a".to_string());
                Ok(json!({ "seed": ctx.graph_input["n"].clone() }))
            }
        })
        .unwrap();
    engine
        .register_fn("b", {
            let log = order_log.clone();
            move |ctx: attograph::NodeContext| {
                let log = log.clone();
                async move {
                    log.lock().unwrap().push("b".to_string());
                    Ok(json!({ "scaled": ctx.state["a"]["seed"].as_i64().unwrap() * 2 }))
                }
            }
        })
        .unwrap();
    engine
        .register_fn("c", {
            let log = order_log.clone();
            move |_ctx| {
                let log = log.clone();
                async move {
                    log.lock().unwrap().push("c".to_string());
                    Ok(json!({ "ok": true }))
                }
            }
        })
        .unwrap();
    engine
        .register_fn("d", {
            let log = order_log.clone();
            move |ctx: attograph::NodeContext| {
                let log = log.clone();
                async move {
                    log.lock().unwrap().push("d".to_string());
                    Ok(json!({
                        "value": ctx.state["b"]["scaled"].as_i64().unwrap(),
                        "valid": ctx.state["c"]["ok"].as_bool().unwrap()
                    }))
                }
            }
        })
        .unwrap();

    let g = diamond();
    let version = engine.save_graph(&g).await.unwrap();
    assert_eq!(version, g.version());

    let exec_id = engine
        .enqueue("diamond", &version, json!({ "n": 4 }))
        .await
        .unwrap();

    let output = wait_for_completion(&engine, &exec_id).await;
    assert_eq!(output, json!({ "d": { "value": 8, "valid": true } }));

    let log = order_log.lock().unwrap();
    assert_eq!(log.len(), 4);
    assert_eq!(log.first().unwrap(), "a");
    assert_eq!(log.last().unwrap(), "d");

    let node_execs = engine.list_node_executions(&exec_id).await.unwrap();
    for ne in &node_execs {
        assert_eq!(ne.status, NodeExecutionStatus::Completed);
    }
    let exec = engine.get_execution(&exec_id).await.unwrap().unwrap();
    assert_eq!(exec.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn retries_then_completes() {
    let storage = Arc::new(MemoryStorage::new());
    let engine = Engine::new(RedisConnectionOptions::default(), storage).node_attempts(3);

    let attempts = Arc::new(Mutex::new(0));
    let counter = attempts.clone();
    engine
        .register_fn("boom", move |_ctx| {
            let counter = counter.clone();
            async move {
                let mut n = counter.lock().unwrap();
                *n += 1;
                if *n < 3 {
                    Err(NodeError::Retryable("not yet".into()))
                } else {
                    Ok(json!({ "done": true }))
                }
            }
        })
        .unwrap();

    let mut g = GraphDef::new("retry");
    g.add_node("boom");
    let version = engine.save_graph(&g).await.unwrap();
    let exec_id = engine.enqueue("retry", &version, json!({})).await.unwrap();

    let output = wait_for_completion(&engine, &exec_id).await;
    assert_eq!(output, json!({ "boom": { "done": true } }));

    let ne = engine
        .get_node_execution(&exec_id, "boom")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ne.attempts, 3);
    assert_eq!(*attempts.lock().unwrap(), 3);
}

#[tokio::test]
async fn rejects_unregistered_node_at_enqueue() {
    let storage = Arc::new(MemoryStorage::new());
    let engine = Engine::new(RedisConnectionOptions::default(), storage);

    let mut g = GraphDef::new("missing");
    g.add_node("nonexistent");
    let version = engine.save_graph(&g).await.unwrap();

    let err = engine
        .enqueue("missing", &version, json!({}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not registered"));
}

#[tokio::test]
async fn alias_resolves_to_version() {
    let storage = Arc::new(MemoryStorage::new());
    let engine = Engine::new(RedisConnectionOptions::default(), storage);

    let mut g = GraphDef::new("aliased");
    g.add_node("n");
    let v1 = engine.save_graph(&g).await.unwrap();
    engine.set_alias("aliased", "release", &v1).await.unwrap();

    let (graph, version) = engine.resolve("aliased", "release").await.unwrap();
    assert_eq!(graph.name, "aliased");
    assert_eq!(version, v1);
}

#[tokio::test]
async fn records_node_code_versions_on_execution() {
    let storage = Arc::new(MemoryStorage::new());
    let engine = Engine::new(RedisConnectionOptions::default(), storage);

    engine
        .register_fn("plain", |_ctx| async move { Ok(json!({ "ok": true })) })
        .unwrap();
    engine
        .register_fn_versioned(
            "vloaded",
            "1.0.0",
            |_ctx| async move { Ok(json!({ "v": 1 })) },
        )
        .unwrap();
    engine
        .register_fn_versioned(
            "tagged",
            "2.0.0",
            |_ctx| async move { Ok(json!({ "t": 2 })) },
        )
        .unwrap();

    let mut g = GraphDef::new("versions");
    for n in ["plain", "vloaded", "tagged"] {
        g.add_node(n);
    }
    let version = engine.save_graph(&g).await.unwrap();
    let exec_id = engine
        .enqueue("versions", &version, json!({}))
        .await
        .unwrap();

    let wait = wait_for_completion(&engine, &exec_id).await;
    assert_eq!(
        wait,
        json!({ "plain": { "ok": true }, "vloaded": { "v": 1 }, "tagged": { "t": 2 } })
    );

    let exec = engine.get_execution(&exec_id).await.unwrap().unwrap();
    assert_eq!(exec.node_versions["vloaded"], Some("1.0.0".to_string()));
    assert_eq!(exec.node_versions["tagged"], Some("2.0.0".to_string()));
    assert_eq!(exec.node_versions["plain"], None);

    assert_eq!(
        engine.get_registered_version("tagged"),
        Some(Some("2.0.0".to_string()))
    );
}

async fn wait_for_completion(engine: &Engine, exec_id: &str) -> Value {
    for _ in 0..100 {
        let exec = engine.get_execution(exec_id).await.unwrap().unwrap();
        match exec.status {
            ExecutionStatus::Completed => return exec.output.unwrap_or(json!(null)),
            ExecutionStatus::Failed => panic!("execution failed: {:?}", exec.error),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    panic!("timed out waiting for execution {exec_id}");
}
