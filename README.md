# attograph

A langgraph-esque graph workflow execution layer built on the Rust library
implementation of BullMQ. Workflows are modeled as a DAG of string-tagged
nodes; each node has a concrete handler registered in a `NodeRegistry`. A
graph runs as a set of BullMQ jobs (one per node) staged in topological order,
executed and retried against Redis, with execution history persisted through a
pluggable `Storage`.

## Features
- DAG graphs of string-tagged nodes and edges (json in/out)
- Content-addressed versions (canonical sha256) with named aliases
- Topological scheduling with dependency-aware `state` (predecessor outputs)
- Retries with exponential backoff (`Retryable`) vs `Permanent` failures
- Recorded `GraphExecution` and `NodeExecution` histories
- Pluggable storage: in-process `MemoryStorage` or `MongoStorage` (`mongo` feature)

## Getting started
```rust
use std::sync::Arc;
use attograph::{Engine, NodeContext};
use attograph::model::GraphDef;
use attograph::storage::MemoryStorage;
use bullmq::options::RedisConnectionOptions;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), attograph::Error> {
    let engine = Engine::new(RedisConnectionOptions::default(), Arc::new(MemoryStorage::new()));

    engine.register_fn("double", |ctx: NodeContext| async move {
        let n = ctx.graph_input["n"].as_i64().unwrap_or(0);
        Ok(json!({ "value": n * 2 }))
    })?;

    let mut g = GraphDef::new("demo");
    g.add_node("double");
    let version = engine.save_graph(&g).await?;
    let exec_id = engine.enqueue("demo", &version, json!({ "n": 21 })).await?;
    // poll engine.get_execution(&exec_id) until Completed/Failed
    Ok(())
}
```

See `examples/pipeline.rs` for a fuller diamond-shaped pipeline.

## Concepts
- **GraphDef** — name + nodes + edges (a DAG). References nodes only via tags;
  loadable from json/file. Validates emptiness, duplicates, unknown edges,
  `:` tags, and cycles.
- **NodeRegistry** — `register_fn`/`register_node`; handlers read
  `NodeContext { graph_input, state }`, where `state` maps predecessor → output.
- **Engine** — `save_graph`, `enqueue`, `set_alias`, `resolve`, `get_execution`,
  `get_node_execution`; configurable `prefix`, `concurrency`, `node_attempts`,
  `node_backoff`.
- **Storage** — save/get graphs and versions, set/resolve aliases, and record
  executions and node-execution snapshots.