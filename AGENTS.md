# AGENTS.md

Rust crate `attograph`, edition 2024. Graph workflow execution over BullMQ
(Redis-backed queues) with pluggable storage; MongoDB storage behind a feature
flag.

## Commands
- Build: `cargo build`
- Test: `cargo test` (requires Redis for `tests/engine.rs`; model/unit tests do not)
- Run example: `cargo run --example pipeline` (requires Redis)
- Lint: `cargo clippy --all-features`
- Format: `cargo fmt`
- Docs: `cargo doc --no-deps`

## Layout
- `src/model.rs` — `GraphDef`, `Edge`, `VersionId`, canonical sha256 hashing,
  `GraphExecution`, `NodeExecution`, `NodeContext`, status enums
- `src/registry.rs` — `NodeRegistry`, `NodeHandler` trait, `NodeError` (`Retryable`/`Permanent`)
- `src/engine.rs` — `Engine`, BullMQ queue + worker supervisor, topological
  scheduling, dependency fan-out, retries, graph completion
- `src/storage.rs` — `Storage` trait + `MemoryStorage`
- `src/mongo.rs` — `MongoStorage` (behind the `mongo` feature, on by default)
- `src/error.rs` — unified `Error` enum
- `tests/model.rs` — unit tests; `tests/engine.rs` — Redis integration tests
- `examples/pipeline.rs` — runnable demo

## Conventions
- Serde `camelCase` on JSON-facing model types (GraphDef, edges, executions).
- Node tags must not contain `:`; node errors are `NodeError::Retryable`
  (retried with backoff) or `NodeError::Permanent` (fails the graph).
- Version ids are sha256 hex of the canonical (sorted-key) json.
- Follow the existing builder style on `Engine` (`prefix`, `concurrency`,
  `node_attempts`, `node_backoff`).
- Async uses `tokio`; storage methods are `async fn` via `async_trait`.