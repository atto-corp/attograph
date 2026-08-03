use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bullmq::options::RedisConnectionOptions;
use bullmq::types::BackoffStrategy;
use bullmq::worker::{CancellationToken, ProcessorFn, WorkerEvent};
use bullmq::{
    DeduplicationOptions, Job, JobOptions, JobState, Queue, QueueOptions, Worker, WorkerOptions,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

use crate::Error;
use crate::model::*;
use crate::registry::{NodeError, NodeHandler, NodeRegistry};
use crate::storage::Storage;

pub(crate) const DEFAULT_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeJobData {
    pub execution_id: String,
    pub node: String,
    pub graph: GraphDef,
    pub graph_input: Value,
}

#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
    registry: Arc<NodeRegistry>,
    storage: Arc<dyn Storage>,
    connection: RedisConnectionOptions,
    prefix: String,
    concurrency: usize,
    node_options: JobOptions,
}

struct EngineInner {
    queues: Mutex<HashMap<String, Queue>>,
    workers: Mutex<HashMap<String, WorkerHandle>>,
}

struct WorkerHandle {
    shutdown: broadcast::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Engine {
    pub fn new(connection: RedisConnectionOptions, storage: Arc<dyn Storage>) -> Self {
        Self {
            inner: Arc::new(EngineInner {
                queues: Mutex::new(HashMap::new()),
                workers: Mutex::new(HashMap::new()),
            }),
            registry: Arc::new(NodeRegistry::new()),
            storage,
            connection,
            prefix: "bull".to_string(),
            concurrency: 4,
            node_options: JobOptions {
                attempts: Some(DEFAULT_ATTEMPTS),
                backoff: Some(BackoffStrategy::Exponential(1_000)),
                ..Default::default()
            },
        }
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn node_attempts(mut self, attempts: u32) -> Self {
        self.node_options.attempts = Some(attempts);
        self
    }

    pub fn node_backoff(mut self, backoff: BackoffStrategy) -> Self {
        self.node_options.backoff = Some(backoff);
        self
    }

    pub fn register_node<H: NodeHandler + 'static>(
        &self,
        tag: impl Into<String>,
        handler: H,
    ) -> Result<(), Error> {
        self.registry.register_node(tag, handler)
    }

    pub fn register_node_versioned<H: NodeHandler + 'static>(
        &self,
        tag: impl Into<String>,
        version: impl Into<String>,
        handler: H,
    ) -> Result<(), Error> {
        self.registry.register_node_versioned(tag, version, handler)
    }

    pub fn register_fn<F, Fut>(&self, tag: impl Into<String>, f: F) -> Result<(), Error>
    where
        F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value, NodeError>> + Send + 'static,
    {
        self.registry.register_fn(tag, f)
    }

    pub fn register_fn_versioned<F, Fut>(
        &self,
        tag: impl Into<String>,
        version: impl Into<String>,
        f: F,
    ) -> Result<(), Error>
    where
        F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value, NodeError>> + Send + 'static,
    {
        self.registry.register_fn_versioned(tag, version, f)
    }

    pub fn get_registered_version(&self, tag: &str) -> Option<Option<String>> {
        self.registry.get_version(tag)
    }

    pub async fn save_graph(&self, def: &GraphDef) -> Result<VersionId, Error> {
        def.validate()?;
        let version = def.version();
        self.storage.save_graph(def, &version).await?;
        Ok(version)
    }

    pub async fn get_graph_version(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Option<GraphDef>, Error> {
        self.storage.get_graph_version(name, version).await
    }

    pub async fn set_alias(&self, name: &str, alias: &str, version: &str) -> Result<(), Error> {
        if self
            .storage
            .get_graph_version(name, version)
            .await?
            .is_none()
        {
            return Err(Error::NotFound(format!(
                "graph '{name}' version '{version}' not found"
            )));
        }
        self.storage.set_alias(name, alias, version).await
    }

    pub async fn resolve(
        &self,
        name: &str,
        version_or_alias: &str,
    ) -> Result<(GraphDef, VersionId), Error> {
        if let Some(def) = self
            .storage
            .get_graph_version(name, version_or_alias)
            .await?
        {
            return Ok((def, version_or_alias.to_string()));
        }
        if let Some(version) = self.storage.resolve_alias(name, version_or_alias).await? {
            if let Some(def) = self.storage.get_graph_version(name, &version).await? {
                return Ok((def, version));
            }
        }
        Err(Error::NotFound(format!(
            "graph '{name}' version or alias '{version_or_alias}' not found"
        )))
    }

    pub async fn enqueue(
        &self,
        graph: &str,
        version_or_alias: &str,
        input: Value,
    ) -> Result<String, Error> {
        let (def, version) = self.resolve(graph, version_or_alias).await?;
        for n in &def.nodes {
            if !self.registry.contains(n) {
                return Err(Error::Validation(format!(
                    "node '{n}' is not registered in the node registry"
                )));
            }
        }
        self.ensure_worker(graph).await?;
        let queue = self.ensure_queue(graph).await?;

        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let mut node_versions = BTreeMap::new();
        for n in &def.nodes {
            node_versions.insert(n.clone(), self.registry.get_version(n).flatten());
        }
        let exec = GraphExecution {
            id: id.clone(),
            graph_name: graph.to_string(),
            version,
            alias: Some(version_or_alias.to_string()),
            input: input.clone(),
            output: None,
            status: ExecutionStatus::Pending,
            error: None,
            node_versions,
            started_at: now,
            finished_at: None,
            created_at: now,
        };
        self.storage.create_execution(&exec).await?;

        let enqueue_start = async {
            for s in def.start_nodes() {
                enqueue_node(&queue, &id, &s, &def, &input, &self.node_options).await?;
            }
            Ok::<(), bullmq::Error>(())
        }
        .await;
        if let Err(e) = enqueue_start {
            let mut failed = exec.clone();
            failed.status = ExecutionStatus::Failed;
            failed.error = Some(format!("failed to enqueue start nodes: {e}"));
            failed.finished_at = Some(Utc::now());
            let _ = self.storage.update_execution(&failed).await;
            return Err(Error::BullMq(e));
        }

        let mut running = exec;
        running.status = ExecutionStatus::Running;
        self.storage.update_execution(&running).await?;
        Ok(id)
    }

    pub async fn get_execution(&self, id: &str) -> Result<Option<GraphExecution>, Error> {
        self.storage.get_execution(id).await
    }

    pub async fn list_executions(
        &self,
        graph: &str,
        limit: usize,
    ) -> Result<Vec<GraphExecution>, Error> {
        self.storage.list_executions(graph, limit).await
    }

    pub async fn get_node_execution(
        &self,
        execution_id: &str,
        node: &str,
    ) -> Result<Option<NodeExecution>, Error> {
        self.storage.get_node_execution(execution_id, node).await
    }

    pub async fn list_node_executions(
        &self,
        execution_id: &str,
    ) -> Result<Vec<NodeExecution>, Error> {
        self.storage.list_node_executions(execution_id).await
    }

    pub async fn shutdown(&self) {
        let workers = {
            let mut w = self.inner.workers.lock().await;
            std::mem::take(&mut *w)
        };
        for (_, handle) in workers {
            let _ = handle.shutdown.send(());
            let _ = handle.task.await;
        }
    }

    fn queue_name(graph: &str) -> String {
        graph.to_string()
    }

    async fn ensure_queue(&self, graph: &str) -> Result<Queue, Error> {
        let mut queues = self.inner.queues.lock().await;
        if let Some(q) = queues.get(graph) {
            return Ok(q.clone());
        }
        let opts = QueueOptions::new()
            .connection(self.connection.clone())
            .prefix(self.prefix.clone());
        let q = Queue::with_options(&Self::queue_name(graph), opts).await?;
        queues.insert(graph.to_string(), q.clone());
        Ok(q)
    }

    async fn ensure_worker(&self, graph: &str) -> Result<(), Error> {
        let mut workers = self.inner.workers.lock().await;
        if workers.contains_key(graph) {
            return Ok(());
        }
        let queue_name = Self::queue_name(graph);
        let queue = self.ensure_queue(graph).await?;
        let opts = WorkerOptions::new()
            .connection(self.connection.clone())
            .prefix(self.prefix.clone())
            .concurrency(self.concurrency)
            .manual_start();
        let processor = self.make_processor(queue.clone());
        let worker = Worker::with_options(&queue_name, processor, opts).await?;
        let (tx, rx) = broadcast::channel(1);
        let storage = self.storage.clone();
        let node_options = self.node_options.clone();
        let task = tokio::spawn(supervisor(queue.clone(), worker, rx, storage, node_options));
        workers.insert(graph.to_string(), WorkerHandle { shutdown: tx, task });
        Ok(())
    }

    fn make_processor(&self, queue: Queue) -> ProcessorFn {
        let registry = self.registry.clone();
        let storage = self.storage.clone();
        let node_options = self.node_options.clone();
        Arc::new(move |job: Job, _token: CancellationToken| {
            let queue = queue.clone();
            let registry = registry.clone();
            let storage = storage.clone();
            let node_options = node_options.clone();
            Box::pin(async move { process_node(queue, registry, storage, node_options, job).await })
        })
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Ok(workers) = self.inner.workers.try_lock() {
            for (_, handle) in workers.iter() {
                let _ = handle.shutdown.send(());
            }
        }
    }
}

fn job_id(execution_id: &str, node: &str) -> String {
    format!("{execution_id}:{node}")
}

fn split_job_id(job_id: &str) -> Option<(String, String)> {
    job_id
        .rsplit_once(':')
        .map(|(e, n)| (e.to_string(), n.to_string()))
}

async fn enqueue_node(
    queue: &Queue,
    execution_id: &str,
    node: &str,
    graph: &GraphDef,
    graph_input: &Value,
    base: &JobOptions,
) -> Result<(), bullmq::Error> {
    let mut opts = base.clone();
    opts.job_id = Some(job_id(execution_id, node));
    opts.deduplication = Some(DeduplicationOptions {
        id: job_id(execution_id, node),
        ttl: None,
        extend: None,
        replace: None,
        keep_last_if_active: None,
    });
    let data = NodeJobData {
        execution_id: execution_id.to_string(),
        node: node.to_string(),
        graph: graph.clone(),
        graph_input: graph_input.clone(),
    };
    queue.add(node, data).options(opts).await?;
    Ok(())
}

async fn is_completed(
    queue: &Queue,
    execution_id: &str,
    node: &str,
) -> Result<bool, bullmq::Error> {
    let jid = job_id(execution_id, node);
    match queue.get_job(&jid).await? {
        Some(job) => Ok(job.get_state().await? == JobState::Completed),
        None => Ok(false),
    }
}

async fn process_node(
    queue: Queue,
    registry: Arc<NodeRegistry>,
    storage: Arc<dyn Storage>,
    _node_options: JobOptions,
    job: Job,
) -> Result<Value, bullmq::Error> {
    let data: NodeJobData = serde_json::from_value(job.data().clone())
        .map_err(|e| bullmq::Error::Unrecoverable(format!("malformed node job: {e}")))?;
    let execution_id = data.execution_id.clone();
    let node = data.node.clone();

    let handler = match registry.get(&node) {
        Some(h) => h,
        None => {
            record_failure(
                &storage,
                &execution_id,
                &node,
                "no handler registered for node",
            )
            .await;
            return Err(bullmq::Error::Unrecoverable(format!(
                "no handler registered for node '{node}'"
            )));
        }
    };

    let mut state = BTreeMap::new();
    for p in data.graph.predecessors(&node) {
        let pj = queue.get_job(&job_id(&execution_id, &p)).await?;
        if let Some(pj) = pj {
            if let Ok(v) = serde_json::from_str::<Value>(pj.returnvalue()) {
                if !v.is_null() {
                    state.insert(p, v);
                }
            }
        }
    }

    let ctx = NodeContext {
        execution_id: execution_id.clone(),
        node: node.clone(),
        graph_input: data.graph_input.clone(),
        state,
    };

    let existing = storage
        .get_node_execution(&execution_id, &node)
        .await
        .ok()
        .flatten();
    let attempts = existing.as_ref().map(|e| e.attempts + 1).unwrap_or(1);
    let running = NodeExecution {
        execution_id: execution_id.clone(),
        node: node.clone(),
        input: ctx.input(),
        output: None,
        status: NodeExecutionStatus::Running,
        error: None,
        attempts,
        started_at: Some(Utc::now()),
        finished_at: None,
    };
    if let Err(e) = storage.save_node_execution(&running).await {
        tracing::warn!(%execution_id, %node, "failed to record node start: {e}");
    }

    match handler.run(&ctx).await {
        Ok(output) => {
            let done = NodeExecution {
                status: NodeExecutionStatus::Completed,
                output: Some(output.clone()),
                finished_at: Some(Utc::now()),
                ..running
            };
            if let Err(e) = storage.save_node_execution(&done).await {
                tracing::warn!(%execution_id, %node, "failed to record node completion: {e}");
            }
            Ok(output)
        }
        Err(NodeError::Retryable(msg)) => {
            record_failure(&storage, &execution_id, &node, &msg).await;
            Err(bullmq::Error::ProcessingError(msg))
        }
        Err(NodeError::Permanent(msg)) => {
            record_failure(&storage, &execution_id, &node, &msg).await;
            Err(bullmq::Error::Unrecoverable(msg))
        }
    }
}

async fn record_failure(storage: &Arc<dyn Storage>, execution_id: &str, node: &str, msg: &str) {
    let now = Utc::now();
    let failed = match storage.get_node_execution(execution_id, node).await {
        Ok(Some(mut ne)) => {
            ne.status = NodeExecutionStatus::Failed;
            ne.error = Some(msg.to_string());
            ne.finished_at = Some(now);
            ne
        }
        _ => NodeExecution {
            execution_id: execution_id.to_string(),
            node: node.to_string(),
            input: Value::Null,
            output: None,
            status: NodeExecutionStatus::Failed,
            error: Some(msg.to_string()),
            attempts: 1,
            started_at: Some(now),
            finished_at: Some(now),
        },
    };
    if let Err(e) = storage.save_node_execution(&failed).await {
        tracing::warn!(%execution_id, %node, "failed to record node failure: {e}");
    }
}

async fn supervisor(
    queue: Queue,
    worker: Worker,
    mut shutdown_rx: broadcast::Receiver<()>,
    storage: Arc<dyn Storage>,
    node_options: JobOptions,
) {
    if let Err(e) = worker.run().await {
        tracing::warn!("failed to start worker: {e}");
        return;
    }

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            evt = worker.next_event() => {
                match evt {
                    Some(WorkerEvent::Closed) | None => break,
                    Some(WorkerEvent::Completed { job_id, .. }) => {
                        if let Some((execution_id, node)) = split_job_id(&job_id) {
                            if let Err(e) = on_node_completed(&queue, &storage, &node_options, &execution_id, &node).await {
                                tracing::warn!(%job_id, "completion handler error: {e}");
                            }
                        }
                    }
                    Some(WorkerEvent::Failed { job_id, error }) => {
                        if let Some((execution_id, node)) = split_job_id(&job_id) {
                            if let Err(e) = on_node_failed(&queue, &storage, &execution_id, &node, &error).await {
                                tracing::warn!(%job_id, "failure handler error: {e}");
                            }
                        }
                    }
                    Some(_) => {}
                }
            }
        }
    }

    let _ = worker.close(5_000).await;
}

async fn on_node_completed(
    queue: &Queue,
    storage: &Arc<dyn Storage>,
    node_options: &JobOptions,
    execution_id: &str,
    node: &str,
) -> Result<(), Error> {
    let job = queue.get_job(&job_id(execution_id, node)).await?;
    let Some(job) = job else {
        return Ok(());
    };
    let data: NodeJobData = serde_json::from_value(job.data().clone())?;
    let graph = &data.graph;

    for succ in graph.successors(node) {
        let mut ready = true;
        for p in graph.predecessors(&succ) {
            if !is_completed(queue, execution_id, p.as_str()).await? {
                ready = false;
                break;
            }
        }
        if ready {
            enqueue_node(
                queue,
                execution_id,
                succ.as_str(),
                graph,
                &data.graph_input,
                node_options,
            )
            .await?;
        }
    }

    let ends = graph.end_nodes();
    if !ends.iter().any(|e| e == node) {
        return Ok(());
    }

    let mut all_done = true;
    let mut output = Map::new();
    for e in &ends {
        let ej = queue.get_job(&job_id(execution_id, e)).await?;
        match ej {
            Some(ej) if ej.get_state().await? == JobState::Completed => {
                if let Ok(v) = serde_json::from_str::<Value>(ej.returnvalue()) {
                    if !v.is_null() {
                        output.insert(e.clone(), v);
                    }
                }
            }
            _ => {
                all_done = false;
                break;
            }
        }
    }

    if all_done {
        if let Some(mut ex) = storage.get_execution(execution_id).await? {
            if ex.status == ExecutionStatus::Running || ex.status == ExecutionStatus::Pending {
                ex.status = ExecutionStatus::Completed;
                ex.output = Some(Value::Object(output));
                ex.finished_at = Some(Utc::now());
                storage.update_execution(&ex).await?;
                tracing::info!(%execution_id, "graph execution completed");
            }
        }
    }
    Ok(())
}

async fn on_node_failed(
    queue: &Queue,
    storage: &Arc<dyn Storage>,
    execution_id: &str,
    node: &str,
    error: &str,
) -> Result<(), Error> {
    let retrying = match queue.get_job(&job_id(execution_id, node)).await {
        Ok(Some(job)) => {
            let max = job.opts().attempts.unwrap_or(1);
            let made = job.attempts_made();
            made < max
        }
        _ => false,
    };
    if retrying {
        return Ok(());
    }
    if let Some(mut ex) = storage.get_execution(execution_id).await? {
        if ex.status == ExecutionStatus::Running || ex.status == ExecutionStatus::Pending {
            ex.status = ExecutionStatus::Failed;
            ex.error = Some(format!("node '{node}' failed: {error}"));
            ex.finished_at = Some(Utc::now());
            storage.update_execution(&ex).await?;
            tracing::info!(%execution_id, "graph execution failed");
        }
    }
    Ok(())
}
