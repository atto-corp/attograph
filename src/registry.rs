use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use crate::Error;
use crate::model::NodeContext;

#[derive(Debug, Clone, thiserror::Error)]
pub enum NodeError {
    #[error("retryable node error: {0}")]
    Retryable(String),
    #[error("permanent node error: {0}")]
    Permanent(String),
}

impl From<String> for NodeError {
    fn from(v: String) -> Self {
        NodeError::Retryable(v)
    }
}

impl From<&str> for NodeError {
    fn from(v: &str) -> Self {
        NodeError::Retryable(v.to_string())
    }
}

impl From<serde_json::Error> for NodeError {
    fn from(v: serde_json::Error) -> Self {
        NodeError::Retryable(v.to_string())
    }
}

impl From<std::io::Error> for NodeError {
    fn from(v: std::io::Error) -> Self {
        NodeError::Retryable(v.to_string())
    }
}

#[async_trait]
pub trait NodeHandler: Send + Sync {
    async fn run(&self, ctx: &NodeContext) -> Result<Value, NodeError>;
}

struct RegisteredNode {
    handler: Arc<dyn NodeHandler>,
    version: Option<String>,
}

#[derive(Clone, Default)]
pub struct NodeRegistry {
    inner: Arc<Mutex<HashMap<String, RegisteredNode>>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn register_inner(
        &self,
        tag: impl Into<String>,
        version: Option<String>,
        handler: Arc<dyn NodeHandler>,
    ) -> Result<(), Error> {
        let tag = tag.into();
        if tag.contains(':') {
            return Err(Error::Validation(format!(
                "node tag must not contain ':': {tag}"
            )));
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.contains_key(&tag) {
            return Err(Error::DuplicateNode(tag));
        }
        inner.insert(tag, RegisteredNode { handler, version });
        Ok(())
    }

    pub fn register(
        &self,
        tag: impl Into<String>,
        handler: Arc<dyn NodeHandler>,
    ) -> Result<(), Error> {
        self.register_inner(tag, None, handler)
    }

    pub fn register_versioned(
        &self,
        tag: impl Into<String>,
        version: impl Into<String>,
        handler: Arc<dyn NodeHandler>,
    ) -> Result<(), Error> {
        self.register_inner(tag, Some(version.into()), handler)
    }

    pub fn register_node<H: NodeHandler + 'static>(
        &self,
        tag: impl Into<String>,
        handler: H,
    ) -> Result<(), Error> {
        self.register(tag, Arc::new(handler))
    }

    pub fn register_node_versioned<H: NodeHandler + 'static>(
        &self,
        tag: impl Into<String>,
        version: impl Into<String>,
        handler: H,
    ) -> Result<(), Error> {
        self.register_versioned(tag, version, Arc::new(handler))
    }

    pub fn register_fn<F, Fut>(&self, tag: impl Into<String>, f: F) -> Result<(), Error>
    where
        F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, NodeError>> + Send + 'static,
    {
        self.register(tag, Arc::new(AsyncFnHandler(f)))
    }

    pub fn register_fn_versioned<F, Fut>(
        &self,
        tag: impl Into<String>,
        version: impl Into<String>,
        f: F,
    ) -> Result<(), Error>
    where
        F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, NodeError>> + Send + 'static,
    {
        self.register_versioned(tag, version, Arc::new(AsyncFnHandler(f)))
    }

    pub fn get(&self, tag: &str) -> Option<Arc<dyn NodeHandler>> {
        self.inner
            .lock()
            .unwrap()
            .get(tag)
            .map(|n| n.handler.clone())
    }

    pub fn get_version(&self, tag: &str) -> Option<Option<String>> {
        self.inner
            .lock()
            .unwrap()
            .get(tag)
            .map(|n| n.version.clone())
    }

    pub fn contains(&self, tag: &str) -> bool {
        self.inner.lock().unwrap().contains_key(tag)
    }
}

struct AsyncFnHandler<F>(F);

#[async_trait]
impl<F, Fut> NodeHandler for AsyncFnHandler<F>
where
    F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, NodeError>> + Send + 'static,
{
    async fn run(&self, ctx: &NodeContext) -> Result<Value, NodeError> {
        (self.0)(ctx.clone()).await
    }
}
