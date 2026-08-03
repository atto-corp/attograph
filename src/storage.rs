use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::Error;
use crate::model::{GraphDef, GraphExecution, NodeExecution, VersionId};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn save_graph(&self, def: &GraphDef, version: &VersionId) -> Result<(), Error>;
    async fn get_graph_version(&self, name: &str, version: &str)
    -> Result<Option<GraphDef>, Error>;
    async fn set_alias(&self, name: &str, alias: &str, version: &str) -> Result<(), Error>;
    async fn resolve_alias(&self, name: &str, alias: &str) -> Result<Option<String>, Error>;
    async fn create_execution(&self, e: &GraphExecution) -> Result<(), Error>;
    async fn update_execution(&self, e: &GraphExecution) -> Result<(), Error>;
    async fn get_execution(&self, id: &str) -> Result<Option<GraphExecution>, Error>;
    async fn list_executions(
        &self,
        graph: &str,
        limit: usize,
    ) -> Result<Vec<GraphExecution>, Error>;
    async fn save_node_execution(&self, ne: &NodeExecution) -> Result<(), Error>;
    async fn get_node_execution(
        &self,
        execution_id: &str,
        node: &str,
    ) -> Result<Option<NodeExecution>, Error>;
    async fn list_node_executions(&self, execution_id: &str) -> Result<Vec<NodeExecution>, Error>;
}

#[derive(Default)]
struct MemoryInner {
    graphs: HashMap<String, HashMap<VersionId, GraphDef>>,
    aliases: HashMap<String, HashMap<String, VersionId>>,
    executions: HashMap<String, GraphExecution>,
    node_executions: HashMap<(String, String), NodeExecution>,
}

#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<Mutex<MemoryInner>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn save_graph(&self, def: &GraphDef, version: &VersionId) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .graphs
            .entry(def.name.clone())
            .or_default()
            .insert(version.clone(), def.clone());
        Ok(())
    }

    async fn get_graph_version(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Option<GraphDef>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.graphs.get(name).and_then(|v| v.get(version).cloned()))
    }

    async fn set_alias(&self, name: &str, alias: &str, version: &str) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .aliases
            .entry(name.to_string())
            .or_default()
            .insert(alias.to_string(), version.to_string());
        Ok(())
    }

    async fn resolve_alias(&self, name: &str, alias: &str) -> Result<Option<String>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.aliases.get(name).and_then(|a| a.get(alias).cloned()))
    }

    async fn create_execution(&self, e: &GraphExecution) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.executions.contains_key(&e.id) {
            return Err(Error::Storage(format!(
                "execution '{}' already exists",
                e.id
            )));
        }
        inner.executions.insert(e.id.clone(), e.clone());
        Ok(())
    }

    async fn update_execution(&self, e: &GraphExecution) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.executions.insert(e.id.clone(), e.clone());
        Ok(())
    }

    async fn get_execution(&self, id: &str) -> Result<Option<GraphExecution>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.executions.get(id).cloned())
    }

    async fn list_executions(
        &self,
        graph: &str,
        limit: usize,
    ) -> Result<Vec<GraphExecution>, Error> {
        let inner = self.inner.lock().unwrap();
        let mut all: Vec<GraphExecution> = inner
            .executions
            .values()
            .filter(|e| e.graph_name == graph)
            .cloned()
            .collect();
        all.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        all.truncate(limit);
        Ok(all)
    }

    async fn save_node_execution(&self, ne: &NodeExecution) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .node_executions
            .insert((ne.execution_id.clone(), ne.node.clone()), ne.clone());
        Ok(())
    }

    async fn get_node_execution(
        &self,
        execution_id: &str,
        node: &str,
    ) -> Result<Option<NodeExecution>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .node_executions
            .get(&(execution_id.to_string(), node.to_string()))
            .cloned())
    }

    async fn list_node_executions(&self, execution_id: &str) -> Result<Vec<NodeExecution>, Error> {
        let inner = self.inner.lock().unwrap();
        let mut all: Vec<NodeExecution> = inner
            .node_executions
            .iter()
            .filter(|((eid, _), _)| eid == execution_id)
            .map(|(_, ne)| ne.clone())
            .collect();
        all.sort_by_key(|ne| ne.started_at);
        Ok(all)
    }
}
