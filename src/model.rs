use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::Error;

pub type VersionId = String;

fn default_schema_version() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GraphDef {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub name: String,
    pub nodes: Vec<String>,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

impl GraphDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema_version: 1,
            name: name.into(),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, tag: impl Into<String>) {
        self.nodes.push(tag.into());
    }

    pub fn add_edge(&mut self, from: impl Into<String>, to: impl Into<String>) {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
        });
    }

    pub fn from_json(value: &Value) -> Result<Self, Error> {
        Ok(serde_json::from_value(value.clone())?)
    }

    pub fn from_str(s: &str) -> Result<Self, Error> {
        Ok(serde_json::from_str(s)?)
    }

    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let s = std::fs::read_to_string(path)?;
        Self::from_str(&s)
    }

    pub fn to_file(&self, path: impl AsRef<std::path::Path>) -> Result<(), Error> {
        let s = serde_json::to_string_pretty(self)?;
        std::fs::write(path, s)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty() {
            return Err(Error::Validation("graph name must not be empty".into()));
        }
        if self.nodes.is_empty() {
            return Err(Error::Validation(format!(
                "graph '{}' must declare at least one node",
                self.name
            )));
        }
        let mut seen = HashSet::new();
        for n in &self.nodes {
            if n.contains(':') {
                return Err(Error::Validation(format!(
                    "node tag must not contain ':': {n}"
                )));
            }
            if !seen.insert(n.clone()) {
                return Err(Error::Validation(format!(
                    "graph '{}' declares duplicate node '{n}'",
                    self.name
                )));
            }
        }
        for e in &self.edges {
            if !seen.contains(&e.from) {
                return Err(Error::Validation(format!(
                    "edge '{} -> {}' references unknown node '{}'",
                    e.from, e.to, e.from
                )));
            }
            if !seen.contains(&e.to) {
                return Err(Error::Validation(format!(
                    "edge '{} -> {}' references unknown node '{}'",
                    e.from, e.to, e.to
                )));
            }
        }
        if let Some(cycle) = self.find_cycle() {
            return Err(Error::Validation(format!(
                "graph '{}' contains a cycle: {}",
                self.name,
                cycle.join(" -> ")
            )));
        }
        Ok(())
    }

    fn find_cycle(&self) -> Option<Vec<String>> {
        let mut indeg: HashMap<&str, usize> = HashMap::new();
        for n in &self.nodes {
            indeg.insert(n, 0);
        }
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.edges {
            adj.entry(&e.from).or_default().push(&e.to);
            *indeg.get_mut(e.to.as_str()).unwrap() += 1;
        }
        let mut queue: VecDeque<&str> = indeg
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(n, _)| *n)
            .collect();
        let mut visited = 0usize;
        let mut order = Vec::with_capacity(self.nodes.len());
        while let Some(n) = queue.pop_front() {
            visited += 1;
            order.push(n.to_string());
            if let Some(next) = adj.get(n) {
                for m in next {
                    let d = indeg.get_mut(*m).unwrap();
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(m);
                    }
                }
            }
        }
        if visited == self.nodes.len() {
            None
        } else {
            Some(order)
        }
    }

    pub fn predecessors(&self, node: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|e| e.to == node)
            .map(|e| e.from.clone())
            .collect()
    }

    pub fn successors(&self, node: &str) -> Vec<String> {
        self.edges
            .iter()
            .filter(|e| e.from == node)
            .map(|e| e.to.clone())
            .collect()
    }

    pub fn start_nodes(&self) -> Vec<String> {
        let has_incoming: HashSet<&str> = self.edges.iter().map(|e| e.to.as_str()).collect();
        self.nodes
            .iter()
            .filter(|n| !has_incoming.contains(n.as_str()))
            .cloned()
            .collect()
    }

    pub fn end_nodes(&self) -> Vec<String> {
        let has_outgoing: HashSet<&str> = self.edges.iter().map(|e| e.from.as_str()).collect();
        self.nodes
            .iter()
            .filter(|n| !has_outgoing.contains(n.as_str()))
            .cloned()
            .collect()
    }

    pub fn version(&self) -> VersionId {
        let value = serde_json::to_value(self).expect("GraphDef serializes");
        let canonical = canonical_value(&value);
        let s = serde_json::to_string(&canonical).expect("canonical json serializes");
        let mut hasher = Sha256::new();
        hasher.update(s.as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

pub(crate) fn canonical_value(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for k in keys {
                out.insert(k.clone(), canonical_value(&map[k]));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        other => other.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphExecution {
    pub id: String,
    pub graph_name: String,
    pub version: VersionId,
    pub alias: Option<String>,
    pub input: Value,
    pub output: Option<Value>,
    pub status: ExecutionStatus,
    pub error: Option<String>,
    #[serde(default)]
    pub node_versions: BTreeMap<String, Option<String>>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecution {
    pub execution_id: String,
    pub node: String,
    pub input: Value,
    pub output: Option<Value>,
    pub status: NodeExecutionStatus,
    pub error: Option<String>,
    pub attempts: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NodeContext {
    pub execution_id: String,
    pub node: String,
    pub graph_input: Value,
    pub state: BTreeMap<String, Value>,
}

impl NodeContext {
    pub fn input(&self) -> Value {
        json!({ "input": self.graph_input, "state": self.state })
    }
}
