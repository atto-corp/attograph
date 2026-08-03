use mongodb::bson::{self, Bson, Document, doc};
use mongodb::{Client, Collection, Database};

use crate::Error;
use crate::model::{GraphDef, GraphExecution, NodeExecution, VersionId};
use crate::storage::Storage;

#[derive(Clone)]
pub struct MongoStorage {
    graphs: Collection<Document>,
    executions: Collection<Document>,
    node_executions: Collection<Document>,
}

impl MongoStorage {
    pub async fn connect(uri: &str, db_name: &str) -> Result<Self, Error> {
        let client = Client::with_uri_str(uri).await?;
        Self::from_client(client, db_name).await
    }

    pub async fn from_client(client: Client, db_name: &str) -> Result<Self, Error> {
        let db: Database = client.database(db_name);
        let graphs = db.collection("graphs");
        let executions = db.collection("executions");
        let node_executions = db.collection("node_executions");
        Ok(Self {
            graphs,
            executions,
            node_executions,
        })
    }
}

fn json_to_bson(v: &serde_json::Value) -> Result<Bson, Error> {
    bson::to_bson(v).map_err(|e| Error::Storage(format!("bson encode: {e}")))
}

fn bson_to_json(b: Bson) -> Result<serde_json::Value, Error> {
    bson::from_bson(b).map_err(|e| Error::Storage(format!("bson decode: {e}")))
}

fn encode<T: serde::Serialize>(v: &T) -> Result<Bson, Error> {
    json_to_bson(&serde_json::to_value(v)?)
}

fn decode<T: serde::de::DeserializeOwned>(b: Bson) -> Result<T, Error> {
    Ok(serde_json::from_value(bson_to_json(b)?)?)
}

#[async_trait::async_trait]
impl Storage for MongoStorage {
    async fn save_graph(&self, def: &GraphDef, version: &VersionId) -> Result<(), Error> {
        let mut set = Document::new();
        set.insert(format!("versions.{version}"), encode(def)?);
        let update = doc! { "$set": set };
        let filter = doc! { "_id": &def.name };
        self.graphs.update_one(filter, update).upsert(true).await?;
        Ok(())
    }

    async fn get_graph_version(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Option<GraphDef>, Error> {
        let filter = doc! { "_id": name };
        let Some(doc) = self.graphs.find_one(filter).await? else {
            return Ok(None);
        };
        let Some(versions) = doc.get_document("versions").ok() else {
            return Ok(None);
        };
        let Some(version_doc) = versions.get(version) else {
            return Ok(None);
        };
        Ok(Some(decode(version_doc.clone())?))
    }

    async fn set_alias(&self, name: &str, alias: &str, version: &str) -> Result<(), Error> {
        let mut set = Document::new();
        set.insert(format!("aliases.{alias}"), version);
        let update = doc! { "$set": set };
        let filter = doc! { "_id": name };
        self.graphs.update_one(filter, update).upsert(true).await?;
        Ok(())
    }

    async fn resolve_alias(&self, name: &str, alias: &str) -> Result<Option<String>, Error> {
        let filter = doc! { "_id": name };
        let Some(doc) = self.graphs.find_one(filter).await? else {
            return Ok(None);
        };
        let Some(aliases) = doc.get_document("aliases").ok() else {
            return Ok(None);
        };
        match aliases.get_str(alias) {
            Ok(v) => Ok(Some(v.to_string())),
            Err(_) => Ok(None),
        }
    }

    async fn create_execution(&self, e: &GraphExecution) -> Result<(), Error> {
        let mut doc = Document::new();
        doc.insert("_id", e.id.clone());
        doc.insert("data", encode(e)?);
        self.executions.insert_one(doc).await?;
        Ok(())
    }

    async fn update_execution(&self, e: &GraphExecution) -> Result<(), Error> {
        let update = doc! { "$set": { "data": encode(e)? } };
        self.executions
            .update_one(doc! { "_id": &e.id }, update)
            .await?;
        Ok(())
    }

    async fn get_execution(&self, id: &str) -> Result<Option<GraphExecution>, Error> {
        let Some(doc) = self.executions.find_one(doc! { "_id": id }).await? else {
            return Ok(None);
        };
        let Some(data) = doc.get("data") else {
            return Ok(None);
        };
        Ok(Some(decode(data.clone())?))
    }

    async fn list_executions(
        &self,
        graph: &str,
        limit: usize,
    ) -> Result<Vec<GraphExecution>, Error> {
        let mut cursor = self
            .executions
            .find(doc! { "data.graphName": graph })
            .sort(doc! { "data.startedAt": -1 })
            .limit(limit as i64)
            .await?;
        let mut out = Vec::new();
        while cursor.advance().await? {
            let doc: Document = cursor.deserialize_current()?;
            if let Some(data) = doc.get("data") {
                if let Ok(e) = decode::<GraphExecution>(data.clone()) {
                    out.push(e);
                }
            }
        }
        Ok(out)
    }

    async fn save_node_execution(&self, ne: &NodeExecution) -> Result<(), Error> {
        let id = format!("{}:{}", ne.execution_id, ne.node);
        let update = doc! { "$set": { "data": encode(ne)? } };
        self.node_executions
            .update_one(doc! { "_id": &id }, update)
            .upsert(true)
            .await?;
        Ok(())
    }

    async fn get_node_execution(
        &self,
        execution_id: &str,
        node: &str,
    ) -> Result<Option<NodeExecution>, Error> {
        let id = format!("{execution_id}:{node}");
        let Some(doc) = self.node_executions.find_one(doc! { "_id": &id }).await? else {
            return Ok(None);
        };
        let Some(data) = doc.get("data") else {
            return Ok(None);
        };
        Ok(Some(decode(data.clone())?))
    }

    async fn list_node_executions(&self, execution_id: &str) -> Result<Vec<NodeExecution>, Error> {
        let mut cursor = self
            .node_executions
            .find(doc! { "data.executionId": execution_id })
            .await?;
        let mut out = Vec::new();
        while cursor.advance().await? {
            let doc: Document = cursor.deserialize_current()?;
            if let Some(data) = doc.get("data") {
                if let Ok(e) = decode::<NodeExecution>(data.clone()) {
                    out.push(e);
                }
            }
        }
        out.sort_by_key(|ne| ne.started_at);
        Ok(out)
    }
}
