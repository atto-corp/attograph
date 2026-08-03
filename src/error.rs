use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("duplicate node tag: {0}")]
    DuplicateNode(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bullmq error: {0}")]
    BullMq(#[from] bullmq::Error),
    #[error("storage error: {0}")]
    Storage(String),
    #[cfg(feature = "mongo")]
    #[error("mongodb error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("internal error: {0}")]
    Internal(String),
}
