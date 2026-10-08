//! Provider-neutral database lifecycle contract. No executor dependency.
use std::{future::Future, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResourceId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgEndpoint {
    Tcp { host: String, port: u16 },
    Unix { directory: PathBuf, port: u16 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgTarget {
    pub endpoint: PgEndpoint,
    pub database: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionedDatabase {
    pub resource_id: ResourceId,
    pub target: PgTarget,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    #[error("{0}")]
    OperationFailed(String),
}

/// Each call makes one attempt. Success means the database is ready to connect.
/// Dropping a future does not guarantee cancellation of remote side effects.
pub trait AsyncDatabaseBackend: Send + Sync + 'static {
    fn create_database(
        &self,
    ) -> impl Future<Output = Result<ProvisionedDatabase, BackendError>> + Send;
    fn delete_database(
        &self,
        resource: ResourceId,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;
}
