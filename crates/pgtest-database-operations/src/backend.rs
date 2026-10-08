//! PostgreSQL composition data and provider implementation.
use std::{num::NonZeroUsize, sync::Arc};

use pgtest_engine_backend::{
    AsyncDatabaseBackend, BackendError, PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId,
};

use crate::manager::{PostgresManager, config::PostgresConfig, errors::PostgresClientError};

pub struct PreparedPostgres {
    pub backend: Arc<PostgresManager>,
    pub template: String,
    pub creation_concurrency: NonZeroUsize,
    pub cleanup_concurrency: NonZeroUsize,
    pub stale_resources: Vec<ResourceId>,
}
impl PreparedPostgres {
    pub async fn prepare(config: PostgresConfig) -> Result<Self, PostgresClientError> {
        let template = config.pgtest_pg_database.to_string();
        let creation_concurrency = config.pgtest_pg_creation_pool_connection.into();
        let cleanup_concurrency = config.pgtest_pg_cleanup_pool_connection.into();
        let backend = Arc::new(PostgresManager::start(config).await?);
        let stale_resources = match backend.discover_stale_databases().await {
            Ok(names) => names.into_iter().map(ResourceId).collect(),
            Err(error) => {
                tracing::warn!(%error, "startup discovery failed; continuing startup");
                Vec::new()
            }
        };
        Ok(Self { backend, template, creation_concurrency, cleanup_concurrency, stale_resources })
    }
}
impl AsyncDatabaseBackend for PostgresManager {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        let name = self
            .create_ddl_database()
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;
        let endpoint = if self.host.starts_with('/') {
            PgEndpoint::Unix { directory: self.host.clone().into(), port: *self.port }
        } else {
            PgEndpoint::Tcp { host: self.host.clone(), port: *self.port }
        };
        Ok(ProvisionedDatabase {
            resource_id: ResourceId(name.to_string()),
            target: PgTarget { endpoint, database: name.to_string() },
        })
    }

    async fn delete_database(&self, resource: ResourceId) -> Result<(), BackendError> {
        self.drop_ddl_database(&resource.0)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }
}
