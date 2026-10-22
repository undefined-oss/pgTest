use deadpool_postgres::{Client, Pool, PoolError};
#[cfg(any(test, feature = "test-support"))]
use futures_util::{StreamExt, stream::FuturesUnordered};
use pgtest_engine_backend::{BackendError, DatabaseCleaner, ResourceId};

use crate::{
    config::PostgresConfig,
    connection::{connect_pool, execute_ddl},
    database_name::PostgresDatabaseName,
    errors::{PostgresClientError, PostgresOperationsError},
    sql_profile,
};
#[cfg(any(test, feature = "test-support"))]
pub(crate) const CLEANUP_PIPELINE_DEPTH: usize = 32;
pub struct CleanupClient {
    pub(crate) pool: Pool,
}
#[hotpath::measure_all]
impl CleanupClient {
    pub async fn connect(config: &PostgresConfig) -> Result<Self, PostgresClientError> {
        Ok(Self {
            pool: connect_pool(config, config.pgtest_pg_cleanup_pool_connection.into(), "cleanup")
                .await?,
        })
    }

    pub async fn drop_ddl_database(
        &self,
        database_name: &str,
    ) -> Result<(), PostgresOperationsError> {
        let client = self
            .acquire_drop_connection()
            .await
            .map_err(|error| Self::drop_error(database_name, error))?;
        Self::drop_on_connection(&client, database_name).await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn drop_ddl_databases(
        &self,
        database_names: &[&str],
        mut on_result: impl FnMut(usize, Result<(), PostgresOperationsError>) + Send,
    ) -> Result<(), PostgresOperationsError> {
        if database_names.is_empty() {
            return Ok(());
        }
        let client = self
            .acquire_drop_connection()
            .await
            .map_err(|error| Self::drop_error("cleanup batch", error))?;
        let mut names = database_names.iter().enumerate();
        let mut drops = FuturesUnordered::new();
        for (index, name) in names.by_ref().take(CLEANUP_PIPELINE_DEPTH) {
            drops.push(Self::drop_indexed(&client, index, name));
        }
        while let Some((index, result)) = drops.next().await {
            on_result(index, result);
            if let Some((index, name)) = names.next() {
                drops.push(Self::drop_indexed(&client, index, name));
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn drop_indexed(
        client: &tokio_postgres::Client,
        index: usize,
        database_name: &str,
    ) -> (usize, Result<(), PostgresOperationsError>) {
        (index, Self::drop_on_connection(client, database_name).await)
    }

    fn drop_error(database_name: &str, error: PoolError) -> PostgresOperationsError {
        tracing::warn!(database_name, %error, "PostgreSQL DROP DATABASE failed");
        PostgresOperationsError::UnableToDropDatabase {
            database_name: database_name.to_owned(),
            source: error,
        }
    }

    pub(crate) async fn drop_on_connection(
        client: &tokio_postgres::Client,
        database_name: &str,
    ) -> Result<(), PostgresOperationsError> {
        let quoted = PostgresDatabaseName::quote_ident(database_name);
        let query = format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)");
        execute_ddl(client, &query, sql_profile::DROP_DATABASE)
            .await
            .map_err(|error| Self::drop_error(database_name, error.into()))?;
        Ok(())
    }

    pub(crate) async fn acquire_drop_connection(&self) -> Result<Client, PoolError> {
        self.pool.get().await
    }
}
impl Drop for CleanupClient {
    fn drop(&mut self) {
        self.pool.close();
    }
}
impl DatabaseCleaner for CleanupClient {
    async fn delete_database(&self, resource: ResourceId) -> Result<(), BackendError> {
        self.drop_ddl_database(&resource.0)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::{CLEANUP_PIPELINE_DEPTH, CleanupClient};
    use crate::{
        config::PostgresConfig,
        connection::execute_ddl,
        database_name::PostgresDatabaseName,
        errors::PostgresOperationsError,
        testcontainer::{TestClients, pg_container_config},
    };

    #[tokio::test]
    async fn pipeline_failure_does_not_skip_later_commands_or_poison_connection() {
        let clients = TestClients::start(pg_container_config().await).await.unwrap();
        let database = clients.creation.create_ddl_database().await.unwrap();
        let client = clients.cleanup.acquire_drop_connection().await.unwrap();
        let (failure, success) = tokio::join!(
            execute_ddl(&client, "SELECT 1 / 0", "SELECT 1 / 0"),
            CleanupClient::drop_on_connection(&client, &database),
        );
        assert_eq!(failure.unwrap_err().code().unwrap().code(), "22012");
        success.expect("a separate Sync must allow the next DROP to succeed");
        client.simple_query("SELECT 1").await.expect("connection must remain usable");
    }

    #[tokio::test]
    async fn cleanup_batch_reports_every_result_and_drains_failures_on_one_connection() {
        let clients = TestClients::start(PostgresConfig {
            pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::MIN.into(),
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let client = clients.creation.acquire_create_connection().await.unwrap();
        let names: Vec<_> = (0..=CLEANUP_PIPELINE_DEPTH)
            .map(|index| {
                format!(
                    "{}_Batch \"{index}",
                    clients.creation.template_database_name.template_name()
                )
            })
            .collect();
        for name in &names {
            execute_ddl(
                &client,
                &format!("CREATE DATABASE {}", PostgresDatabaseName::quote_ident(name)),
                "CREATE DATABASE \"<database>\"",
            )
            .await
            .unwrap();
        }
        // Both errors are safe: PostgreSQL refuses to drop a template database
        // or the database to which this cleanup connection is connected.
        let mut batch = vec!["template0", "postgres"];
        batch.extend(names.iter().map(String::as_str));
        let mut results = std::collections::BTreeMap::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            clients.cleanup.drop_ddl_databases(&batch, |index, result| {
                assert!(results.insert(index, result).is_none(), "duplicate result");
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(results.len(), batch.len());
        assert!(matches!(
            results.remove(&0).unwrap(),
            Err(crate::errors::PostgresOperationsError::UnableToDropDatabase { .. })
        ));
        assert!(matches!(
            results.remove(&1).unwrap(),
            Err(crate::errors::PostgresOperationsError::UnableToDropDatabase { .. })
        ));
        assert!(results.into_values().all(|result| result.is_ok()));
        let cleanup = clients.cleanup.acquire_drop_connection().await.unwrap();
        let row = cleanup
            .query_one("SELECT count(*) FROM pg_database WHERE datname::text = ANY($1)", &[&names])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        let template = clients.creation.template_database_name.template_name();
        let exists = cleanup
            .query_one("SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)", &[&template])
            .await
            .unwrap();
        assert!(exists.get::<_, bool>(0), "cleanup must preserve the template");
        drop(cleanup);

        clients.cleanup.pool.close();
        clients
            .cleanup
            .drop_ddl_databases(&[], |_, _| panic!("empty batch must not report results"))
            .await
            .unwrap();
        assert!(matches!(
            clients
                .cleanup
                .drop_ddl_databases(&["unused"], |_, _| panic!("no drops were submitted"))
                .await,
            Err(crate::errors::PostgresOperationsError::UnableToDropDatabase { .. })
        ));
    }

    #[tokio::test]
    async fn closed_pool_reports_cleanup_error() {
        let clients = TestClients::start(pg_container_config().await).await.unwrap();
        clients.cleanup.pool.close();
        assert!(matches!(
            clients.cleanup.drop_ddl_database("db").await,
            Err(PostgresOperationsError::UnableToDropDatabase {
                source: deadpool_postgres::PoolError::Closed,
                ..
            })
        ));
    }
}
