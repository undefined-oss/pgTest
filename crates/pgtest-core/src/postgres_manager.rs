use pgtest_database_operations::manager::PostgresManager;
use pgtest_utils::read_string::ReadString;

use crate::worker_engine::{errors::PostgresDDLClientError, traits::PostgresClient};

#[hotpath::measure_all]
impl PostgresClient for PostgresManager {
    async fn create_databases(
        &self,
        amount: usize,
        mut on_finished: impl FnMut(usize, Result<ReadString, PostgresDDLClientError>) + Send,
    ) {
        self.create_ddl_databases(amount, |index, result| {
            on_finished(index, result.map_err(Into::into));
        })
        .await;
    }

    async fn drop_databases(
        &self,
        database_names: &[ReadString],
        mut on_finished: impl FnMut(usize, Result<(), PostgresDDLClientError>) + Send,
    ) {
        let names: Vec<&str> = database_names.iter().map(|name| &**name).collect();
        let result = self
            .drop_ddl_databases(&names, |index, result| {
                on_finished(index, result.map_err(Into::into));
            })
            .await;
        // Connection acquisition failed before any per-database result existed.
        if let Err(error) = result {
            for (index, name) in database_names.iter().enumerate() {
                on_finished(
                    index,
                    Err(PostgresDDLClientError::OperationFailed(format!(
                        "unable to drop database {name}: {error}"
                    ))),
                );
            }
        }
    }

    async fn drop_database(&self, database_name: &str) -> Result<(), PostgresDDLClientError> {
        self.drop_ddl_database(database_name).await.map_err(Into::into)
    }

    async fn create_database(&self) -> Result<ReadString, PostgresDDLClientError> {
        self.create_ddl_database().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pgtest_database_operations::{
        manager::config::PostgresConfig, testcontainer::pg_container_config,
    };

    use super::*;

    #[tokio::test]
    async fn batch_reports_each_success_and_failure_once() {
        let manager = PostgresManager::start(PostgresConfig {
            pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::MIN,
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let database = manager.create_database().await.unwrap();
        let names = [ReadString::from("postgres"), database, ReadString::from("template0")];
        let mut results = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(10),
            manager.drop_databases(&names, |index, result| results.push((index, result))),
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().any(|result| matches!(result, (1, Ok(())))));
        for index in [0, 2] {
            assert!(results.iter().any(|result| matches!(
                result,
                (i, Err(PostgresDDLClientError::OperationFailed(_))) if *i == index
            )));
        }
        // The failed DROP must not poison the connection or future batches.
        let next = manager.create_database().await.unwrap();
        let mut completed = false;
        manager
            .drop_databases(&[next], |index, result| {
                assert_eq!(index, 0);
                result.unwrap();
                assert!(!completed);
                completed = true;
            })
            .await;
        assert!(completed);
    }
}
