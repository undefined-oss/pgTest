//! Handle for the creation worker.

pub mod creation_pg_client;
mod creation_worker;

use std::{num::NonZeroUsize, sync::Arc};

pub use creation_pg_client::CreationClient;
use creation_worker::CreationActor;
use pgtest_engine_backend::{
    DatabaseCreator, WorkerUnavailable,
    jobs::{CreateDatabases, DatabaseWorkerMessages},
    workers::CreationState,
};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{backend::PostgresMetadata, config::PostgresConfig, errors::PostgresClientError};

#[derive(Clone)]
pub struct CreationHandle {
    sender: mpsc::UnboundedSender<CreateDatabases>,
}
impl CreationHandle {
    pub async fn new<ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static>(
        config: &PostgresConfig,
        metadata: PostgresMetadata,
        manager: mpsc::UnboundedSender<ManagerMessage>,
        tracker: TaskTracker,
        shutdown: CancellationToken,
    ) -> Result<Self, PostgresClientError> {
        let client = CreationClient::connect(config, metadata).await?;
        Ok(Self::spawn(
            Arc::new(client),
            config.pgtest_pg_creation_pool_connection.into(),
            manager,
            tracker,
            shutdown,
        ))
    }

    fn spawn<
        DatabaseClient: DatabaseCreator,
        ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static,
    >(
        client: Arc<DatabaseClient>,
        limit: NonZeroUsize,
        manager: mpsc::UnboundedSender<ManagerMessage>,
        tracker: TaskTracker,
        shutdown: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let actor =
            CreationActor { client, state: CreationState::new(limit), receiver, manager, shutdown };
        tokio::spawn(tracker.track_future(async move { actor.run().await }));
        Self { sender }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_with_client<
        DatabaseClient: DatabaseCreator,
        ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static,
    >(
        client: Arc<DatabaseClient>,
        limit: NonZeroUsize,
        manager: mpsc::UnboundedSender<ManagerMessage>,
        tracker: TaskTracker,
        shutdown: CancellationToken,
    ) -> Self {
        Self::spawn(client, limit, manager, tracker, shutdown)
    }

    pub fn create(&self, request: CreateDatabases) -> Result<(), WorkerUnavailable> {
        self.sender.send(request).map_err(|_| WorkerUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use pgtest_engine_backend::jobs::{CreateDatabases, DatabaseId, DatabaseWorkerMessages};
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    use super::CreationHandle;
    use crate::{backend::bootstrap, testcontainer::pg_container_config};

    #[tokio::test]
    async fn creation_handle_owns_creation_pool_and_reports_per_database() {
        let mut config = pg_container_config().await;
        config.pgtest_pg_creation_pool_connection = std::num::NonZeroUsize::MIN.into();
        let metadata = bootstrap(&config).await.unwrap();
        let (events, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<DatabaseWorkerMessages>();
        let shutdown = CancellationToken::new();
        let tracker = TaskTracker::new();
        let handle =
            CreationHandle::new(&config, metadata, events, tracker.clone(), shutdown.clone())
                .await
                .unwrap();
        handle
            .create(CreateDatabases {
                first_database_id: DatabaseId(7),
                amount: std::num::NonZeroUsize::new(2).unwrap(),
            })
            .unwrap();
        for id in [7, 8] {
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            match result {
                pgtest_engine_backend::jobs::DatabaseWorkerMessages::CreationFinished {
                    database_id,
                    result,
                } => {
                    assert_eq!(database_id, DatabaseId(id));
                    result.unwrap();
                }
                _ => panic!("unexpected deletion"),
            }
        }
        shutdown.cancel();
        tracker.close();
        tracker.wait().await;
        assert!(
            handle
                .create(CreateDatabases {
                    first_database_id: DatabaseId(9),
                    amount: std::num::NonZeroUsize::MIN
                })
                .is_err()
        );
        bootstrap(&config).await.unwrap();
    }
}
