//! Handle for the cleanup worker.

pub mod cleanup_pg_client;
mod cleanup_worker;

use std::{num::NonZeroUsize, sync::Arc};

pub use cleanup_pg_client::CleanupClient;
use cleanup_worker::CleanupActor;
use pgtest_engine_backend::{
    DatabaseCleaner, WorkerUnavailable,
    jobs::{CleanupDatabase, DatabaseWorkerMessages},
    workers::CleanupState,
};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{config::PostgresConfig, errors::PostgresClientError};

#[derive(Clone)]
pub struct CleanupHandle {
    sender: mpsc::UnboundedSender<CleanupDatabase>,
}
impl CleanupHandle {
    pub async fn new<ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static>(
        config: &PostgresConfig,
        manager: mpsc::UnboundedSender<ManagerMessage>,
        tracker: TaskTracker,
        shutdown: CancellationToken,
    ) -> Result<Self, PostgresClientError> {
        let client = CleanupClient::connect(config).await?;
        Ok(Self::spawn(
            Arc::new(client),
            config.pgtest_pg_cleanup_pool_connection.into(),
            manager,
            tracker,
            shutdown,
        ))
    }

    fn spawn<
        DatabaseClient: DatabaseCleaner,
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
            CleanupActor { client, state: CleanupState::new(limit), receiver, manager, shutdown };
        tokio::spawn(tracker.track_future(async move { actor.run().await }));
        Self { sender }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_with_client<
        DatabaseClient: DatabaseCleaner,
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

    pub fn delete(&self, request: CleanupDatabase) -> Result<(), WorkerUnavailable> {
        self.sender.send(request).map_err(|_| WorkerUnavailable)
    }
}
