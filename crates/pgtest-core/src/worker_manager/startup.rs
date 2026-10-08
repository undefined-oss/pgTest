use std::sync::Arc;

use pgtest_database_operations::manager::{
    PostgresManager, config::PostgresConfig, errors::PostgresClientError,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::{
    WorkerEngineManager, WorkerEngineType,
    database_cleanup_worker::DatabaseCleanupWorker,
    database_creation_worker::DatabaseCreationWorker,
    worker_io::{DatabaseWorkerSenders, WorkerEngineIO, WorkerEngineInbox},
};
use crate::worker_engine::{core::WorkerEngineConfig, errors::PostgresDDLClientError};

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("failed to initialize PostgreSQL: {0}")]
    Postgres(#[from] PostgresClientError),
    #[error("failed to create the initial databases: {0}")]
    InitialDatabaseCreation(#[from] PostgresDDLClientError),
}

pub(super) async fn prepare_postgres(
    config: PostgresConfig,
) -> Result<Arc<PostgresManager>, StartError> {
    let client = Arc::new(PostgresManager::start(config).await?);

    // Keep the large, instrumented cleanup future off the startup future's
    // inline state to avoid overflowing the stack with profiling enabled.
    if let Err(error) = Box::pin(client.drop_ddl_templates_like()).await {
        tracing::warn!(?error, "startup cleanup failed; continuing startup");
    }
    Ok(client)
}

pub(super) async fn start_workers(
    postgres_client: Arc<PostgresManager>,
    worker_engine_config: WorkerEngineConfig,
) -> Result<WorkerEngineManager, StartError> {
    let (inbox_tx, inbox_rx) =
        hotpath::channel!(tokio::sync::mpsc::unbounded_channel(), label = "worker-inbox");

    let timeout_claim = worker_engine_config.lease_claim_timeout_ms;

    let tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();

    let (database_worker_senders, creation_rx, cleanup_rx) =
        DatabaseWorkerSenders::init_database_worker_channels();

    let creation_worker = DatabaseCreationWorker::new(
        inbox_tx.clone(),
        tracker.clone(),
        shutdown_token.clone(),
        postgres_client.clone(),
        creation_rx,
    );

    let cleanup_worker = DatabaseCleanupWorker::new(
        inbox_tx.clone(),
        tracker.clone(),
        shutdown_token.clone(),
        postgres_client.clone(),
        cleanup_rx,
    );

    let worker_engine_inbox = WorkerEngineInbox::new(inbox_rx);

    let worker_engine_io = WorkerEngineIO::new(
        inbox_tx.clone(),
        tracker.clone(),
        shutdown_token.clone(),
        database_worker_senders,
    );

    let mut worker_engine = WorkerEngineType::new(
        worker_engine_config,
        postgres_client.clone(),
        worker_engine_io,
        worker_engine_inbox,
    );

    // Preserve the stack bound when engine initialization is instrumented.
    Box::pin(worker_engine.try_init()).await?;

    tracker.spawn(creation_worker.run());
    tracker.spawn(cleanup_worker.run());

    let engine_handle = tokio::spawn(async move {
        worker_engine.run().await;
        worker_engine
    });

    Ok(WorkerEngineManager {
        worker_inbox_tx: inbox_tx,
        pg_client: postgres_client,
        lease_claim_timeout: timeout_claim,
        engine_handle,
        tracker,
        shutdown_token,
    })
}
