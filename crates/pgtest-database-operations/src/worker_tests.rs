//! Integration coverage spanning creation and cleanup workers.
use pgtest_engine_backend::jobs::DatabaseWorkerMessages;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    backend::bootstrap,
    cleanup_worker_handle::CleanupHandle,
    config::PostgresConfig,
    creation_worker_handle::CreationHandle,
    testcontainer::{TestClients, pg_container_config},
};

#[tokio::test]
async fn cleanup_and_creation_have_independent_connection_capacity() {
    let config = PostgresConfig {
        pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::MIN.into(),
        pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::MIN.into(),
        ..pg_container_config().await
    };
    let clients = TestClients::start(config).await.unwrap();

    let cleanup_connection = clients.cleanup.acquire_drop_connection().await.unwrap();
    let creation_connection = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        clients.creation.acquire_create_connection(),
    )
    .await
    .expect("a full cleanup pool must not block creation")
    .unwrap();

    drop(cleanup_connection);
    let cleanup_connection = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        clients.cleanup.acquire_drop_connection(),
    )
    .await
    .expect("a full creation pool must not block cleanup")
    .unwrap();

    drop(creation_connection);
    drop(cleanup_connection);
    clients.creation.pool.close();
    clients.cleanup.pool.close();
}

#[tokio::test]
async fn created_database_can_be_deleted_repeatedly() {
    let clients = TestClients::start(pg_container_config().await).await.unwrap();
    let database = clients.creation.create_ddl_database().await.unwrap();
    clients.cleanup.drop_ddl_database(&database).await.unwrap();
    clients.cleanup.drop_ddl_database(&database).await.unwrap();
}

#[tokio::test]
async fn handles_validate_their_own_clients_before_spawning() {
    let mut config = pg_container_config().await;
    let metadata = bootstrap(&config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    config.pgtest_pg_host = "127.0.0.1".parse().unwrap();
    config.pgtest_pg_port = port.try_into().unwrap();
    let (events, mut receiver) = tokio::sync::mpsc::unbounded_channel::<DatabaseWorkerMessages>();
    let shutdown = CancellationToken::new();
    let tracker = TaskTracker::new();
    assert!(
        CreationHandle::new(&config, metadata, events.clone(), tracker.clone(), shutdown.clone())
            .await
            .is_err()
    );
    assert!(CleanupHandle::new(&config, events.clone(), tracker.clone(), shutdown).await.is_err());
    drop(events);
    assert!(receiver.recv().await.is_none());
}
