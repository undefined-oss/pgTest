//! Integration coverage spanning creation and cleanup workers.
use crate::{
    backend::bootstrap,
    cleanup_client::CleanupClient,
    config::PostgresConfig,
    creation_client::CreationClient,
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
async fn clients_reject_unavailable_backend() {
    let mut config = pg_container_config().await;
    let metadata = bootstrap(&config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    config.pgtest_pg_host = "127.0.0.1".parse().unwrap();
    config.pgtest_pg_port = port.try_into().unwrap();
    assert!(CreationClient::connect(&config, metadata).await.is_err());
    assert!(CleanupClient::connect(&config).await.is_err());
}
