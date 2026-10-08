//! Real PostgreSQL tests at the backend/runtime composition boundary.
use std::{collections::HashSet, num::NonZeroUsize};

use pgtest::{
    worker_engine::core::{LeaseId, WorkerEngineConfig},
    worker_manager::{RuntimeConfig, StartError, TokioRuntime},
};
use pgtest_database_operations::{
    backend::PreparedPostgres,
    manager::{PostgresManager, config::PostgresConfig},
    testcontainer::pg_container_config,
};

fn engine(initial: u16) -> WorkerEngineConfig {
    WorkerEngineConfig {
        initial_slots: initial.into(),
        grow_batch_size: 0.into(),
        lease_claim_timeout_ms: 0,
        ..WorkerEngineConfig::default()
    }
}
fn runtime_config(prepared: &PreparedPostgres, engine: WorkerEngineConfig) -> RuntimeConfig {
    RuntimeConfig {
        template: prepared.template.clone(),
        engine,
        creation_concurrency: prepared.creation_concurrency,
        cleanup_concurrency: prepared.cleanup_concurrency,
        stale_resources: prepared.stale_resources.clone(),
    }
}
#[tokio::test]
async fn startup_prefills_large_batch_using_one_administrative_connection() {
    let mut config = pg_container_config().await;
    config.pgtest_pg_creation_pool_connection = NonZeroUsize::MIN.into();
    let prepared = PreparedPostgres::prepare(config).await.unwrap();
    let template = prepared.template.clone();
    let config = runtime_config(&prepared, engine(33));
    let runtime = TokioRuntime::start(prepared.backend, config).await.unwrap();
    let handle = runtime.handle();
    let mut targets = HashSet::new();
    for index in 0..33 {
        let session =
            handle.attach(&template, LeaseId::new(index.to_string()).unwrap()).await.unwrap();
        assert!(targets.insert(session.target.database.clone()));
    }
    assert_eq!(targets.len(), 33);
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn initial_creation_failure_returns_error_without_panicking() {
    let prepared = PreparedPostgres::prepare(pg_container_config().await).await.unwrap();
    prepared.backend.drop_ddl_database(&prepared.template).await.unwrap();
    let config = runtime_config(&prepared, engine(2));
    let result = TokioRuntime::start(prepared.backend, config).await;
    assert!(matches!(result, Err(StartError::InitialDatabaseCreation(_))));
}
#[tokio::test]
async fn startup_reconciliation_preserves_another_managers_active_resource() {
    let first = PreparedPostgres::prepare(pg_container_config().await).await.unwrap();
    let template = first.template.clone();
    let config = runtime_config(&first, engine(1));
    let first_runtime = TokioRuntime::start(first.backend.clone(), config).await.unwrap();
    let session =
        first_runtime.handle().attach(&template, LeaseId::new("active").unwrap()).await.unwrap();
    let second_config = pg_container_config().await;
    // Simulate a resource left behind before the next runtime starts.
    let stale_backend = PostgresManager::start(PostgresConfig {
        pgtest_pg_database: second_config.pgtest_pg_database.clone(),
        pgtest_pg_host: second_config.pgtest_pg_host.clone(),
        pgtest_pg_port: second_config.pgtest_pg_port,
        pgtest_pg_user: second_config.pgtest_pg_user.clone(),
        ..PostgresConfig::default()
    })
    .await
    .unwrap();
    let stale = stale_backend.create_ddl_database().await.unwrap();
    let second = PreparedPostgres::prepare(second_config).await.unwrap();
    assert!(second.stale_resources.iter().any(|r| r.0 == stale.as_ref()));
    let config = runtime_config(&second, engine(0));
    let second_runtime = TokioRuntime::start(second.backend, config).await.unwrap();
    let mut options = tokio_postgres::Config::new();
    options.host(&first.backend.host).port(*first.backend.port).user("postgres").dbname("postgres");
    let (client, connection) = options.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    let names: Vec<String> = client
        .query("SELECT datname FROM pg_database", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(names.contains(&session.target.database));
    assert!(!names.iter().any(|name| name == stale.as_ref()));
    drop(client);
    connection.await.unwrap().unwrap();
    first_runtime.shutdown().await.unwrap();
    second_runtime.shutdown().await.unwrap();
}
#[test]
fn provider_preparation_future_fits_stack_budget() {
    let future = PreparedPostgres::prepare(PostgresConfig::default());
    assert!(
        std::mem::size_of_val(&future) < 64 * 1024,
        "preparation future uses {} bytes",
        std::mem::size_of_val(&future)
    );
}
