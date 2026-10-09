//! Real PostgreSQL tests at the backend/runtime composition boundary.
use std::{collections::HashSet, num::NonZeroUsize};

use pgtest::{
    config::ManagerConfig,
    manager_handle::LeaseId,
    runtime::{StartError, TokioRuntime},
};
use pgtest_database_operations::{
    backend::{PreparedPostgres, bootstrap},
    config::PostgresConfig,
    creation_worker_handle::CreationClient,
    testcontainer::pg_container_config,
};

fn engine(initial: u16) -> ManagerConfig {
    ManagerConfig {
        initial_slots: initial.into(),
        grow_batch_size: 0.into(),
        lease_claim_timeout_ms: 0,
        ..ManagerConfig::default()
    }
}
#[tokio::test]
async fn startup_prefills_large_batch_using_one_administrative_connection() {
    let mut config = pg_container_config().await;
    config.pgtest_pg_creation_pool_connection = NonZeroUsize::MIN.into();
    let runtime = TokioRuntime::start(config, engine(33)).await.unwrap();
    let handle = runtime.handle();
    let mut targets = HashSet::new();
    for index in 0..33 {
        let session = handle.attach(LeaseId::new(index.to_string()).unwrap()).await.unwrap();
        assert!(targets.insert(session.target.database.clone()));
    }
    assert_eq!(targets.len(), 33);
    runtime.shutdown().await;
}
#[tokio::test]
async fn initial_creation_failure_returns_error_without_panicking() {
    let mut config = pg_container_config().await;
    let (setup, driver) = admin(&config).await;
    let role = format!("{}_nocreate", config.pgtest_pg_database);
    setup
        .execute_typed(
            &format!("CREATE ROLE {} LOGIN NOCREATEDB PASSWORD 'postgres'", quote(&role)),
            &[],
        )
        .await
        .unwrap();
    config.pgtest_pg_user = role.parse().unwrap();
    let result = TokioRuntime::start(config, engine(2)).await;
    assert!(matches!(result, Err(StartError::InitialDatabaseCreation(_))));
    wait_role_disconnected(&setup, &role).await;
    setup.execute_typed(&format!("DROP ROLE {}", quote(&role)), &[]).await.unwrap();
    drop(setup);
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn startup_reconciliation_preserves_another_managers_active_resource() {
    let first_config = pg_container_config().await;
    let first_runtime = TokioRuntime::start(first_config.clone(), engine(1)).await.unwrap();
    let session = first_runtime.handle().attach(LeaseId::new("active").unwrap()).await.unwrap();
    let second_config = pg_container_config().await;
    // Simulate a resource left behind before the next runtime starts.
    let metadata = bootstrap(&second_config).await.unwrap();
    let stale_backend = CreationClient::connect(&second_config, metadata).await.unwrap();
    let stale = stale_backend.create_ddl_database().await.unwrap();
    drop(stale_backend);
    let second_runtime = TokioRuntime::start(second_config, engine(0)).await.unwrap();
    let mut options = tokio_postgres::Config::new();
    options
        .host(first_config.pgtest_pg_host.as_str())
        .port(*first_config.pgtest_pg_port)
        .user("postgres")
        .dbname("postgres");
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
    first_runtime.shutdown().await;
    second_runtime.shutdown().await;
}
#[test]
fn postgres_preparation_and_runtime_startup_fit_stack_budget() {
    let future = TokioRuntime::start(PostgresConfig::default(), engine(16));
    assert!(
        std::mem::size_of_val(&future) < 64 * 1024,
        "PostgreSQL runtime startup future uses {} bytes",
        std::mem::size_of_val(&future)
    );
}

#[tokio::test]
async fn worker_initialization_failure_releases_worker_connections() {
    let mut config = pg_container_config().await;
    let (setup, driver) = admin(&config).await;
    let role = format!("{}_limited", config.pgtest_pg_database);
    setup
        .execute_typed(
            &format!("CREATE ROLE {} LOGIN CONNECTION LIMIT 1 PASSWORD 'postgres'", quote(&role)),
            &[],
        )
        .await
        .unwrap();
    config.pgtest_pg_user = role.parse().unwrap();
    let prepared = PreparedPostgres::prepare(config.clone()).await.unwrap();
    // Preparation has completed and returned data without opening worker pools.
    assert_eq!(
        setup
            .query_one("SELECT count(*) FROM pg_stat_activity WHERE usename = $1", &[&role],)
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(prepared);
    let result = TokioRuntime::start(config, engine(0)).await;
    assert!(matches!(result, Err(StartError::Postgres(_))));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let count = setup
                .query_one("SELECT count(*) FROM pg_stat_activity WHERE usename = $1", &[&role])
                .await
                .unwrap()
                .get::<_, i64>(0);
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    setup.execute_typed(&format!("DROP ROLE {}", quote(&role)), &[]).await.unwrap();
    drop(setup);
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_worker_initialization_releases_connections_when_cancelled() {
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        sync::mpsc,
        task::JoinSet,
    };
    let mut config = pg_container_config().await;
    let upstream = (config.pgtest_pg_host.to_string(), *config.pgtest_pg_port);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.pgtest_pg_host = "127.0.0.1".parse().unwrap();
    config.pgtest_pg_port = listener.local_addr().unwrap().port().try_into().unwrap();
    let (accepted_tx, mut accepted) = mpsc::unbounded_channel();
    let (closed_tx, mut closed) = mpsc::unbounded_channel();
    let proxy = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        for index in 0..3 {
            let (mut local, _) = listener.accept().await.unwrap();
            let upstream = upstream.clone();
            let closed = closed_tx.clone();
            tasks.spawn(async move {
                if index != 1 {
                    let mut remote =
                        TcpStream::connect((upstream.0.as_str(), upstream.1)).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut local, &mut remote).await;
                } else {
                    // Hold the first worker's handshake. The other worker
                    // must still connect before this handshake completes.
                    let mut buffer = [0; 1024];
                    while local.read(&mut buffer).await.unwrap_or(0) > 0 {}
                }
                closed.send(index).unwrap();
            });
            accepted_tx.send(index).unwrap();
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    });
    let startup = tokio::spawn(TokioRuntime::start(config, engine(0)));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for expected in 0..3 {
            assert_eq!(accepted.recv().await, Some(expected));
        }
    })
    .await
    .unwrap();
    // Bootstrap was closed before either worker connection opened.
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(3), closed.recv()).await.unwrap(),
        Some(0)
    );
    startup.abort();
    let _ = startup.await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut remaining = vec![closed.recv().await.unwrap(), closed.recv().await.unwrap()];
        remaining.sort_unstable();
        assert_eq!(remaining, [1, 2]);
        proxy.await.unwrap();
    })
    .await
    .unwrap();
}

fn quote(name: &str) -> String {
    pgtest_database_operations::database_name::PostgresDatabaseName::quote_ident(name)
}
async fn admin(
    config: &PostgresConfig,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
    let (client, connection) = tokio_postgres::Config::new()
        .host(config.pgtest_pg_host.as_str())
        .port(*config.pgtest_pg_port)
        .user("postgres")
        .password("postgres")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    (client, tokio::spawn(connection))
}
async fn wait_role_disconnected(client: &tokio_postgres::Client, role: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let count: i64 = client
                .query_one("SELECT count(*) FROM pg_stat_activity WHERE usename = $1", &[&role])
                .await
                .unwrap()
                .get(0);
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
