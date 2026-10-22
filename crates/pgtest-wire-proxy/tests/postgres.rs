//! PostgreSQL authentication and Unix transport regression tests (requires
//! Docker).
use std::num::NonZeroUsize;

use pgtest::config::PoolConfig;
use pgtest_database_operations::{backend::PreparedPostgres, testcontainer::pg_container_config};

use crate::{Listeners, test_support::TestServer as ProxyServer};
#[tokio::test]
async fn upstream_scram_authentication_and_errors_reach_the_client() {
    use pgtest_database_operations::{config::PostgresConfig, testcontainer::Postgres};
    use testcontainers::{ImageExt, runners::AsyncRunner};

    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let postgres = Postgres::default()
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "scram-sha-256")
            .start()
            .await
            .unwrap();
        let config = PostgresConfig {
            pgtest_pg_host: postgres.get_host().await.unwrap().to_string().parse().unwrap(),
            pgtest_pg_port: postgres.get_host_port_ipv4(5432).await.unwrap().try_into().unwrap(),
            ..PostgresConfig::default()
        };
        let template = config.pgtest_pg_database.clone();
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        let prepared = PreparedPostgres::prepare(config).await.unwrap();
        let pool = PoolConfig::default();
        let initial_databases =
            prepared.create_initial_databases(usize::from(*pool.initial_slots)).await.unwrap();
        let server = ProxyServer::start_async(
            prepared,
            pool,
            initial_databases,
            Listeners::new(tcp),
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
        let mut client_config = tokio_postgres::Config::new();
        client_config
            .host("127.0.0.1")
            .port(address.port())
            .user("postgres")
            .password("incorrect")
            .dbname(&format!("{template}/scram"));
        let error = match client_config.connect(tokio_postgres::NoTls).await {
            Err(error) => error,
            Ok(_) => panic!("incorrect password was accepted"),
        };
        assert_eq!(error.as_db_error().unwrap().code().code(), "28P01");

        client_config.password("postgres");
        let (client, connection) = client_config.connect(tokio_postgres::NoTls).await.unwrap();
        let driver = tokio::spawn(connection);
        let database: String =
            client.query_one("SELECT current_database()", &[]).await.unwrap().get(0);
        assert!(database.starts_with(&format!("{template}_")));
        let (control, control_driver) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(address.port())
            .user("postgres")
            .dbname("pgtest")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let control_driver = tokio::spawn(control_driver);
        assert!(
            control
                .query_one("SELECT pgtest_release($1)", &[&"scram"])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        drop(control);
        control_driver.await.unwrap().unwrap();
        let _ = driver.await.unwrap();
        assert!(client.is_closed());
        drop(client);
        server.shutdown_async().await.unwrap();
    })
    .await
    .expect("SCRAM startup and relay must finish");
}

#[cfg(unix)]
#[tokio::test]
async fn unix_application_and_control_connections_share_lease_state() {
    tokio::time::timeout(std::time::Duration::from_secs(30), unix_lease_flow(false)).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_upstream_supports_database_pools_and_leased_sessions() {
    tokio::time::timeout(std::time::Duration::from_secs(30), unix_lease_flow(true)).await.unwrap();
}

#[cfg(unix)]
async fn unix_lease_flow(use_unix_upstream: bool) {
    let directory = tempfile::Builder::new().prefix("pgi-").tempdir_in("/tmp").unwrap();
    let mut pg_config = pg_container_config().await;
    let mut bridge_tasks = tokio::task::JoinSet::new();
    if use_unix_upstream {
        let upstream_directory = directory.path().join("upstream");
        std::fs::create_dir(&upstream_directory).unwrap();
        let listener = tokio::net::UnixListener::bind(
            upstream_directory.join(format!(".s.PGSQL.{}", pg_config.pgtest_pg_port.port())),
        )
        .unwrap();
        let host = pg_config.pgtest_pg_host.clone();
        let port = pg_config.pgtest_pg_port.port();
        bridge_tasks.spawn(async move {
                let mut sessions = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (mut unix_stream, _) = accepted.unwrap();
                            let host = host.clone();
                            sessions.spawn(async move {
                                let mut tcp_stream = tokio::net::TcpStream::connect((host.as_str(), port)).await.unwrap();
                                let _ = tokio::io::copy_bidirectional(&mut unix_stream, &mut tcp_stream).await;
                            });
                        }
                        Some(result) = sessions.join_next(), if !sessions.is_empty() => { result.unwrap(); }
                    }
                }
            });
        pg_config.pgtest_pg_host = upstream_directory.to_str().unwrap().parse().unwrap();
    }
    let template = pg_config.pgtest_pg_database.clone();
    let port = if use_unix_upstream { 7432 } else { 6432 };
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listeners = Listeners::new(tcp)
        .bind_unix(directory.path(), std::num::NonZeroU16::new(port).unwrap())
        .unwrap();
    let socket_path = listeners.unix_path().unwrap().to_owned();
    let prepared = PreparedPostgres::prepare(pg_config).await.unwrap();
    let pool = PoolConfig::default();
    let initial_databases =
        prepared.create_initial_databases(usize::from(*pool.initial_slots)).await.unwrap();
    let server = ProxyServer::start_async(
        prepared,
        pool,
        initial_databases,
        listeners,
        NonZeroUsize::new(2).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(socket_path.file_name().unwrap(), format!(".s.PGSQL.{port}").as_str());
    let mut config = tokio_postgres::Config::new();
    config.host_path(&directory.path()).port(port).user("postgres");
    let mut control_config = config.clone();
    control_config.dbname("pgtest");
    config.dbname(&format!("{template}/unix-test"));

    let (application, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let application_task = tokio::spawn(connection);
    let row = application.query_one("SELECT current_database(), 42::int4", &[]).await.unwrap();
    let database: String = row.get(0);
    assert!(database.starts_with(&format!("{template}_")));
    assert_eq!(row.get::<_, i32>(1), 42);

    let (control, connection) = control_config.connect(tokio_postgres::NoTls).await.unwrap();
    let control_task = tokio::spawn(connection);
    assert_eq!(control.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
    drop(application);
    application_task.await.unwrap().unwrap();
    let row = control.query_one("SELECT pgtest_release($1::text)", &[&"unix-test"]).await.unwrap();
    assert!(row.get::<_, bool>(0));
    // A released lease ID can be reused, but its database must be fresh.
    let (reconnected, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let reconnected_task = tokio::spawn(connection);
    let replacement: String =
        reconnected.query_one("SELECT current_database()", &[]).await.unwrap().get(0);
    assert_ne!(replacement, database);
    assert!(replacement.starts_with(&format!("{template}_")));
    drop(reconnected);
    reconnected_task.await.unwrap().unwrap();
    drop(control);
    control_task.await.unwrap().unwrap();

    server.shutdown_async().await.unwrap();
    assert!(!socket_path.exists());
    bridge_tasks.shutdown().await;
}
