use std::{net::SocketAddr, sync::Arc};

use pgtest::worker_manager::ManagerHandle;
use thiserror::Error;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::connection::{ClientStream, handle_connection};
#[cfg(unix)]
pub use crate::unix_listener::UnixWireListener;
pub use crate::{
    connection::{ConnectionFieldError, parse_connection_field},
    postgres_upstream::RawBytes,
};

#[derive(Error, Debug)]
pub enum WireError {
    #[error("failed to open wire listener: {0}")]
    Listen(#[from] std::io::Error),
}

const DEFAULT_WIRE_PORT: u16 = 6432;

pub struct TcpWireListener {
    task: Option<JoinHandle<()>>,
    stop: CancellationToken,
    address: SocketAddr,
}

impl TcpWireListener {
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub async fn shutdown(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    fn detach(mut self) -> SocketAddr {
        // Dropping a JoinHandle detaches it; dropping a token does not cancel
        // it.
        self.task.take();
        self.address
    }
}

impl Drop for TcpWireListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            self.stop.cancel();
            task.abort();
        }
    }
}

#[hotpath::measure]
pub async fn run(
    manager: Arc<ManagerHandle>,
    address: SocketAddr,
) -> Result<SocketAddr, WireError> {
    Ok(run_with_handle(manager, address).await?.detach())
}

#[hotpath::measure]
pub async fn run_with_handle(
    manager: Arc<ManagerHandle>,
    address: SocketAddr,
) -> Result<TcpWireListener, WireError> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    let local_address = listener.local_addr()?;

    let mut pg_connection_sessions: JoinSet<()> = JoinSet::new();

    let stop = CancellationToken::new();
    let stopped = stop.clone();
    let task = tokio::spawn(async move {
        tracing::debug!("to wait connection");
        loop {
            tokio::select! {
                biased;
                _ = stopped.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, _peer) = match accepted {
                        Ok(client) => client,
                        Err(error) => {
                            tracing::warn!("Unable to connect, failed due: {}", error);
                            continue;
                        }
                    };

                    pg_connection_sessions.spawn(handle_connection(ClientStream::Tcp(stream), manager.clone()));
                }
                Some(_finished) = pg_connection_sessions.join_next(), if !pg_connection_sessions.is_empty() => {}
            }
        }
        pg_connection_sessions.shutdown().await;
    });

    Ok(TcpWireListener { task: Some(task), stop, address: local_address })
}

#[cfg(unix)]
#[hotpath::measure]
pub async fn run_unix(
    manager: Arc<ManagerHandle>,
    directory: &std::path::Path,
) -> Result<UnixWireListener, WireError> {
    run_unix_on_port(manager, directory, DEFAULT_WIRE_PORT).await
}

/// Listen on `<directory>/.s.PGSQL.<port>` in an existing directory.
#[cfg(unix)]
#[hotpath::measure]
pub async fn run_unix_on_port(
    manager: Arc<ManagerHandle>,
    directory: &std::path::Path,
    port: u16,
) -> Result<UnixWireListener, WireError> {
    let listener = crate::unix_listener::BoundUnixListener::bind(directory, port)?;
    let path = listener.path().to_owned();
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => break,
                Some(_finished) = sessions.join_next(), if !sessions.is_empty() => {}
                accepted = listener.accept() => {
                    match accepted {
                        Ok(stream) => {
                            sessions.spawn(handle_connection(ClientStream::Unix(stream), manager.clone()));
                        }
                        Err(error) => tracing::warn!(%error, "failed to accept Unix connection"),
                    }
                }
            }
        }
        sessions.shutdown().await;
    });
    Ok(UnixWireListener { task: Some(task), stop: Some(stop), path })
}

#[cfg(test)]
mod listener_test {
    use std::sync::Arc;

    use pgtest::worker_engine::core::WorkerEngineConfig;
    use pgtest_database_operations::testcontainer::pg_container_config;
    use tracing_test::traced_test;

    use crate::wire_listener;

    async fn start_runtime(
        config: pgtest_database_operations::manager::config::PostgresConfig,
        engine: WorkerEngineConfig,
    ) -> Result<pgtest::worker_manager::TokioRuntime, Box<dyn std::error::Error>> {
        let prepared =
            pgtest_database_operations::backend::PreparedPostgres::prepare(config).await?;
        Ok(pgtest::worker_manager::TokioRuntime::start(
            prepared.backend,
            pgtest::worker_manager::RuntimeConfig {
                template: prepared.template,
                engine,
                creation_concurrency: prepared.creation_concurrency,
                cleanup_concurrency: prepared.cleanup_concurrency,
                stale_resources: prepared.stale_resources,
            },
        )
        .await?)
    }

    #[tokio::test]
    async fn owned_tcp_shutdown_closes_sessions_and_releases_manager() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let runtime = start_runtime(pg_container_config().await, WorkerEngineConfig::default())
                .await
                .unwrap();
            let engine = Arc::new(runtime.handle());
            let listener =
                wire_listener::run_with_handle(engine.clone(), ([127, 0, 0, 1], 0).into())
                    .await
                    .unwrap();
            let address = listener.local_addr();
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user("postgres")
                .dbname("pgtest")
                .connect(tokio_postgres::NoTls)
                .await
                .unwrap();
            let task = tokio::spawn(connection);
            client.query_one("SELECT 1", &[]).await.unwrap();
            listener.shutdown().await;
            let _ = task.await.unwrap();
            assert!(client.is_closed());
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
            drop(engine);
            runtime.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn dropping_owned_tcp_listener_aborts_its_task() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let runtime = start_runtime(pg_container_config().await, WorkerEngineConfig::default())
                .await
                .unwrap();
            let engine = Arc::new(runtime.handle());
            let listener =
                wire_listener::run_with_handle(engine.clone(), ([127, 0, 0, 1], 0).into())
                    .await
                    .unwrap();
            let address = listener.local_addr();
            drop(listener);
            while Arc::strong_count(&engine) != 1 {
                tokio::task::yield_now().await;
            }
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
            drop(engine);
            runtime.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_application_and_control_connections_share_lease_state() {
        tokio::time::timeout(std::time::Duration::from_secs(30), unix_lease_flow(false))
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_upstream_supports_database_pools_and_leased_sessions() {
        tokio::time::timeout(std::time::Duration::from_secs(30), unix_lease_flow(true))
            .await
            .unwrap();
    }

    #[cfg(unix)]
    async fn unix_lease_flow(use_unix_upstream: bool) {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path =
            std::env::temp_dir().join(format!("pgi-{}-{use_unix_upstream}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let directory = Directory(path);
        let mut pg_config = pg_container_config().await;
        let mut bridge_tasks = tokio::task::JoinSet::new();
        if use_unix_upstream {
            let upstream_directory = directory.0.join("upstream");
            std::fs::create_dir(&upstream_directory).unwrap();
            let listener = crate::unix_listener::BoundUnixListener::bind(
                &upstream_directory,
                pg_config.pgtest_pg_port.port(),
            )
            .unwrap();
            let host = pg_config.pgtest_pg_host.clone();
            let port = pg_config.pgtest_pg_port.port();
            bridge_tasks.spawn(async move {
                let mut sessions = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let mut unix_stream = accepted.unwrap();
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
        let runtime = start_runtime(pg_config, WorkerEngineConfig::default()).await.unwrap();
        let engine = Arc::new(runtime.handle());
        let port = if use_unix_upstream { 7432 } else { 6432 };
        let listener = if use_unix_upstream {
            wire_listener::run_unix_on_port(engine.clone(), &directory.0, port).await.unwrap()
        } else {
            wire_listener::run_unix(engine.clone(), &directory.0).await.unwrap()
        };
        let socket_path = listener.path().to_owned();
        assert_eq!(socket_path.file_name().unwrap(), format!(".s.PGSQL.{port}").as_str());
        let mut config = tokio_postgres::Config::new();
        config.host_path(&directory.0).port(port).user("postgres");
        let mut control_config = config.clone();
        control_config.dbname("pgtest");
        config.dbname(&format!("{template}/unix-test"));

        let (application, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let application_task = tokio::spawn(connection);
        let row = application.query_one("SELECT current_database(), 42::int4", &[]).await.unwrap();
        assert!(row.get::<_, &str>(0).starts_with(&format!("{template}_")));
        assert_eq!(row.get::<_, i32>(1), 42);

        let (control, connection) = control_config.connect(tokio_postgres::NoTls).await.unwrap();
        let control_task = tokio::spawn(connection);
        assert_eq!(control.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
        drop(application);
        application_task.await.unwrap().unwrap();
        let row =
            control.query_one("SELECT pgtest_release($1::text)", &[&"unix-test"]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        assert!(config.connect(tokio_postgres::NoTls).await.is_err());
        drop(control);
        control_task.await.unwrap().unwrap();

        listener.shutdown().await;
        assert!(!socket_path.exists());
        drop(engine);
        runtime.shutdown().await.unwrap();
        bridge_tasks.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    async fn listener_test() {
        let pg_config = pg_container_config().await;
        let template_database = pg_config.pgtest_pg_database.clone();
        let worker_engine_config = WorkerEngineConfig::default();

        let runtime =
            start_runtime(pg_config, worker_engine_config).await.expect("Manager started up");
        let engine = Arc::new(runtime.handle());

        let address = wire_listener::run(engine.clone(), ([0, 0, 0, 0], 0).into()).await.unwrap();
        assert!(address.ip().is_unspecified());
        assert_ne!(address.port(), 0);

        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(address.port()).user("postgres");
        for (database, code) in [
            ("postgres".to_owned(), "22023"),
            (format!("{template_database}/"), "22023"),
            (format!("{template_database}/a/b"), "22023"),
            ("unknown_template/lease".to_owned(), "3D000"),
        ] {
            config.dbname(&database);
            let error = match config.connect(tokio_postgres::NoTls).await {
                Err(error) => error,
                Ok(_) => panic!("unexpectedly accepted database {database:?}"),
            };
            assert_eq!(error.as_db_error().unwrap().code().code(), code);
        }

        config.dbname("pgtest");
        let (control, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let control_task = tokio::spawn(connection);
        assert_control_queries(&control).await;
        drop(control);
        control_task.await.unwrap().unwrap();

        let conn_str = format!(
            "host=127.0.0.1 port={} user=postgres password=ignored-by-trust \
             dbname={template_database}/123412312",
            address.port()
        );

        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await.unwrap();
        let conn_handle = tokio::spawn(connection);

        let row = client
            .query_one("SELECT 1::int4 AS x, current_database() AS database", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>("x"), 1);
        let database: &str = row.get("database");
        assert_ne!(database, template_database.as_str());
        assert!(database.starts_with(&format!("{template_database}_")));

        drop(client);
        let _ = conn_handle.await;
    }

    async fn assert_control_queries(control: &tokio_postgres::Client) {
        // The simple protocol uses text rows; extended queries use binary rows.
        for (query, column, value) in [
            ("SELECT 1", "?column?", "1"),
            ("SELECT pgtest_release('simple-release')", "pgtest_release", "t"),
        ] {
            let messages = control.simple_query(query).await.unwrap();
            let row = messages
                .iter()
                .find_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .expect("query must return a data row");
            assert_eq!(row.columns()[0].name(), column);
            assert_eq!(row.get(0), Some(value));
        }
        let ping = control.query_one("SELECT 1", &[]).await.unwrap();
        assert_eq!(ping.columns()[0].name(), "?column?");
        assert_eq!(ping.get::<_, i32>(0), 1);
        for query in
            ["SELECT pgtest_release('extended-release')", "SELECT pgtest_release($1::text)"]
        {
            let parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
                if query.contains('$') { &[&"parameter-release"] } else { &[] };
            let row = control.query_one(query, parameters).await.unwrap();
            assert_eq!(row.columns()[0].name(), "pgtest_release");
            assert!(row.get::<_, bool>(0));
        }

        for (query, code) in [
            ("SELECT pgtest_release($1)", "42P02"),
            ("SELECT pgtest_release('a/b')", "22023"),
            ("SELECT 2", "0A000"),
        ] {
            let error = control.simple_query(query).await.unwrap_err();
            assert_eq!(error.as_db_error().unwrap().code().code(), code);
        }
        use tokio_postgres::types::Type;
        for (query, types, code) in [
            ("SELECT 1", vec![Type::TEXT], "42804"),
            ("SELECT pgtest_release('literal')", vec![Type::TEXT], "42804"),
            ("SELECT pgtest_release($1)", vec![Type::INT4], "42804"),
            ("SELECT pgtest_release($1)", vec![Type::TEXT, Type::TEXT], "0A000"),
        ] {
            let error = control.prepare_typed(query, &types).await.unwrap_err();
            assert_eq!(error.as_db_error().unwrap().code().code(), code);
        }
        for (value, code) in [(None, "22004"), (Some("a/b"), "22023")] {
            let error =
                control.query_one("SELECT pgtest_release($1::text)", &[&value]).await.unwrap_err();
            assert_eq!(error.as_db_error().unwrap().code().code(), code);
        }
        // A Sync after Parse/Bind errors must leave the connection usable.
        assert_eq!(control.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
    }
}
