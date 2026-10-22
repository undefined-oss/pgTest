use std::{io, sync::Arc};

use async_trait::async_trait;
use pgtest::{LeaseId, LeaseService, errors::AttachError};
use pgwire::{error::PgWireResult, messages::startup::Startup};
use pingora_core::{
    apps::ServerApp, connectors::TransportConnector, protocols::Stream, server::ShutdownWatch,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::{
    control_panel::{self, PgTestControlPanel},
    relay, route, startup, upstream,
};

enum PreparedSession {
    Application(Stream, CancellationToken),
    Control(Arc<PgTestControlPanel>),
}

/// Resolve startup through the lease service, then relay the client's
/// PostgreSQL session.
pub struct WireProxy {
    template: Arc<str>,
    leases: LeaseService,
    connector: TransportConnector,
}

impl WireProxy {
    /// The caller keeps the lifecycle services alive until Pingora has stopped.
    /// Use the same template name used to initialize the lifecycle service.
    pub fn new(template: impl Into<Arc<str>>, leases: LeaseService) -> Self {
        Self { template: template.into(), leases, connector: TransportConnector::new(None) }
    }

    #[hotpath::measure(future = true)]
    async fn handle_startup<S>(
        &self,
        connection: &mut startup::ClientConnection<S>,
    ) -> PgWireResult<Option<PreparedSession>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let startup = startup::read(connection).await?;
        // Validate before attach: an unrelated template must not allocate a
        // lease.
        let route = match route::parse(&startup, &self.template) {
            Ok(route) => route,
            Err(error) => {
                return startup::reject(connection, error.code(), error.message())
                    .await
                    .map(|()| None);
            }
        };
        match route {
            route::Route::Application { lease, .. } => {
                self.connect_application(connection, startup, lease).await.map(|session| {
                    session.map(|(stream, stop)| PreparedSession::Application(stream, stop))
                })
            }
            route::Route::Control => {
                let handlers = Arc::new(PgTestControlPanel::new(Arc::new(self.leases.clone())));
                Ok(control_panel::initialize(connection, startup, &handlers)
                    .await?
                    .then_some(PreparedSession::Control(handlers)))
            }
        }
    }

    #[hotpath::measure(future = true)]
    async fn connect_application<S>(
        &self,
        connection: &mut startup::ClientConnection<S>,
        startup: Startup,
        lease: LeaseId,
    ) -> PgWireResult<Option<(Stream, CancellationToken)>>
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let session = match self.leases.attach(lease.clone()).await {
            Ok(session) => session,
            Err(error) => {
                let code = match error {
                    AttachError::LeaseClosed => "55000",
                    _ => "08006",
                };
                return startup::reject(connection, code, &error.to_string()).await.map(|()| None);
            }
        };
        tracing::info!(
            template = %self.template,
            %lease,
            database = %session.target.database,
            "resolved application lease"
        );
        let cancellation = session.cancellation_token();
        let upstream = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return startup::reject(connection, "55000", "lease was closed during connection startup").await.map(|()| None);
            }
            result = upstream::connect(&self.connector, &session.target, startup) => {
                match result {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::warn!(%error, "upstream connection or startup submission failed");
                        return startup::reject(connection, "08006", "unable to connect to PostgreSQL").await.map(|()| None);
                    }
                }
            }
        };
        tracing::info!(database = %session.target.database, "forwarded startup to PostgreSQL");
        // Dropping LeaseSession does not release the lease. Keep its
        // cancellation token so release can close the relay even during
        // authentication.
        Ok(Some((upstream, cancellation)))
    }

    async fn serve<S>(
        &self,
        connection: startup::ClientConnection<S>,
        shutdown: ShutdownWatch,
    ) -> PgWireResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        self.serve_inner(connection, shutdown, None).await
    }

    pub(crate) async fn serve_accepted(
        &self,
        stream: Stream,
        shutdown: ShutdownWatch,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let connection = match startup::accepted(stream) {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(%error, "unable to identify client transport");
                return;
            }
        };
        if let Err(error) = self.serve_inner(connection, shutdown, Some(permit)).await {
            tracing::warn!(%error, "PostgreSQL connection ended with an error");
        }
    }

    #[hotpath::measure(future = true)]
    async fn serve_inner<S>(
        &self,
        mut connection: startup::ClientConnection<S>,
        mut shutdown: ShutdownWatch,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> PgWireResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        let prepared = tokio::select! {
            biased;
            _ = shutdown.wait_for(|stopping| *stopping) => return Ok(()),
            // One deadline covers parsing, assignment and upstream startup.
            _ = tokio::time::sleep_until(deadline) => {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "PostgreSQL startup timed out").into());
            }
            result = self.handle_startup(&mut connection) => result?,
        };
        drop(permit); // Established application and control sessions release capacity.
        match prepared {
            Some(PreparedSession::Application(upstream, cancellation)) => {
                // The startup deadline ends here. Authentication and queries
                // are forwarded immediately, without waiting
                // for ReadyForQuery.
                relay::run(connection, upstream, cancellation, shutdown).await?;
            }
            Some(PreparedSession::Control(handlers)) => {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|stopping| *stopping) => {},
                    result = control_panel::run(connection, handlers) => result?,
                }
            }
            None => {}
        }
        Ok(())
    }
}

#[async_trait]
impl ServerApp for WireProxy {
    async fn process_new(self: &Arc<Self>, io: Stream, shutdown: &ShutdownWatch) -> Option<Stream> {
        let connection = match startup::accepted(io) {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(%error, "unable to identify client transport");
                return None;
            }
        };
        if let Err(error) = self.serve(connection, shutdown.clone()).await {
            tracing::warn!(%error, "PostgreSQL connection ended with an error");
        }
        // Session streams are closed by serve(); none are returned for reuse.
        None
    }
}

#[cfg(test)]
mod tests {
    //! Exercise the Pingora database services using the repository's isolated
    //! PostgreSQL fixture.

    use std::time::Duration;

    use pgtest::{LeaseId, config::PoolConfig};
    use pgtest_database_operations::{
        backend::PreparedPostgres, database_name::PostgresDatabaseName,
        testcontainer::pg_container_config,
    };
    use tokio_postgres::{Config, NoTls, config::SslMode};

    use super::WireProxy;
    use crate::{ServiceBuilder, startup};

    async fn rejection(proxy: &WireProxy, database: &str, code: &str, message: &str) {
        let (client, server) = tokio::io::duplex(128);
        let connection = startup::connection(server, ([127, 0, 0, 1], 54321).into());
        let (_shutdown, watch) = tokio::sync::watch::channel(false);
        let mut config = Config::new();
        config.user("alice").dbname(database).ssl_mode(SslMode::Disable);
        let client = async {
            let error = match config.connect_raw(client, NoTls).await {
                Err(error) => error,
                Ok(_) => panic!("expected a startup rejection"),
            };
            let error = error.as_db_error().expect("client must receive a PostgreSQL error");
            assert_eq!(error.code().code(), code);
            assert_eq!(error.message(), message);
            assert_eq!(error.severity(), "FATAL");
        };
        let server = async {
            proxy.serve(connection, watch).await.unwrap();
        };
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(client, server) })
            .await
            .expect("startup must finish promptly");
    }

    async fn query_database(proxy: &WireProxy, database: &str) -> String {
        let (client, server) = tokio::io::duplex(128);
        let connection = startup::connection(server, ([127, 0, 0, 1], 54321).into());
        let (_shutdown, watch) = tokio::sync::watch::channel(false);
        let mut config = Config::new();
        config.user("postgres").password("postgres").dbname(database).ssl_mode(SslMode::Disable);
        let client = async {
            let (client, driver) = config.connect_raw(client, NoTls).await.unwrap();
            let driver = tokio::spawn(driver);
            let row = client.query_one("SELECT current_database(), 42::int4", &[]).await.unwrap();
            let database: String = row.get(0);
            assert_eq!(row.get::<_, i32>(1), 42);
            drop(client);
            driver.await.unwrap().unwrap();
            database
        };
        let server = async {
            proxy.serve(connection, watch).await.unwrap();
        };
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(client, server).0 })
            .await
            .expect("authentication, query, and disconnect must finish")
    }

    #[tokio::test]
    async fn lease_attachment_checks_template_reuses_lease_and_reports_failures() {
        let postgres = pg_container_config().await;
        let template = postgres.pgtest_pg_database.to_string();
        let config = PoolConfig {
            initial_slots: 1.into(),
            starvation_threshold: 0.into(),
            grow_batch_size: 0.into(),
            lease_claim_timeout_ms: 100,
        };
        let prepared = PreparedPostgres::prepare(postgres).await.unwrap();
        let initial_databases = prepared.create_initial_databases(1).await.unwrap();
        let runtime =
            ServiceBuilder::new(prepared, config, initial_databases).start_async().await.unwrap();
        let leases = runtime.handle();
        let proxy = WireProxy::new(template.clone(), leases.clone());
        // A mismatched template must not consume the only database.
        rejection(
            &proxy,
            "wrong-template/not-allocated",
            "3D000",
            "database does not match the configured template",
        )
        .await;

        let database = format!("{template}/demo");
        let assigned = query_database(&proxy, &database).await;
        let lease = LeaseId::new("demo").unwrap();
        let first = leases.attach(lease.clone()).await.unwrap();
        assert!(first.target.database.starts_with(&format!("{template}_")));
        assert_eq!(assigned, first.target.database);
        assert_eq!(query_database(&proxy, &database).await, assigned);
        let second = leases.attach(lease).await.unwrap();
        assert_eq!(first.database_id, second.database_id);
        assert_eq!(first.target, second.target);
        assert!(!first.cancellation_token().is_cancelled());

        rejection(
            &proxy,
            &format!("{template}/another-lease"),
            "08006",
            "timed out waiting for a database",
        )
        .await;
        runtime.shutdown_async().await.unwrap();
        assert!(first.cancellation_token().is_cancelled());
        rejection(&proxy, &database, "08006", "the worker engine is unavailable").await;
    }

    #[tokio::test]
    async fn prepared_startup_preserves_other_services_databases() {
        let postgres = pg_container_config().await;
        let template = postgres.pgtest_pg_database.to_string();
        let (admin, driver) = Config::new()
            .host(postgres.pgtest_pg_host.as_str())
            .port(postgres.pgtest_pg_port.port())
            .user(postgres.pgtest_pg_user.as_str())
            .password("postgres")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(driver);
        let stale = format!("{template}_old");
        admin
            .batch_execute(&format!(
                "CREATE DATABASE {}",
                PostgresDatabaseName::quote_ident(&stale)
            ))
            .await
            .unwrap();

        let prepared = PreparedPostgres::prepare(postgres).await.unwrap();
        let row = admin
            .query_one("SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)", &[&stale])
            .await
            .unwrap();
        assert!(!row.get::<_, bool>(0), "the single bootstrap must clean stale clones");
        let config = PoolConfig {
            initial_slots: 1.into(),
            starvation_threshold: 0.into(),
            grow_batch_size: 0.into(),
            lease_claim_timeout_ms: 0,
        };
        let initial_databases = prepared.create_initial_databases(1).await.unwrap();
        let first = ServiceBuilder::new(prepared.clone(), config, initial_databases)
            .start_async()
            .await
            .unwrap();
        let first_proxy = WireProxy::new(template.clone(), first.handle());
        let route = format!("{template}/same");
        let first_database = query_database(&first_proxy, &route).await;

        // Starting the second service must not repeat discovery/cleanup and
        // remove the first service's assigned database. Each gets its
        // own initial supply.
        let initial_databases = prepared.create_initial_databases(1).await.unwrap();
        let second =
            ServiceBuilder::new(prepared, config, initial_databases).start_async().await.unwrap();
        let second_proxy = WireProxy::new(template, second.handle());
        let second_database = query_database(&second_proxy, &route).await;
        assert_ne!(first_database, second_database);
        assert_eq!(query_database(&first_proxy, &route).await, first_database);

        let lease = LeaseId::new("same").unwrap();
        let first_session = first.handle().attach(lease.clone()).await.unwrap();
        let second_session = second.handle().attach(lease.clone()).await.unwrap();
        first.handle().release(lease.clone()).await.unwrap();
        assert!(first_session.cancellation_token().is_cancelled());
        assert!(!second_session.cancellation_token().is_cancelled());
        first.shutdown_async().await.unwrap();
        assert_eq!(query_database(&second_proxy, &route).await, second_database);
        assert!(!second_session.cancellation_token().is_cancelled());
        second.shutdown_async().await.unwrap();
        assert!(second_session.cancellation_token().is_cancelled());
        drop(admin);
        driver.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn control_preserves_pipelined_startup_and_outlives_startup_deadline() {
        use std::sync::Arc;

        use bytes::BytesMut;
        use pgtest::lifecycle::LifecycleWorkers;
        use pgwire::messages::{Message, simplequery::Query, startup::Startup};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_util::sync::CancellationToken;

        async fn until_ready(client: &mut tokio::io::DuplexStream) -> Vec<u8> {
            let mut tags = Vec::new();
            loop {
                let tag = client.read_u8().await.unwrap();
                tags.push(tag);
                let length = client.read_u32().await.unwrap() as usize;
                let mut payload = vec![0; length - 4];
                client.read_exact(&mut payload).await.unwrap();
                assert_ne!(tag, b'E', "unexpected error: {payload:?}");
                if tag == b'Z' {
                    return tags;
                }
            }
        }

        let workers =
            LifecycleWorkers::new(PoolConfig::default(), Vec::new(), CancellationToken::new());
        let proxy = WireProxy::new("template", workers.service.clone());
        let (mut client, server) = tokio::io::duplex(4096);
        let (shutdown, watch) = tokio::sync::watch::channel(false);
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = capacity.clone().acquire_owned().await.unwrap();
        let serving = tokio::spawn(async move {
            proxy
                .serve_inner(
                    startup::connection(server, ([127, 0, 0, 1], 1234).into()),
                    watch,
                    Some(permit),
                )
                .await
        });
        let mut startup = Startup::new();
        startup.parameters.insert("user".into(), "alice".into());
        startup.parameters.insert("database".into(), "pgtest".into());
        let mut packet = BytesMut::new();
        startup.encode(&mut packet).unwrap();
        let mut query = BytesMut::new();
        Query::new("SELECT 1".into()).encode(&mut query).unwrap();
        packet.extend_from_slice(&query);
        client.write_all(&packet).await.unwrap();
        assert_eq!(until_ready(&mut client).await[0], b'R');
        assert_eq!(until_ready(&mut client).await, b"TDCZ");
        assert_eq!(capacity.available_permits(), 1);
        tokio::time::advance(Duration::from_secs(61)).await;
        client.write_all(&query).await.unwrap();
        assert_eq!(until_ready(&mut client).await, b"TDCZ");
        shutdown.send(true).unwrap();
        serving.await.unwrap().unwrap();
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    }
}
