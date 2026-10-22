//! Listeners and the lifecycle of the complete proxy service.
use std::{io, net::TcpListener, num::NonZeroUsize, sync::Arc};

use pgtest::config::PoolConfig;
use pgtest_database_operations::backend::PreparedPostgres;
use pgtest_engine_backend::ProvisionedDatabase;
use pingora_core::protocols::{Stream, l4::listener::Listener};

use crate::services::ServiceBuilder;

/// Bound standard listeners, moved into Pingora's shared runtime. `bind_unix`
/// owns socket-file cleanup; `with_unix` accepts a caller-owned path.
pub struct Listeners {
    tcp: TcpListener,
    #[cfg(unix)]
    unix: Option<std::os::unix::net::UnixListener>,
    #[cfg(unix)]
    socket_file: Option<Arc<crate::unix_socket::SocketFile>>,
}
impl Listeners {
    pub fn new(tcp: TcpListener) -> Self {
        Self {
            tcp,
            #[cfg(unix)]
            unix: None,
            #[cfg(unix)]
            socket_file: None,
        }
    }

    #[cfg(unix)]
    pub fn with_unix(mut self, unix: std::os::unix::net::UnixListener) -> Self {
        self.unix = Some(unix);
        self.socket_file = None;
        self
    }

    /// Bind `.s.PGSQL.<port>` in an existing directory. Remove only this socket
    /// when the last listener closes, including startup failure or
    /// cancellation.
    #[cfg(unix)]
    pub fn bind_unix(
        mut self,
        directory: &std::path::Path,
        port: std::num::NonZeroU16,
    ) -> io::Result<Self> {
        let (listener, socket) = crate::unix_socket::SocketFile::bind(directory, port)?;
        self.unix = Some(listener);
        self.socket_file = Some(socket);
        Ok(self)
    }

    #[cfg(unix)]
    pub fn unix_path(&self) -> Option<&std::path::Path> {
        self.socket_file.as_ref().map(|socket| socket.path())
    }

    pub(crate) fn register(self) -> io::Result<RegisteredListeners> {
        self.tcp.set_nonblocking(true)?;
        #[cfg(unix)]
        if let Some(unix) = &self.unix {
            unix.set_nonblocking(true)?;
        }
        Ok(RegisteredListeners {
            tcp: tokio::net::TcpListener::from_std(self.tcp)?.into(),
            #[cfg(unix)]
            unix: self.unix.map(tokio::net::UnixListener::from_std).transpose()?.map(Into::into),
            #[cfg(unix)]
            _socket_file: self.socket_file,
        })
    }
}
pub(crate) struct RegisteredListeners {
    tcp: Listener,
    #[cfg(unix)]
    unix: Option<Listener>,
    #[cfg(unix)]
    _socket_file: Option<Arc<crate::unix_socket::SocketFile>>,
}
impl RegisteredListeners {
    pub(crate) async fn accept(&self) -> io::Result<Stream> {
        let mut stream = {
            #[cfg(unix)]
            if let Some(unix) = &self.unix {
                tokio::select! {
                    stream = self.tcp.accept() => stream,
                    stream = unix.accept() => stream,
                }
            } else {
                self.tcp.accept().await
            }
            #[cfg(not(unix))]
            self.tcp.accept().await
        }?;
        // The low-level Pingora listener skips ListenerEndpoint's socket
        // settings. Match that endpoint and pgwire's former negotiate_tls
        // path: send small PostgreSQL replies without Nagle coalescing.
        // Pingora treats this setting as a no-op for Unix sockets.
        stream.set_nodelay().map_err(io::Error::other)?;
        Ok(Box::new(stream))
    }
}

/// Run the proxy synchronously until shutdown or a service failure.
pub struct ProxyServer;
impl ProxyServer {
    /// Run Pingora on the calling thread, outside any Tokio runtime. The
    /// shutdown future is polled by Pingora's main loop. Returns after service
    /// runtimes stop so caller-owned resources and profiling guards can drop.
    pub fn run_until(
        prepared: PreparedPostgres,
        config: PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
        listeners: Listeners,
        connection_workers: NonZeroUsize,
        shutdown: impl std::future::Future<Output = io::Result<()>> + Send + 'static,
    ) -> io::Result<()> {
        let address = listeners.tcp.local_addr()?;
        #[cfg(unix)]
        let unix_path = listeners.unix_path().map(ToOwned::to_owned);
        let services =
            Self::services(prepared, config, initial_databases, listeners, connection_workers)?;
        let _span = tracing::info_span!(
            target: "server", "server", %address, workers = %connection_workers,
        )
        .entered();
        #[cfg(unix)]
        if let Some(path) = unix_path {
            tracing::info!(target: "server", path = %path.display(), "pgtest Unix socket bound");
        }
        services.run_until(shutdown)
    }

    pub(crate) fn services(
        prepared: PreparedPostgres,
        config: PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
        listeners: Listeners,
        connection_workers: NonZeroUsize,
    ) -> io::Result<ServiceBuilder> {
        let template: Arc<str> = prepared.metadata.template.clone().into();
        let mut services = ServiceBuilder::new(prepared, config, initial_databases);
        services.add_connections(template, listeners, connection_workers, 1024)?;
        Ok(services)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::BytesMut;
    use pgtest::LeaseId;
    use pgtest_engine_backend::{
        BackendError, DatabaseCleaner, DatabaseCreator, PgEndpoint, PgTarget, ProvisionedDatabase,
        ResourceId,
    };
    use pgwire::messages::{Message, startup::Startup};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use tokio_postgres::{Config, NoTls};

    use super::*;
    use crate::{startup, test_support::TestServer as ProxyServer};

    const LIMIT: Duration = Duration::from_secs(5);
    const SSL: &[u8] = b"\x00\x00\x00\x08\x04\xd2\x16\x2f";

    #[cfg(unix)]
    #[tokio::test]
    async fn accepted_tcp_connections_disable_nagle() {
        use std::os::fd::{AsRawFd, BorrowedFd};

        let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        let listeners = Listeners::new(tcp).register().unwrap();
        let _client = TcpStream::connect(address).await.unwrap();
        let accepted = listeners.accept().await.unwrap();
        let stream = accepted
            .as_ref()
            .as_any()
            .downcast_ref::<pingora_core::protocols::l4::stream::Stream>()
            .unwrap();
        // SAFETY: `accepted` owns this live descriptor throughout the borrow.
        // Clone it so the std socket cannot close Pingora's descriptor.
        let fd = unsafe { BorrowedFd::borrow_raw(stream.as_raw_fd()) };
        let socket = std::net::TcpStream::from(fd.try_clone_to_owned().unwrap());
        assert!(socket.nodelay().unwrap(), "client replies must not wait for Nagle coalescing");
    }

    struct Backend {
        port: u16,
        created: AtomicUsize,
        deleted: AtomicUsize,
    }
    impl Backend {
        fn provision(&self) -> ProvisionedDatabase {
            let id = self.created.fetch_add(1, Ordering::SeqCst);
            ProvisionedDatabase {
                resource_id: ResourceId(format!("provider/{id}")),
                target: PgTarget {
                    database: format!("database_{id}"),
                    endpoint: PgEndpoint::Tcp { host: "127.0.0.1".into(), port: self.port },
                },
            }
        }
    }
    impl DatabaseCreator for Backend {
        async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
            Ok(self.provision())
        }
    }
    impl DatabaseCleaner for Backend {
        async fn delete_database(&self, _: ResourceId) -> Result<(), BackendError> {
            self.deleted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn config(initial: u16) -> PoolConfig {
        PoolConfig {
            initial_slots: initial.into(),
            starvation_threshold: 0.into(),
            grow_batch_size: 0.into(),
            lease_claim_timeout_ms: 0,
        }
    }
    fn services(initial: u16, port: u16) -> (ServiceBuilder, Arc<Backend>) {
        let backend =
            Arc::new(Backend { port, created: AtomicUsize::new(0), deleted: AtomicUsize::new(0) });
        let creation = backend.clone();
        let cleanup = backend.clone();
        let builder = ServiceBuilder::with_clients(
            config(initial),
            (0..initial).map(|_| backend.provision()).collect(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            async move { Ok(creation) },
            async move { Ok(cleanup) },
        );
        (builder, backend)
    }
    fn packet(database: &str) -> Vec<u8> {
        let mut startup = Startup::new();
        startup.parameters.insert("user".into(), "alice".into());
        startup.parameters.insert("database".into(), database.into());
        let mut bytes = BytesMut::new();
        startup.encode(&mut bytes).unwrap();
        bytes.to_vec()
    }
    async fn reject(config: &Config, code: &str) {
        let error = match config.connect(NoTls).await {
            Ok(_) => panic!("expected rejection"),
            Err(error) => error,
        };
        assert_eq!(error.as_db_error().unwrap().code().code(), code);
    }

    #[tokio::test]
    async fn tcp_and_unix_accept_locally_reuse_inventory_and_cancel_during_authentication() {
        tokio::time::timeout(LIMIT, async {
            let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (mut builder, backend) = services(1, upstream.local_addr().unwrap().port());
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let listeners = Listeners::new(listener);
            #[cfg(unix)]
            let directory =
                tempfile::Builder::new().prefix("pgtest-wire-").tempdir_in("/tmp").unwrap();
            #[cfg(unix)]
            let path = directory.path().join(".s.PGSQL.6433");
            #[cfg(unix)]
            let listeners = {
                use std::os::unix::fs::PermissionsExt;
                let unix = std::os::unix::net::UnixListener::bind(&path).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
                listeners.with_unix(unix)
            };
            builder
                .add_connections("template".into(), listeners, NonZeroUsize::new(2).unwrap(), 1)
                .unwrap();
            let server = ProxyServer { host: builder.start_async().await.unwrap() };
            let clients = if cfg!(unix) { 2 } else { 1 };
            let upstream = tokio::spawn(async move {
                let mut sessions = tokio::task::JoinSet::new();
                for _ in 0..clients {
                    let (socket, peer) = upstream.accept().await.unwrap();
                    sessions.spawn(async move {
                        let mut connection = startup::connection(socket, peer);
                        let initial = startup::read(&mut connection).await.unwrap();
                        assert_eq!(initial.parameters["database"], "database_0");
                        assert_eq!(initial.parameters["user"], "alice");
                        // Bytes coalesced after startup must survive into the
                        // relay.
                        let mut stream = connection.into_parts();
                        let mut received = stream.read_buf.to_vec();
                        while received.len() < 4 {
                            received.push(stream.io.read_u8().await.unwrap());
                        }
                        assert_eq!(received, b"tail");
                        stream.io.write_all(b"challenge").await.unwrap();
                        assert_eq!(stream.io.read(&mut [0]).await.unwrap(), 0);
                    });
                }
                while let Some(result) = sessions.join_next().await {
                    result.unwrap();
                }
            });
            let mut tcp = TcpStream::connect(address).await.unwrap();
            tcp.write_all(SSL).await.unwrap();
            assert_eq!(tcp.read_u8().await.unwrap(), b'N');
            let mut initial = packet("template/demo");
            initial.extend_from_slice(b"tail");
            for chunk in initial.chunks(3) {
                tcp.write_all(chunk).await.unwrap();
            }
            let mut challenge = [0; 9];
            tcp.read_exact(&mut challenge).await.unwrap();
            assert_eq!(&challenge, b"challenge");
            #[cfg(unix)]
            let mut unix = {
                let mut unix = tokio::net::UnixStream::connect(&path).await.unwrap();
                unix.write_all(&initial).await.unwrap();
                unix.read_exact(&mut challenge).await.unwrap();
                unix
            };
            assert_eq!(backend.created.load(Ordering::SeqCst), 1);
            // A control connection can release application sessions accepted by
            // either worker, even while PostgreSQL authentication is in
            // progress.
            let (control, driver) = Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user("alice")
                .dbname("pgtest")
                .connect(NoTls)
                .await
                .unwrap();
            let driver = tokio::spawn(driver);
            assert!(
                control
                    .query_one("SELECT pgtest_release($1)", &[&"demo"])
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
            );
            assert_eq!(tcp.read(&mut [0]).await.unwrap(), 0);
            #[cfg(unix)]
            assert_eq!(unix.read(&mut [0]).await.unwrap(), 0);
            upstream.await.unwrap();
            // Established relays released their startup permits; reject
            // requests also release capacity without touching the
            // shared inventory.
            let mut config = Config::new();
            config.host("127.0.0.1").port(address.port()).user("alice").dbname("pgtest");
            config.dbname("other/demo");
            reject(&config, "3D000").await;
            server.shutdown().unwrap();
            assert!(driver.await.unwrap().is_err());
            assert!(TcpStream::connect(address).await.is_err());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn pending_limit_stops_accept_until_partial_startup_ends() {
        tokio::time::timeout(LIMIT, async {
            let (mut builder, _) = services(0, 1);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            builder
                .add_connections("template".into(), Listeners::new(listener), NonZeroUsize::MIN, 1)
                .unwrap();
            let server = ProxyServer { host: builder.start_async().await.unwrap() };
            let mut slow = TcpStream::connect(address).await.unwrap();
            slow.write_all(SSL).await.unwrap();
            assert_eq!(slow.read_u8().await.unwrap(), b'N');
            let mut waiting = TcpStream::connect(address).await.unwrap();
            waiting.write_all(&packet("pgtest")).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), waiting.read_u8()).await.is_err()
            );
            drop(slow);
            assert_eq!(waiting.read_u8().await.unwrap(), b'R');
            server.shutdown().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn worker_failure_and_cancelled_wait_close_partial_startups() {
        for cancel_wait in [false, true] {
            tokio::time::timeout(LIMIT, async {
                let (mut builder, _) = services(0, 1);
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let stop = builder.service.shutdown_token();
                builder
                    .add_connections(
                        "template".into(),
                        Listeners::new(listener),
                        NonZeroUsize::new(2).unwrap(),
                        1,
                    )
                    .unwrap();
                let server = Arc::new(ProxyServer { host: builder.start_async().await.unwrap() });
                let serving = tokio::spawn({
                    let server = server.clone();
                    async move { server.serve().await }
                });
                let mut client = TcpStream::connect(address).await.unwrap();
                client.write_all(SSL).await.unwrap();
                assert_eq!(client.read_u8().await.unwrap(), b'N');
                if cancel_wait {
                    serving.abort();
                    assert!(serving.await.unwrap_err().is_cancelled());
                } else {
                    stop.cancel();
                    assert!(serving.await.unwrap().is_err());
                }
                assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
                let server = Arc::try_unwrap(server).ok().unwrap();
                server.shutdown().unwrap();
                assert!(TcpStream::connect(address).await.is_err());
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn postgres_full_server_reuses_one_database_across_connections() {
        use pgtest_database_operations::testcontainer::pg_container_config;
        let config = pg_container_config().await;
        let template = config.pgtest_pg_database.to_string();
        let prepared = PreparedPostgres::prepare(config).await.unwrap();
        let initial_databases = prepared.create_initial_databases(1).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::task::spawn_blocking(move || {
            ProxyServer::start(
                prepared,
                self::config(1),
                initial_databases,
                Listeners::new(listener),
                NonZeroUsize::new(2).unwrap(),
            )
        })
        .await
        .unwrap()
        .unwrap();
        let mut expected = None;
        for _ in 0..8 {
            let (client, driver) = Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user("postgres")
                .password("postgres")
                .dbname(format!("{template}/same"))
                .connect(NoTls)
                .await
                .unwrap();
            let driver = tokio::spawn(driver);
            let database: String =
                client.query_one("SELECT current_database()", &[]).await.unwrap().get(0);
            if let Some(expected) = &expected {
                assert_eq!(&database, expected);
            } else {
                expected = Some(database);
            }
            drop(client);
            driver.await.unwrap().unwrap();
        }
        tokio::task::spawn_blocking(move || server.shutdown()).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn control_queries_over_tcp_and_unix_preserve_protocol_and_share_leases() {
        tokio::time::timeout(LIMIT, async {
            let (mut builder, backend) = services(2, 1);
            let leases = builder.service.clone();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let listeners = Listeners::new(listener);
            #[cfg(unix)]
            let directory =
                tempfile::Builder::new().prefix("pgtest-control-").tempdir_in("/tmp").unwrap();
            #[cfg(unix)]
            let listeners = listeners
                .bind_unix(directory.path(), std::num::NonZeroU16::new(6433).unwrap())
                .unwrap();
            // One startup slot: an established control session must release it.
            builder.add_connections("template".into(), listeners, NonZeroUsize::MIN, 1).unwrap();
            let server = ProxyServer { host: builder.start_async().await.unwrap() };
            let mut tcp = Config::new();
            tcp.host("127.0.0.1").port(address.port()).user("alice").dbname("pgtest");
            let (first, driver) = tcp.connect(NoTls).await.unwrap();
            let first_driver = tokio::spawn(driver);
            #[cfg(not(unix))]
            let second_config = tcp.clone();
            #[cfg(unix)]
            let second_config = {
                let mut second_config = Config::new();
                second_config.host_path(directory.path()).port(6433).user("alice").dbname("pgtest");
                second_config
            };
            let (second, driver) = second_config.connect(NoTls).await.unwrap();
            let second_driver = tokio::spawn(driver);
            for client in [&first, &second] {
                let rows = client.simple_query("SELECT 1;").await.unwrap();
                let row = rows
                    .iter()
                    .find_map(|message| match message {
                        tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                        _ => None,
                    })
                    .expect("expected a row");
                assert_eq!(row.get(0), Some("1"));
                let statement = client.prepare("SELECT 1").await.unwrap();
                assert_eq!(statement.columns()[0].type_(), &tokio_postgres::types::Type::INT4);
                assert_eq!(client.query_one(&statement, &[]).await.unwrap().get::<_, i32>(0), 1);
                for (query, code) in [
                    ("SELECT 2", "0A000"),
                    ("SELECT 1; SELECT 1", "0A000"),
                    ("SELECT pgtest_release('a/b')", "22023"),
                    ("SELECT pgtest_release($1)", "42P02"),
                ] {
                    let error = client.simple_query(query).await.unwrap_err();
                    assert_eq!(error.code().unwrap().code(), code);
                }
                let statement = client.prepare("SELECT pgtest_release($1::text)").await.unwrap();
                assert_eq!(statement.params(), &[tokio_postgres::types::Type::TEXT]);
                assert_eq!(statement.columns()[0].type_(), &tokio_postgres::types::Type::BOOL);
                for (value, code) in [(None, "22004"), (Some("a/b"), "22023")] {
                    let error = client.query_one(&statement, &[&value]).await.unwrap_err();
                    assert_eq!(error.code().unwrap().code(), code);
                    // The driver's Sync restores the connection after a Bind
                    // error.
                    assert_eq!(
                        client.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0),
                        1
                    );
                }
                assert!(
                    client.query_one(&statement, &[&"unseen"]).await.unwrap().get::<_, bool>(0)
                );
            }
            // Both initial databases remain available after every control
            // request.
            let literal = leases.attach(LeaseId::new("suite's 雪").unwrap()).await.unwrap();
            let parameter = leases.attach(LeaseId::new(" Mixed Case 雪 ").unwrap()).await.unwrap();
            assert_eq!(backend.created.load(Ordering::SeqCst), 2);
            let rows = first.simple_query("SELECT pgtest_release('suite''s 雪')").await.unwrap();
            let row = rows
                .iter()
                .find_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .expect("expected a row");
            assert_eq!(row.get(0), Some("t"));
            assert!(literal.cancellation_token().is_cancelled());
            assert!(!parameter.cancellation_token().is_cancelled());
            assert!(
                second
                    .query_one("SELECT pgtest_release($1)", &[&" Mixed Case 雪 "])
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
            );
            assert!(parameter.cancellation_token().is_cancelled());
            drop(first);
            first_driver.await.unwrap().unwrap();
            // Lifecycle shutdown closes an idle control connection.
            server.shutdown().unwrap();
            assert!(second_driver.await.unwrap().is_err());
            #[cfg(unix)]
            assert!(!directory.path().join(".s.PGSQL.6433").exists());
        })
        .await
        .expect("control requests and shutdown must complete");
    }
}
