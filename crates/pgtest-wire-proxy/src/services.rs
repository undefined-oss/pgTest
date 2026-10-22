//! One Pingora runtime hosts lifecycle workers and connection sessions.
use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::future::BoxFuture;
use pgtest::{
    config::PoolConfig,
    lifecycle::{LeaseService, LifecycleWorkers, StartError},
};
use pgtest_database_operations::{
    backend::PreparedPostgres, cleanup_client::CleanupClient, creation_client::CreationClient,
};
use pgtest_engine_backend::{DatabaseCleaner, DatabaseCreator, ProvisionedDatabase};
use pingora_core::{
    server::{
        RunArgs, Server, ShutdownSignal, ShutdownSignalWatch, ShutdownWatch,
        configuration::ServerConf,
    },
    services::{ServiceReadyNotifier, ServiceWithDependents},
};
use tokio::{sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{connection_service::ConnectionService, server::Listeners};
pub(crate) type Startup = Result<(), StartError>;

pub struct ServiceBuilder {
    pub(crate) service: LeaseService,
    lifecycle_tasks: Vec<BoxFuture<'static, ()>>,
    readiness: Vec<oneshot::Receiver<Startup>>,
    connections: Option<ConnectionService>,
    threads: NonZeroUsize,
}

impl ServiceBuilder {
    pub fn new(
        prepared: PreparedPostgres,
        config: PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
    ) -> Self {
        let cleanup_config = prepared.config.clone();
        let creation_limit = prepared.config.pgtest_pg_creation_pool_connection.into();
        let cleanup_limit = prepared.config.pgtest_pg_cleanup_pool_connection.into();
        Self::with_clients(
            config,
            initial_databases,
            creation_limit,
            cleanup_limit,
            async move {
                Ok(Arc::new(CreationClient::connect(&prepared.config, prepared.metadata).await?))
            },
            async move { Ok(Arc::new(CleanupClient::connect(&cleanup_config).await?)) },
        )
    }

    pub(crate) fn with_clients<C, D, CF, DF>(
        config: PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
        creation_limit: NonZeroUsize,
        cleanup_limit: NonZeroUsize,
        create: CF,
        clean: DF,
    ) -> Self
    where
        C: DatabaseCreator,
        D: DatabaseCleaner,
        CF: Future<Output = Result<Arc<C>, StartError>> + Send + 'static,
        DF: Future<Output = Result<Arc<D>, StartError>> + Send + 'static,
    {
        let LifecycleWorkers { service, creation, cleanup } =
            LifecycleWorkers::new(config, initial_databases, CancellationToken::new());
        let (creation_ready, created) = oneshot::channel();
        let (cleanup_ready, cleaned) = oneshot::channel();
        Self {
            service,
            lifecycle_tasks: vec![
                Box::pin(creation.run(create, creation_limit, creation_ready)),
                Box::pin(cleanup.run(clean, cleanup_limit, cleanup_ready)),
            ],
            readiness: vec![created, cleaned],
            connections: None,
            threads: NonZeroUsize::MIN,
        }
    }

    pub(crate) fn add_connections(
        &mut self,
        template: Arc<str>,
        listeners: Listeners,
        workers: NonZeroUsize,
        pending_limit: usize,
    ) -> io::Result<()> {
        if pending_limit == 0 {
            return Err(io::Error::other("startup capacity must be nonzero"));
        }
        self.connections =
            Some(ConnectionService::new(template, self.service.clone(), listeners, pending_limit));
        self.threads = workers;
        Ok(())
    }

    pub(crate) fn build(self) -> (Server, LeaseService, oneshot::Receiver<Startup>) {
        let (ready, startup) = oneshot::channel();
        let mut server = Server::new_with_opt_and_conf(
            None,
            ServerConf {
                threads: self.threads.get(),
                work_stealing: true,
                grace_period_seconds: Some(0),
                graceful_shutdown_timeout_seconds: Some(5),
                ..Default::default()
            },
        );
        server.add_boxed_service(Box::new(SharedService {
            lifecycle_tasks: Mutex::new(self.lifecycle_tasks),
            readiness: self.readiness,
            connections: self.connections,
            ready: Some(ready),
            stop: self.service.shutdown_token(),
        }));
        (server, self.service, startup)
    }

    /// Run on a synchronous caller. The shutdown future creates any Tokio I/O
    /// when polled by Pingora's main loop, including during initialization.
    pub fn run_until(
        self,
        shutdown: impl Future<Output = io::Result<()>> + Send + 'static,
    ) -> io::Result<()> {
        let (server, service, startup) = self.build();
        let stop = service.shutdown_token();
        let _stop_on_exit = stop.clone().drop_guard();
        let (result, mut outcome) = oneshot::channel();
        let signal = async move {
            let _stop_on_exit = stop.clone().drop_guard();
            let mut shutdown = std::pin::pin!(shutdown);
            let outcome = tokio::select! {
                biased;
                outcome = &mut shutdown => outcome,
                started = startup => match started.unwrap_or(Err(StartError::RuntimeStopped)) {
                    Err(error) => Err(io::Error::other(error)),
                    Ok(()) => {
                        tracing::info!(target: "server", "pgtest server listening");
                        tokio::select! {
                            biased;
                            outcome = &mut shutdown => outcome,
                            _ = stop.cancelled() => Err(io::Error::other("proxy worker stopped")),
                        }
                    },
                },
            };
            tracing::info!(target: "server", "Stopping pgtest server");
            let _ = result.send(outcome);
            ShutdownSignal::GracefulTerminate
        };
        server.run(RunArgs {
            shutdown_signal: Box::new(RunSignal(Mutex::new(Some(Box::pin(signal))))),
        });
        // Server::run has already awaited the signal and shut down its
        // runtimes.
        outcome
            .try_recv()
            .map_err(|_| io::Error::other("Pingora stopped without a shutdown result"))?
    }
}

struct SharedService {
    // BoxFuture is Send but not Sync; Pingora requires a Sync service.
    // Used only at startup through get_mut(), without locking.
    lifecycle_tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    readiness: Vec<oneshot::Receiver<Startup>>,
    connections: Option<ConnectionService>,
    ready: Option<oneshot::Sender<Startup>>,
    stop: CancellationToken,
}

impl SharedService {
    async fn initialize(&mut self, shutdown: &mut ShutdownWatch) -> Startup {
        let mut failure = None;
        for mut status in self.readiness.drain(..) {
            let result = tokio::select! {
                biased;
                result = &mut status => result,
                _ = async { let _ = shutdown.wait_for(|stopped| *stopped).await; } => {
                    self.stop.cancel();
                    status.await
                },
            };
            // Collect both results: a worker's actual error takes precedence
            // over the cancellation it triggers in its sibling.
            match result.unwrap_or(Err(StartError::RuntimeStopped)) {
                Ok(()) => {}
                Err(StartError::RuntimeStopped) => {
                    failure.get_or_insert(StartError::RuntimeStopped);
                }
                Err(error) => failure = Some(error),
            }
        }
        if let Some(error) = failure {
            Err(error)
        } else if self.stop.is_cancelled() {
            Err(StartError::RuntimeStopped)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl ServiceWithDependents for SharedService {
    async fn start_service(
        &mut self,
        #[cfg(unix)] _fds: Option<pingora_core::server::ListenFds>,
        mut shutdown: ShutdownWatch,
        _listeners_per_fd: usize,
        ready: ServiceReadyNotifier,
    ) {
        let _stop_on_exit = self.stop.clone().drop_guard();
        let mut lifecycle_tasks = JoinSet::new();
        for task in self.lifecycle_tasks.get_mut().unwrap().drain(..) {
            lifecycle_tasks.spawn(task);
        }
        let status = self.ready.take().expect("service starts once");
        match self.initialize(&mut shutdown).await {
            Err(error) => {
                let _ = status.send(Err(error));
            }
            Ok(()) => {
                ready.notify_ready();
                if let Some(connections) = self.connections.take() {
                    if let Err(error) = connections.run(shutdown, status).await {
                        tracing::error!(%error, "connection service stopped");
                    }
                } else if status.send(Ok(())).is_ok() {
                    tokio::select! {
                        _ = self.stop.cancelled() => {},
                        _ = shutdown.wait_for(|stopped| *stopped) => {},
                    }
                }
            }
        }
        self.stop.cancel();
        lifecycle_tasks.shutdown().await;
    }

    fn name(&self) -> &str {
        "pgtest-shared"
    }

    fn listen_addresses(&self) -> Option<Vec<String>> {
        Some(Vec::new())
    }
}

// Pingora's watcher takes &self; take the future once, then await without a
// lock.
struct RunSignal(Mutex<Option<BoxFuture<'static, ShutdownSignal>>>);
#[async_trait]
impl ShutdownSignalWatch for RunSignal {
    async fn recv(&self) -> ShutdownSignal {
        let signal = self.0.lock().unwrap().take().expect("Pingora waits for shutdown once");
        signal.await
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use pgtest::{LeaseId, errors::AttachError};
    use pgtest_engine_backend::{
        BackendError, PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId,
    };
    use tokio::sync::Notify;

    use super::*;
    use crate::test_support::ServiceHost;

    const LIMIT: Duration = Duration::from_secs(10);

    struct Backend {
        events: Mutex<
            Vec<(
                &'static str,
                thread::ThreadId,
                String,
                tokio::runtime::RuntimeFlavor,
                tokio::runtime::Id,
                usize,
            )>,
        >,
        creating: Notify,
        allow_creation: Notify,
        fail_creation: bool,
    }
    impl Backend {
        fn record(&self, event: &'static str) {
            self.events.lock().unwrap().push((
                event,
                thread::current().id(),
                thread::current().name().unwrap().to_owned(),
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::Handle::current().id(),
                tokio::runtime::Handle::current().metrics().num_workers(),
            ));
        }
    }
    impl DatabaseCreator for Backend {
        async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
            self.record("create");
            self.creating.notify_one();
            if self.fail_creation {
                return Err(BackendError::OperationFailed("injected".into()));
            }
            self.allow_creation.notified().await;
            Ok(ProvisionedDatabase {
                resource_id: ResourceId("db".into()),
                target: PgTarget {
                    database: "db".into(),
                    endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
                },
            })
        }
    }
    impl DatabaseCleaner for Backend {
        async fn delete_database(&self, _: ResourceId) -> Result<(), BackendError> {
            self.record("delete");
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
    fn backend(fail_creation: bool) -> Arc<Backend> {
        Arc::new(Backend {
            events: Mutex::new(Vec::new()),
            creating: Notify::new(),
            allow_creation: Notify::new(),
            fail_creation,
        })
    }
    async fn launch(backend: Arc<Backend>, initial: u16) -> Result<ServiceHost, StartError> {
        let initial_databases = pgtest_database_operations::backend::create_initial_databases(
            backend.as_ref(),
            usize::from(initial),
            NonZeroUsize::MIN,
        )
        .await
        .map_err(pgtest_database_operations::backend::BootstrapError::from)?;
        let cleanup = backend.clone();
        ServiceBuilder::with_clients(
            config(initial),
            initial_databases,
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            async move {
                backend.record("creation-init");
                Ok(backend)
            },
            async move {
                cleanup.record("cleanup-init");
                Ok(cleanup)
            },
        )
        .start_async()
        .await
    }

    #[test]
    fn direct_run_serves_on_workers_and_returns_on_the_calling_thread() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        assert!(tokio::runtime::Handle::try_current().is_err());
        let caller = thread::current().id();
        let database = backend(false);
        let creation = database.clone();
        let cleanup = database.clone();
        let mut builder = ServiceBuilder::with_clients(
            config(0),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            async move {
                creation.record("creation-init");
                Ok(creation)
            },
            async move {
                cleanup.record("cleanup-init");
                Ok(cleanup)
            },
        );
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        let listeners = Listeners::new(tcp);
        #[cfg(unix)]
        let directory =
            tempfile::Builder::new().prefix("pgtest-direct-").tempdir_in("/tmp").unwrap();
        #[cfg(unix)]
        let listeners = listeners
            .bind_unix(directory.path(), std::num::NonZeroU16::new(6433).unwrap())
            .unwrap();
        #[cfg(unix)]
        let socket_path = listeners.unix_path().unwrap().to_owned();
        builder
            .add_connections("template".into(), listeners, NonZeroUsize::new(2).unwrap(), 1)
            .unwrap();
        let stop = builder.service.shutdown_token();
        builder
            .run_until(async move {
                // This future runs inside Pingora's block_on on the caller,
                // while listeners and database clients run on service threads.
                assert_eq!(thread::current().id(), caller);
                tokio::time::timeout(LIMIT, async {
                    let mut client = tokio::net::TcpStream::connect(address).await?;
                    client.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await?;
                    assert_eq!(client.read_u8().await?, b'N');
                    Ok(())
                })
                .await
                .map_err(io::Error::other)?
            })
            .unwrap();
        assert_eq!(thread::current().id(), caller);
        assert!(tokio::runtime::Handle::try_current().is_err());
        assert!(stop.is_cancelled());
        let events = database.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.1 != caller));
        assert!(events.iter().all(|event| event.5 == 2));
        assert_eq!(events[0].4, events[1].4);
        assert!(std::net::TcpStream::connect(address).is_err());
        #[cfg(unix)]
        assert!(!socket_path.exists());
    }

    #[test]
    fn direct_run_preserves_startup_errors() {
        let builder = ServiceBuilder::with_clients(
            config(0),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            async {
                Err::<Arc<Backend>, _>(StartError::Thread(io::Error::other("initializer failed")))
            },
            std::future::pending::<Result<Arc<Backend>, StartError>>(),
        );
        let error = builder
            .run_until(async {
                tokio::time::sleep(LIMIT).await;
                Err(io::Error::other("startup did not stop"))
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "initializer failed");
    }

    #[test]
    fn direct_run_reports_worker_exit_after_readiness() {
        let mut builder = ServiceBuilder::with_clients(
            config(0),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            async { Ok(backend(false)) },
            async { Ok(backend(false)) },
        );
        let stop = builder.service.shutdown_token();
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        builder
            .add_connections("template".into(), Listeners::new(tcp), NonZeroUsize::MIN, 1)
            .unwrap();
        let error = builder
            .run_until(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut client = tokio::net::TcpStream::connect(address).await?;
                client.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await?;
                assert_eq!(client.read_u8().await?, b'N');
                stop.cancel();
                tokio::time::sleep(LIMIT).await;
                Err(io::Error::other("worker exit did not stop Pingora"))
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "proxy worker stopped");
    }

    #[test]
    fn direct_run_can_stop_during_initialization_and_preserves_signal_errors() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        for fail_signal in [false, true] {
            let dropped = Arc::new(AtomicBool::new(false));
            let guard = DropFlag(dropped.clone());
            let (entered, initialized) = oneshot::channel();
            let builder = ServiceBuilder::with_clients(
                config(0),
                Vec::new(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                async move {
                    let _guard = guard;
                    entered.send(()).unwrap();
                    std::future::pending::<Result<Arc<Backend>, StartError>>().await
                },
                std::future::pending::<Result<Arc<Backend>, StartError>>(),
            );
            let result = builder.run_until(async move {
                tokio::time::timeout(LIMIT, initialized)
                    .await
                    .map_err(io::Error::other)?
                    .map_err(io::Error::other)?;
                if fail_signal { Err(io::Error::other("signal failed")) } else { Ok(()) }
            });
            if fail_signal {
                assert_eq!(result.unwrap_err().to_string(), "signal failed");
            } else {
                result.unwrap();
            }
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn services_share_one_runtime_with_the_configured_worker_count() {
        for workers in [1, 4] {
            let backend = backend(false);
            let creation = backend.clone();
            let cleanup = backend.clone();
            let mut builder = ServiceBuilder::with_clients(
                config(0),
                Vec::new(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                async move {
                    creation.record("creation-init");
                    Ok(creation)
                },
                async move {
                    cleanup.record("cleanup-init");
                    Ok(cleanup)
                },
            );
            let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = tcp.local_addr().unwrap();
            builder
                .add_connections(
                    "template".into(),
                    Listeners::new(tcp),
                    NonZeroUsize::new(workers).unwrap(),
                    1,
                )
                .unwrap();
            let host = builder.start_async().await.unwrap();
            // Verify connections remain usable with a one-thread shared pool
            // too.
            tokio::time::timeout(LIMIT, async {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                for _ in 0..16 {
                    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
                    client.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
                    assert_eq!(client.read_u8().await.unwrap(), b'N');
                }
            })
            .await
            .unwrap();
            {
                let events = backend.events.lock().unwrap();
                assert_eq!(events.len(), 2);
                for event in events.iter() {
                    assert_eq!(event.3, tokio::runtime::RuntimeFlavor::MultiThread);
                    assert_eq!(event.5, workers);
                }
                assert_eq!(events[0].4, events[1].4, "lifecycle tasks must share the runtime");
            }
            host.shutdown_async().await.unwrap();
        }
    }

    #[tokio::test]
    async fn bootstrap_prepopulates_inventory_before_pingora_services_start() {
        let backend = backend(false);
        let bootstrap_thread = std::thread::current().id();
        let starting = tokio::spawn({
            let backend = backend.clone();
            async move { launch(backend, 1).await }
        });
        tokio::time::timeout(LIMIT, backend.creating.notified()).await.unwrap();
        assert!(!starting.is_finished(), "creation must finish before startup succeeds");
        assert!(
            backend.events.lock().unwrap().iter().all(|event| event.0 == "create"),
            "worker initialization must not start before bootstrap finishes"
        );
        backend.allow_creation.notify_one();
        let lifecycle = tokio::time::timeout(LIMIT, starting).await.unwrap().unwrap().unwrap();
        let service = lifecycle.handle();
        let lease = LeaseId::new("shared").unwrap();
        let session = service.attach(lease.clone()).await.unwrap();
        assert_eq!(session.target.database, "db");
        service.release(lease).await.unwrap();
        tokio::time::timeout(LIMIT, async {
            while !backend.events.lock().unwrap().iter().any(|event| event.0 == "delete") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::spawn_blocking(move || lifecycle.shutdown()).await.unwrap().unwrap();
        assert!(session.cancellation_token().is_cancelled());
        let events = backend.events.lock().unwrap();
        let creation = events.iter().find(|event| event.0 == "creation-init").unwrap();
        let cleanup = events.iter().find(|event| event.0 == "cleanup-init").unwrap();
        assert_eq!(creation.4, cleanup.4);
        assert_eq!(creation.2, "pgtest-shared");
        assert_eq!(cleanup.2, "pgtest-shared");
        assert_eq!(events.iter().filter(|event| event.0 == "create").count(), 1);
        for event in events.iter() {
            if event.0 == "create" {
                assert_eq!(event.1, bootstrap_thread);
                continue;
            }
            assert_eq!(event.3, tokio::runtime::RuntimeFlavor::MultiThread);
            assert_eq!(
                event.4,
                if event.0 == "delete" || event.0 == "cleanup-init" {
                    cleanup.4
                } else {
                    creation.4
                }
            );
        }
    }

    #[tokio::test]
    async fn initialization_failure_stops_pingora_and_preserves_the_error() {
        for creation_fails in [false, true] {
            let starting = tokio::task::spawn_blocking(move || {
                let initialize = |fails| async move {
                    if fails {
                        Err::<Arc<Backend>, _>(StartError::Thread(io::Error::other(
                            "initializer failed",
                        )))
                    } else {
                        std::future::pending().await
                    }
                };
                ServiceBuilder::with_clients(
                    config(0),
                    Vec::new(),
                    NonZeroUsize::MIN,
                    NonZeroUsize::MIN,
                    initialize(creation_fails),
                    initialize(!creation_fails),
                )
                .start()
            });
            let result = tokio::time::timeout(LIMIT, starting).await.unwrap().unwrap();
            assert!(
                matches!(result, Err(StartError::Thread(error)) if error.to_string() == "initializer failed")
            );
        }
        let database = backend(true);
        let result = tokio::time::timeout(LIMIT, launch(database.clone(), 1)).await.unwrap();
        assert!(matches!(
            result,
            Err(StartError::Bootstrap(
                pgtest_database_operations::backend::BootstrapError::InitialDatabaseCreation(_)
            ))
        ));
        assert!(database.events.lock().unwrap().iter().all(|event| event.0 == "create"));
    }

    #[tokio::test]
    async fn pingora_shutdown_watch_stops_shared_workers_and_invalidates_leases() {
        let builder = ServiceBuilder::with_clients(
            config(0),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            async { Ok(backend(false)) },
            async { Ok(backend(false)) },
        );
        let service = builder.service.clone();
        let (status, started) = oneshot::channel();
        let mut task = SharedService {
            lifecycle_tasks: Mutex::new(builder.lifecycle_tasks),
            readiness: builder.readiness,
            connections: None,
            ready: Some(status),
            stop: service.shutdown_token(),
        };
        let (shutdown, watch) = tokio::sync::watch::channel(false);
        let (ready, _) = tokio::sync::watch::channel(false);
        let running = tokio::spawn(async move {
            task.start_service(
                #[cfg(unix)]
                None,
                watch,
                1,
                ServiceReadyNotifier::new(ready),
            )
            .await;
        });
        tokio::time::timeout(LIMIT, started).await.unwrap().unwrap().unwrap();
        shutdown.send_replace(true);
        tokio::time::timeout(LIMIT, running).await.unwrap().unwrap();
        assert!(service.shutdown_token().is_cancelled());
        assert!(matches!(
            service.attach(LeaseId::new("stopped").unwrap()).await,
            Err(AttachError::EngineUnavailable)
        ));
    }

    #[tokio::test]
    async fn worker_panic_after_startup_stops_the_pingora_host() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        };

        struct Panics;
        impl DatabaseCreator for Panics {
            async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
                panic!("injected creation panic")
            }
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let starting = tokio::task::spawn_blocking(move || {
            let mut pool = config(0);
            pool.grow_batch_size = 1.into();
            let mut builder = ServiceBuilder::with_clients(
                pool,
                Vec::new(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                async { Ok(Arc::new(Panics)) },
                async { Ok(backend(false)) },
            );
            builder
                .add_connections("template".into(), Listeners::new(listener), NonZeroUsize::MIN, 1)
                .unwrap();
            builder.start()
        });
        let lifecycle = tokio::time::timeout(LIMIT, starting).await.unwrap().unwrap().unwrap();
        let (_control, connection) = tokio::time::timeout(
            LIMIT,
            tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user("postgres")
                .dbname("pgtest")
                .connect(tokio_postgres::NoTls),
        )
        .await
        .unwrap()
        .unwrap();
        let control = tokio::spawn(connection);
        let mut partial = TcpStream::connect(address).await.unwrap();
        partial.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
        assert_eq!(tokio::time::timeout(LIMIT, partial.read_u8()).await.unwrap().unwrap(), b'N');
        let service = lifecycle.handle();
        let result = tokio::time::timeout(LIMIT, service.attach(LeaseId::new("panic").unwrap()))
            .await
            .unwrap();
        assert!(matches!(result, Err(AttachError::EngineUnavailable)));
        tokio::time::timeout(LIMIT, service.stopped()).await.unwrap();
        // Lifecycle failure must reach Pingora's watch and close both idle
        // control sessions and incomplete startups before the host is
        // explicitly joined.
        assert_eq!(
            tokio::time::timeout(LIMIT, partial.read_u8()).await.unwrap().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert!(tokio::time::timeout(LIMIT, control).await.unwrap().unwrap().is_err());
        tokio::time::timeout(LIMIT, tokio::task::spawn_blocking(move || lifecycle.shutdown()))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn listeners_wait_for_database_initialization_and_never_accept_on_failure() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        };

        for fail in [false, true] {
            let database = backend(false);
            let initializing = Arc::new(Notify::new());
            let allow_initialization = Arc::new(Notify::new());
            let creation = database.clone();
            let cleanup = database.clone();
            let mut builder = ServiceBuilder::with_clients(
                config(0),
                Vec::new(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                {
                    let initializing = initializing.clone();
                    let allow_initialization = allow_initialization.clone();
                    async move {
                        initializing.notify_one();
                        allow_initialization.notified().await;
                        if fail {
                            Err(StartError::Thread(io::Error::other(
                                "database initialization failed",
                            )))
                        } else {
                            Ok(creation)
                        }
                    }
                },
                async move { Ok(cleanup) },
            );
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let listeners = Listeners::new(listener);
            #[cfg(unix)]
            let directory =
                tempfile::Builder::new().prefix("pgtest-init-").tempdir_in("/tmp").unwrap();
            #[cfg(unix)]
            let listeners = listeners
                .bind_unix(directory.path(), std::num::NonZeroU16::new(6433).unwrap())
                .unwrap();
            #[cfg(unix)]
            let socket_path = listeners.unix_path().unwrap().to_owned();
            builder
                .add_connections("template".into(), listeners, NonZeroUsize::new(2).unwrap(), 1)
                .unwrap();
            let starting = tokio::spawn(builder.start_async());
            tokio::time::timeout(LIMIT, initializing.notified()).await.unwrap();
            // TCP may queue in the kernel, but no service may read PostgreSQL
            // startup.
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(b"\x00\x00\x00\x08\x04\xd2\x16\x2f").await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), client.read_u8()).await.is_err()
            );
            assert!(!starting.is_finished());
            allow_initialization.notify_one();
            let result = tokio::time::timeout(LIMIT, starting).await.unwrap().unwrap();
            if fail {
                assert!(
                    matches!(result, Err(StartError::Thread(error)) if error.to_string() == "database initialization failed")
                );
                assert!(tokio::time::timeout(LIMIT, client.read_u8()).await.unwrap().is_err());
                assert!(TcpStream::connect(address).await.is_err());
            } else {
                let host = result.unwrap();
                assert_eq!(
                    tokio::time::timeout(LIMIT, client.read_u8()).await.unwrap().unwrap(),
                    b'N'
                );
                host.shutdown_async().await.unwrap();
                assert!(tokio::time::timeout(LIMIT, client.read_u8()).await.unwrap().is_err());
                assert!(TcpStream::connect(address).await.is_err());
            }
            #[cfg(unix)]
            assert!(!socket_path.exists(), "shutdown or startup failure must remove the socket");
        }
    }

    #[tokio::test]
    async fn cancelling_async_startup_drops_both_initializers() {
        struct InitializerGuard(tokio::sync::mpsc::UnboundedSender<()>);
        impl Drop for InitializerGuard {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let (entered, mut entries) = tokio::sync::mpsc::unbounded_channel();
        let (closed, mut exits) = tokio::sync::mpsc::unbounded_channel();
        let initialize = move || {
            let entered = entered.clone();
            let closed = closed.clone();
            async move {
                let _guard = InitializerGuard(closed);
                entered.send(()).unwrap();
                std::future::pending::<Result<Arc<Backend>, StartError>>().await
            }
        };
        let mut builder = ServiceBuilder::with_clients(
            config(0),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            initialize(),
            initialize(),
        );
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = tcp.local_addr().unwrap();
        let listeners = Listeners::new(tcp);
        #[cfg(unix)]
        let directory =
            tempfile::Builder::new().prefix("pgtest-cancel-").tempdir_in("/tmp").unwrap();
        #[cfg(unix)]
        let listeners = listeners
            .bind_unix(directory.path(), std::num::NonZeroU16::new(6433).unwrap())
            .unwrap();
        #[cfg(unix)]
        let socket_path = listeners.unix_path().unwrap().to_owned();
        builder
            .add_connections("template".into(), listeners, NonZeroUsize::new(2).unwrap(), 1)
            .unwrap();
        let service = builder.service.clone();
        let starting = tokio::spawn(builder.start_async());
        tokio::time::timeout(LIMIT, async {
            entries.recv().await.unwrap();
            entries.recv().await.unwrap();
        })
        .await
        .unwrap();
        starting.abort();
        assert!(matches!(starting.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(LIMIT, async {
            service.stopped().await;
            exits.recv().await.unwrap();
            exits.recv().await.unwrap();
            // The detached blocking startup task must finish joining Pingora
            // too.
            loop {
                #[cfg(unix)]
                let socket_gone = !socket_path.exists();
                #[cfg(not(unix))]
                let socket_gone = true;
                if socket_gone && tokio::net::TcpStream::connect(address).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
