//! Test-only hosting for exercising the server from async test cases.
use std::{io, num::NonZeroUsize, thread};

use async_trait::async_trait;
use pgtest::{
    config::PoolConfig,
    lifecycle::{LeaseService, StartError},
};
use pgtest_database_operations::backend::PreparedPostgres;
use pgtest_engine_backend::ProvisionedDatabase;
use pingora_core::server::{RunArgs, ShutdownSignal, ShutdownSignalWatch};
use tokio_util::sync::CancellationToken;

use crate::{Listeners, ProxyServer, ServiceBuilder};

/// Keeps a test server alive while a test drives its clients.
pub struct ServiceHost {
    service: LeaseService,
    thread: Option<thread::JoinHandle<()>>,
}

impl ServiceBuilder {
    /// Cancellation also stops initialization on the supervisor thread.
    pub async fn start_async(self) -> Result<ServiceHost, StartError> {
        let cancel_on_drop = self.service.shutdown_token().drop_guard();
        let host = tokio::task::spawn_blocking(move || self.start())
            .await
            .map_err(|error| StartError::Thread(io::Error::other(error)))??;
        cancel_on_drop.disarm();
        Ok(host)
    }

    /// Async callers use `start_async` to avoid blocking their runtime.
    pub fn start(self) -> Result<ServiceHost, StartError> {
        let (server, service, startup) = self.build();
        let stop = service.shutdown_token();
        let mut owner = ServiceHost { service, thread: None };
        owner.thread =
            Some(thread::Builder::new().name("pgtest-pingora".into()).spawn(move || {
                let _stop_on_exit = stop.clone().drop_guard();
                server.run(RunArgs { shutdown_signal: Box::new(StopSignal(stop)) });
            })?);
        startup.blocking_recv().unwrap_or(Err(StartError::RuntimeStopped))?;
        if owner.service.shutdown_token().is_cancelled() {
            return Err(StartError::RuntimeStopped);
        }
        Ok(owner)
    }
}

impl ServiceHost {
    pub fn handle(&self) -> LeaseService {
        self.service.clone()
    }

    pub fn shutdown(mut self) -> io::Result<()> {
        self.stop_and_join()
    }

    pub async fn shutdown_async(self) -> io::Result<()> {
        tokio::task::spawn_blocking(move || self.shutdown()).await.map_err(io::Error::other)?
    }

    fn stop_and_join(&mut self) -> io::Result<()> {
        self.service.shutdown_token().cancel();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| io::Error::other("Pingora supervisor panicked"))?;
        }
        Ok(())
    }
}
impl Drop for ServiceHost {
    fn drop(&mut self) {
        if let Err(error) = self.stop_and_join() {
            tracing::error!(%error, "Pingora shutdown failed");
        }
    }
}

struct StopSignal(CancellationToken);
#[async_trait]
impl ShutdownSignalWatch for StopSignal {
    async fn recv(&self) -> ShutdownSignal {
        self.0.cancelled().await;
        ShutdownSignal::GracefulTerminate
    }
}

pub(crate) struct TestServer {
    pub(crate) host: ServiceHost,
}
impl TestServer {
    pub fn start(
        prepared: PreparedPostgres,
        config: PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
        listeners: Listeners,
        connection_workers: NonZeroUsize,
    ) -> io::Result<Self> {
        let services = ProxyServer::services(
            prepared,
            config,
            initial_databases,
            listeners,
            connection_workers,
        )?;
        let host = services.start().map_err(io::Error::other)?;
        Ok(Self { host })
    }

    /// Start Pingora without blocking the caller's runtime. Dropping the future
    /// cancels initialization and closes the listeners.
    pub async fn start_async(
        prepared: PreparedPostgres,
        config: PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
        listeners: Listeners,
        connection_workers: NonZeroUsize,
    ) -> io::Result<Self> {
        let services = ProxyServer::services(
            prepared,
            config,
            initial_databases,
            listeners,
            connection_workers,
        )?;
        let host = services.start_async().await.map_err(io::Error::other)?;
        Ok(Self { host })
    }

    /// Wait for a worker failure. All accepting and I/O happen on workers.
    /// Dropping this future requests shutdown; call `shutdown_async` to join.
    pub async fn serve(&self) -> io::Result<()> {
        let stop = self.host.handle().shutdown_token();
        let _stop_on_exit = stop.clone().drop_guard();
        stop.cancelled().await;
        Err(io::Error::other("proxy worker stopped"))
    }

    pub fn shutdown(self) -> io::Result<()> {
        self.host.shutdown()
    }

    pub async fn shutdown_async(self) -> io::Result<()> {
        self.host.shutdown_async().await
    }
}
