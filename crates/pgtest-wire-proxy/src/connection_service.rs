//! Accept connections and spawn sessions on the shared Pingora runtime.
use std::{io, sync::Arc};

use pgtest::lifecycle::{LeaseService, StartError};
use pingora_core::server::ShutdownWatch;
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinSet,
};

use crate::{WireProxy, server::Listeners, services::Startup};

pub(crate) struct ConnectionService {
    template: Arc<str>,
    leases: LeaseService,
    listeners: Listeners,
    pending_limit: usize,
}
impl ConnectionService {
    pub(crate) fn new(
        template: Arc<str>,
        leases: LeaseService,
        listeners: Listeners,
        pending_limit: usize,
    ) -> Self {
        Self { template, leases, listeners, pending_limit }
    }

    pub(crate) async fn run(
        self,
        mut shutdown: ShutdownWatch,
        status: oneshot::Sender<Startup>,
    ) -> io::Result<()> {
        let initialized = self.listeners.register();
        let listeners = match initialized {
            Ok(listeners) => listeners,
            Err(error) => {
                let _ = status.send(Err(StartError::Thread(error)));
                return Err(io::Error::other("listener initialization failed"));
            }
        };
        let proxy = Arc::new(WireProxy::new(self.template.clone(), self.leases.clone()));
        let capacity = Arc::new(Semaphore::new(self.pending_limit));
        status.send(Ok(())).map_err(|_| io::Error::other("startup supervisor stopped"))?;
        let mut sessions = JoinSet::new();
        let result = loop {
            tokio::select! {
                biased;
                _ = async { let _ = shutdown.wait_for(|stopped| *stopped).await; } => break Ok(()),
                result = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = result { break Err(io::Error::other(error)); }
                },
                accepted = async {
                    let permit = capacity.clone().acquire_owned()
                        .await.map_err(io::Error::other)?;
                    listeners.accept().await.map(|stream| (stream, permit))
                } => {
                    let (stream, permit) = match accepted { Ok(pair) => pair, Err(error) => break Err(error) };
                    let proxy = proxy.clone();
                    let shutdown = shutdown.clone();
                    sessions.spawn(async move { proxy.serve_accepted(stream, shutdown, permit).await; });
                }
            }
        };
        drop(listeners);
        sessions.shutdown().await;
        result
    }
}
