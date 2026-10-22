//! Shared lease metadata with creation-only publication and cleanup-only
//! removal.
mod cleanup;
mod creation;
mod registry;
mod start_error;

use std::{sync::Arc, time::Duration};

pub use cleanup::CleanupWorker;
pub use creation::CreationWorker;
use hotpath::wrap::tokio::sync::mpsc;
use pgtest_engine_backend::ProvisionedDatabase;
use registry::{Entry, Reader};
pub use start_error::StartError;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{
    LeaseId, LeaseSession,
    errors::{AttachError, ReleaseError},
};

pub(crate) enum CreationRequest {
    Attach { lease: LeaseId, reply: oneshot::Sender<Result<LeaseSession, AttachError>> },
    Cancel { lease: LeaseId },
    Release { lease: LeaseId, reply: oneshot::Sender<()> },
}

pub(crate) enum Cleanup {
    Release { entry: Arc<Entry>, reply: Option<oneshot::Sender<()>> },
    Expire { entry: Arc<Entry>, at: tokio::time::Instant },
}

/// A read handle and request endpoints, with no manager or core ownership.
#[derive(Clone)]
pub struct LeaseService {
    registry: Reader,
    creation: mpsc::UnboundedSender<CreationRequest>,
    cleanup: mpsc::UnboundedSender<Cleanup>,
    timeout: Duration,
    stop: CancellationToken,
}

impl LeaseService {
    #[hotpath::measure(future = true)]
    pub async fn attach(&self, lease: LeaseId) -> Result<LeaseSession, AttachError> {
        if self.stop.is_cancelled() {
            return Err(AttachError::EngineUnavailable);
        }
        let wait = async {
            if let Some(entry) = self.registry.get(&lease) {
                return entry.ready();
            }
            let (reply, receiver) = oneshot::channel();
            self.creation
                .send(CreationRequest::Attach { lease: lease.clone(), reply })
                .map_err(|_| AttachError::EngineUnavailable)?;
            let mut pending = PendingAttach {
                lease,
                receiver,
                creation: self.creation.clone(),
                completed: false,
            };
            let result =
                (&mut pending.receiver).await.map_err(|_| AttachError::EngineUnavailable)?;
            pending.completed = true;
            let session = result?;
            if session.cancellation_token().is_cancelled() {
                Err(AttachError::LeaseClosed)
            } else {
                Ok(session)
            }
        };
        tokio::select! {
            biased;
            _ = self.stop.cancelled() => Err(AttachError::EngineUnavailable),
            result = async {
                if self.timeout.is_zero() { wait.await }
                else { tokio::time::timeout(self.timeout, wait).await.map_err(|_| AttachError::TimedOut)? }
            } => result,
        }
    }

    /// Acknowledge logical removal; physical deletion proceeds independently.
    #[hotpath::measure(future = true)]
    pub async fn release(&self, lease: LeaseId) -> Result<(), ReleaseError> {
        if self.stop.is_cancelled() {
            return Err(ReleaseError::EngineUnavailable);
        }
        let (reply, receiver) = oneshot::channel();
        if let Some(entry) = self.registry.get(&lease) {
            self.cleanup
                .send(Cleanup::Release { entry, reply: Some(reply) })
                .map_err(|_| ReleaseError::EngineUnavailable)?;
        } else {
            // Creation owns pending requests and serializes this release with
            // publication. It rechecks the registry before cancelling waiters.
            self.creation
                .send(CreationRequest::Release { lease, reply })
                .map_err(|_| ReleaseError::EngineUnavailable)?;
        }
        tokio::select! {
            biased;
            _ = self.stop.cancelled() => Err(ReleaseError::EngineUnavailable),
            result = tokio::time::timeout(Duration::from_secs(5), receiver) => {
                result.map_err(|_| ReleaseError::ReplyTimedOut)?.map_err(|_| ReleaseError::EngineUnavailable)?;
                Ok(())
            }
        }
    }

    pub async fn stopped(&self) {
        self.stop.cancelled().await;
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.stop.clone()
    }
}

/// A dropped connection/timeout wakes creation to prune its local waiters.
/// Close the receiver before sending the notification so pruning sees it.
struct PendingAttach {
    lease: LeaseId,
    receiver: oneshot::Receiver<Result<LeaseSession, AttachError>>,
    creation: mpsc::UnboundedSender<CreationRequest>,
    completed: bool,
}
impl Drop for PendingAttach {
    fn drop(&mut self) {
        if !self.completed {
            self.receiver.close();
            let _ = self.creation.send(CreationRequest::Cancel { lease: self.lease.clone() });
        }
    }
}

/// Independently hosted workers sharing one lease registry and cancellation
/// signal.
///
/// Construction seeds the inventory from bootstrap database descriptors; it
/// does not spawn threads, create runtimes, or connect to PostgreSQL.
/// Run each worker once on its owning runtime; initialize its
/// client there. The host must cancel `stop`
/// during shutdown. Either running worker exiting or being dropped also cancels
/// its sibling.
pub struct LifecycleWorkers {
    pub service: LeaseService,
    pub creation: CreationWorker,
    pub cleanup: CleanupWorker,
}

impl LifecycleWorkers {
    pub fn new(
        config: crate::config::PoolConfig,
        initial_databases: Vec<ProvisionedDatabase>,
        stop: CancellationToken,
    ) -> Self {
        let (reader, publisher, remover) = registry::split();
        let (creation_tx, creation_rx) =
            hotpath::channel!(tokio::sync::mpsc::unbounded_channel(), label = "creation");
        let (cleanup_tx, cleanup_rx) =
            hotpath::channel!(tokio::sync::mpsc::unbounded_channel(), label = "cleanup");
        let timeout = Duration::from_nanos_u128(
            config.lease_claim_timeout_ms.checked_mul(1_000_000).expect("lease timeout too large"),
        );
        Self {
            service: LeaseService {
                registry: reader,
                creation: creation_tx,
                cleanup: cleanup_tx.clone(),
                timeout,
                stop: stop.clone(),
            },
            creation: CreationWorker::new(
                publisher,
                creation_rx,
                cleanup_tx,
                config,
                crate::inventory::DatabaseInventory::from_initial(initial_databases),
                timeout,
                stop.clone(),
            ),
            cleanup: CleanupWorker::new(remover, cleanup_rx, stop),
        }
    }
}

#[cfg(test)]
mod tests;
