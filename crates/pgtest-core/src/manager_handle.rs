//! Manager's caller-facing API and shared message-processing modules.

pub mod database_inventory;
pub mod errors;
pub mod lease;
pub mod manager_worker;
pub mod messages;
#[cfg(test)]
mod tests;
#[cfg(feature = "tokio-runtime")]
pub(crate) mod waiting;

pub use lease::LeaseId;
#[cfg(feature = "tokio-runtime")]
pub use lease::LeaseSession;
#[cfg(feature = "tokio-runtime")]
pub use tokio_handle::ManagerHandle;
#[cfg(feature = "tokio-runtime")]
pub(crate) use tokio_handle::{Reply, TokioMessage};

#[cfg(feature = "tokio-runtime")]
mod tokio_handle {
    use std::{sync::Arc, time::Duration};

    use hotpath::wrap::tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::{
        LeaseId, LeaseSession,
        errors::{AttachError, ReleaseError},
        lease::SharedLeases,
        messages::{ConsumerReply, ManagerMessage},
        waiting::WaitingRegistry,
    };
    #[derive(Clone, Debug)]
    pub(crate) enum Reply {
        Attached(LeaseSession),
        Engine(ConsumerReply),
    }
    pub(crate) type TokioMessage = ManagerMessage<oneshot::Sender<Reply>>;

    #[derive(Clone)]
    pub struct ManagerHandle {
        pub(crate) leases: SharedLeases,
        pub(crate) waiting: Arc<WaitingRegistry>,
        pub(crate) commands: mpsc::UnboundedSender<TokioMessage>,
        pub(crate) epoch: tokio::time::Instant,
        pub(crate) claim_timeout: Duration,
        pub(crate) shutdown: CancellationToken,
    }
    impl ManagerHandle {
        #[hotpath::measure]
        pub async fn attach(&self, lease: LeaseId) -> Result<LeaseSession, AttachError> {
            if self.shutdown.is_cancelled() || self.commands.is_closed() {
                return Err(AttachError::EngineUnavailable);
            }
            // Only successfully claimed assignments are available here. Drop
            // the map guard before entering the asynchronous message path.
            let session = hotpath::measure_block!("manager_handle::lease_lookup", {
                let leases = self.leases.pin();
                leases.get(&lease).and_then(|entry| entry.session.get()).cloned()
            });
            if let Some(session) = session {
                if self.shutdown.is_cancelled() || self.commands.is_closed() {
                    return Err(AttachError::EngineUnavailable);
                }
                return if session.cancellation.is_cancelled() {
                    Err(AttachError::LeaseClosed)
                } else {
                    Ok(session)
                };
            }
            let started = tokio::time::Instant::now();
            let (assignment, work) = self.waiting.register(lease);
            if let Some(work) = work {
                hotpath::measure_block!("manager_handle::submit_assignment", {
                    self.commands.send(work)
                })
                .map_err(|_| AttachError::EngineUnavailable)?;
            }
            // Own the shared clone inside the timed future. Cancelling this
            // caller drops its subscription even when the manager is idle.
            let wait = async move {
                tokio::select! {
                    biased;
                    _ = self.shutdown.cancelled() => Err(AttachError::EngineUnavailable),
                    reply = hotpath::future!(assignment, label = "manager_handle::wait_assignment") => reply,
                }
            };
            let reply = if self.claim_timeout.is_zero() {
                wait.await
            } else {
                tokio::time::timeout_at(started + self.claim_timeout, wait)
                    .await
                    .map_err(|_| AttachError::TimedOut)?
            }?;
            match reply {
                Reply::Attached(session) if !session.cancellation.is_cancelled() => Ok(session),
                Reply::Attached(_) => Err(AttachError::LeaseClosed),
                Reply::Engine(ConsumerReply::AttachRejected(error)) => Err(error),
                _ => Err(AttachError::Failed),
            }
        }

        /// Releases the current assignment if present; deletion happens
        /// asynchronously.
        #[hotpath::measure]
        pub async fn release(&self, lease: LeaseId) -> Result<(), ReleaseError> {
            if self.shutdown.is_cancelled() {
                return Err(ReleaseError::EngineUnavailable);
            }
            let (tx, rx) = oneshot::channel();
            self.commands
                .send(ManagerMessage::ReleaseLease { lease, reply: tx })
                .map_err(|_| ReleaseError::EngineUnavailable)?;
            let wait = async {
                tokio::select! {
                    biased;
                    _ = self.shutdown.cancelled() => Err(ReleaseError::EngineUnavailable),
                    reply = rx => reply.map_err(|_| ReleaseError::EngineUnavailable),
                }
            };
            match tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .map_err(|_| ReleaseError::ReplyTimedOut)??
            {
                Reply::Engine(ConsumerReply::ReleaseResult(result)) => result,
                _ => Err(ReleaseError::UnexpectedReply),
            }
        }

        pub async fn stopped(&self) {
            self.commands.closed().await;
        }
    }
}
