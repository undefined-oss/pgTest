//! Manager's caller-facing API and shared message-processing modules.

pub mod database_inventory;
pub mod errors;
pub mod lease;
pub mod manager_worker;
pub mod messages;
#[cfg(test)]
mod tests;

pub use lease::LeaseId;
#[cfg(feature = "tokio-runtime")]
pub use lease::LeaseSession;
#[cfg(feature = "tokio-runtime")]
pub use tokio_handle::ManagerHandle;
#[cfg(feature = "tokio-runtime")]
pub(crate) use tokio_handle::{Reply, TokioMessage};

#[cfg(feature = "tokio-runtime")]
mod tokio_handle {
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;

    use super::{
        LeaseId, LeaseSession,
        errors::{AttachError, ReleaseError},
        messages::{ConsumerReply, ElapsedTime, ManagerMessage},
    };
    pub(crate) enum Reply {
        Attached(LeaseSession),
        Engine(ConsumerReply),
    }
    pub(crate) type TokioMessage = ManagerMessage<oneshot::Sender<Reply>>;

    #[derive(Clone)]
    pub struct ManagerHandle {
        pub(crate) commands: mpsc::UnboundedSender<TokioMessage>,
        pub(crate) epoch: tokio::time::Instant,
        pub(crate) claim_timeout: Duration,
        pub(crate) shutdown: CancellationToken,
    }
    impl ManagerHandle {
        #[hotpath::measure]
        pub async fn attach(&self, lease: LeaseId) -> Result<LeaseSession, AttachError> {
            if self.shutdown.is_cancelled() {
                return Err(AttachError::EngineUnavailable);
            }
            let started = tokio::time::Instant::now();
            let (tx, rx) = oneshot::channel();
            self.commands
                .send(ManagerMessage::AttachOrJoin {
                    lease,
                    reply: tx,
                    message_time: ElapsedTime(started.duration_since(self.epoch)),
                })
                .map_err(|_| AttachError::EngineUnavailable)?;
            let wait = async {
                tokio::select! {
                    biased;
                    _ = self.shutdown.cancelled() => Err(AttachError::EngineUnavailable),
                    reply = rx => reply.map_err(|_| AttachError::EngineUnavailable),
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
