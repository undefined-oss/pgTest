//! Tokio driver for the shared lifecycle actors.
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use pgtest_database_operations::{
    backend::{BootstrapError, PreparedPostgres},
    cleanup_worker_handle::CleanupHandle,
    config::PostgresConfig,
    creation_worker_handle::CreationHandle,
    errors::PostgresClientError,
};
use pgtest_engine_backend::{BackendError, PgTarget, jobs::DatabaseWorkerMessages};
use tokio::sync::{mpsc, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::worker_engine::{
    core::{LeaseId, WorkerEngine, WorkerEngineConfig},
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId},
    errors::{AttachError, IOError, ReleaseError},
    messages::{ConsumerReply, EngineMessage, LeaseKey, RequestId, Tick},
    traits::EngineIO,
};
#[cfg(all(test, feature = "runtime-tests"))]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    #[error(transparent)]
    Postgres(#[from] PostgresClientError),
    #[error("initial database creation failed: {0}")]
    InitialDatabaseCreation(#[from] BackendError),
    #[error("actor runtime stopped during startup")]
    RuntimeStopped,
}

enum Reply {
    Attached(LeaseSession),
    Engine(ConsumerReply),
}
struct Envelope {
    message: EngineMessage,
    reply: Option<(RequestId, oneshot::Sender<Reply>)>,
}

impl From<DatabaseWorkerMessages> for Envelope {
    fn from(message: DatabaseWorkerMessages) -> Self {
        Self { message: EngineMessage::DatabaseWorker(message), reply: None }
    }
}

#[derive(Clone)]
pub struct ManagerHandle {
    commands: mpsc::UnboundedSender<Envelope>,
    next_request: Arc<AtomicU64>,
    epoch: tokio::time::Instant,
    claim_timeout: Duration,
    shutdown: CancellationToken,
}
impl ManagerHandle {
    fn request_id(&self) -> RequestId {
        RequestId(
            self.next_request
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .expect("request identity exhausted"),
        )
    }

    #[hotpath::measure]
    pub async fn attach(
        &self,
        template: &str,
        lease: LeaseId,
    ) -> Result<LeaseSession, AttachError> {
        if self.shutdown.is_cancelled() {
            return Err(AttachError::EngineUnavailable);
        }
        let started = tokio::time::Instant::now();
        let id = self.request_id();
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Envelope {
                message: EngineMessage::AttachOrJoin {
                    template: template.into(),
                    lease,
                    reply: id,
                    message_time: Tick(started.duration_since(self.epoch)),
                },
                reply: Some((id, tx)),
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

    /// Acknowledges logical closure; deletion happens asynchronously.
    #[hotpath::measure]
    pub async fn release(&self, lease: LeaseId) -> Result<(), ReleaseError> {
        if self.shutdown.is_cancelled() {
            return Err(ReleaseError::EngineUnavailable);
        }
        let id = self.request_id();
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Envelope {
                message: EngineMessage::ReleaseLease { lease, reply: id },
                reply: Some((id, tx)),
            })
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

pub struct LeaseSession {
    pub database_id: DatabaseId,
    pub target: Arc<PgTarget>,
    key: LeaseKey,
    cancellation: CancellationToken,
    commands: mpsc::WeakUnboundedSender<Envelope>,
    detach_on_drop: bool,
}
impl LeaseSession {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}
impl Drop for LeaseSession {
    fn drop(&mut self) {
        if self.detach_on_drop
            && let Some(sender) = self.commands.upgrade()
        {
            let _ = sender.send(Envelope {
                message: EngineMessage::Detach {
                    lease: self.key.lease.clone(),
                    generation: self.key.generation,
                },
                reply: None,
            });
        }
    }
}

/// Owns explicit shutdown for three independent actors.
pub struct TokioRuntime {
    manager: ManagerHandle,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}
impl TokioRuntime {
    pub async fn start(
        postgres_config: PostgresConfig,
        engine_config: WorkerEngineConfig,
    ) -> Result<Self, StartError> {
        let prepared = PreparedPostgres::prepare(postgres_config).await?;
        let template = prepared.metadata.template.clone();
        let (runtime, inbox) = Self::new(&engine_config);
        let creation = CreationHandle::new(
            &prepared.config,
            prepared.metadata,
            runtime.manager.commands.clone(),
            runtime.tracker.clone(),
            runtime.shutdown.clone(),
        )
        .await;
        let creation = match creation {
            Ok(handle) => handle,
            Err(error) => {
                runtime.shutdown().await;
                return Err(error.into());
            }
        };
        let cleanup = CleanupHandle::new(
            &prepared.config,
            runtime.manager.commands.clone(),
            runtime.tracker.clone(),
            runtime.shutdown.clone(),
        )
        .await;
        let cleanup = match cleanup {
            Ok(handle) => handle,
            Err(error) => {
                runtime.shutdown().await;
                return Err(error.into());
            }
        };
        runtime.start_manager(creation, cleanup, template, engine_config, inbox).await
    }

    fn new(config: &WorkerEngineConfig) -> (Self, mpsc::UnboundedReceiver<Envelope>) {
        let shutdown = CancellationToken::new();
        let (commands, inbox) = mpsc::unbounded_channel();
        let manager = ManagerHandle {
            commands,
            next_request: Arc::new(AtomicU64::new(1)),
            epoch: tokio::time::Instant::now(),
            claim_timeout: Duration::from_millis(config.lease_claim_timeout_ms),
            shutdown: shutdown.clone(),
        };
        (Self { manager, shutdown, tracker: TaskTracker::new() }, inbox)
    }

    async fn start_manager(
        self,
        creation: CreationHandle,
        cleanup: CleanupHandle,
        template: String,
        config: WorkerEngineConfig,
        inbox: mpsc::UnboundedReceiver<Envelope>,
    ) -> Result<Self, StartError> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let epoch = self.manager.epoch;
        let ports = TokioPorts {
            creation,
            cleanup,
            replies: HashMap::new(),
            cancellations: HashMap::new(),
            commands: self.manager.commands.downgrade(),
            epoch,
            shutdown: self.shutdown.clone(),
            tracker: self.tracker.clone(),
        };
        let engine = WorkerEngine::new(config, template, ports);
        tokio::spawn(self.tracker.track_future(run_manager(
            engine,
            inbox,
            ready_tx,
            epoch,
            self.shutdown.clone(),
        )));
        match ready_rx.await {
            Ok(Ok(())) => Ok(self),
            Ok(Err(error)) => {
                self.shutdown().await;
                Err(StartError::InitialDatabaseCreation(error))
            }
            Err(_) => {
                self.shutdown().await;
                Err(StartError::RuntimeStopped)
            }
        }
    }

    pub fn handle(&self) -> ManagerHandle {
        self.manager.clone()
    }

    #[hotpath::measure]
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}
impl Drop for TokioRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct TokioPorts {
    creation: CreationHandle,
    cleanup: CleanupHandle,
    replies: HashMap<RequestId, oneshot::Sender<Reply>>,
    cancellations: HashMap<LeaseKey, CancellationToken>,
    commands: mpsc::WeakUnboundedSender<Envelope>,
    epoch: tokio::time::Instant,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}
impl EngineIO for TokioPorts {
    fn request_creation(&mut self, request: CreateDatabases) -> Result<(), IOError> {
        self.creation.create(request).map_err(|_| IOError::FailedToSendTheMessage)
    }

    fn request_cleanup(&mut self, request: CleanupDatabase) -> Result<(), IOError> {
        self.cleanup.delete(request).map_err(|_| IOError::FailedToSendTheMessage)
    }

    fn reply(&mut self, id: RequestId, reply: ConsumerReply) -> Result<(), ConsumerReply> {
        let Some(sender) = self.replies.remove(&id) else {
            return Err(reply);
        };
        let message = match &reply {
            ConsumerReply::Attached { database_id, target, key } => {
                let cancellation = self
                    .cancellations
                    .entry(key.clone())
                    .or_insert_with(|| self.shutdown.child_token())
                    .clone();
                Reply::Attached(LeaseSession {
                    database_id: *database_id,
                    target: target.clone(),
                    key: key.clone(),
                    cancellation,
                    commands: self.commands.clone(),
                    detach_on_drop: true,
                })
            }
            _ => Reply::Engine(reply.clone()),
        };
        sender.send(message).map_err(|mut unsent| {
            if let Reply::Attached(session) = &mut unsent {
                session.detach_on_drop = false;
            }
            reply
        })
    }

    fn schedule(&mut self, key: LeaseKey, deadline: Tick, message: EngineMessage) {
        let cancel =
            self.cancellations.entry(key).or_insert_with(|| self.shutdown.child_token()).clone();
        let commands = self.commands.clone();
        let until = self.epoch + deadline.0;
        self.tracker.spawn(async move {
            tokio::select! { biased;
                _ = cancel.cancelled() => {},
                _ = tokio::time::sleep_until(until) => {
                    if let Some(sender) = commands.upgrade() {
                        let _ = sender.send(Envelope { message, reply: None });
                    }
                }
            }
        });
    }

    fn cancel_sessions(&mut self, key: &LeaseKey) {
        if let Some(token) = self.cancellations.remove(key) {
            token.cancel();
        }
    }
}

async fn run_manager(
    mut engine: WorkerEngine<TokioPorts>,
    mut commands: mpsc::UnboundedReceiver<Envelope>,
    ready: oneshot::Sender<Result<(), BackendError>>,
    epoch: tokio::time::Instant,
    shutdown: CancellationToken,
) {
    engine.initialize();
    let mut ready = Some(ready);
    loop {
        if let Some(result) = engine.startup_result()
            && let Some(sender) = ready.take()
        {
            let _ = sender.send(result.clone());
        }
        let message = tokio::select! {
            _ = shutdown.cancelled() => break,
            command = commands.recv() => {
                let Some(envelope) = command else { break; };
                if let Some((id, sender)) = envelope.reply { engine.io_mut().replies.insert(id, sender); }
                envelope.message
            },

        };
        engine.handle(message, Tick(epoch.elapsed()));
        // Release dead response senders even if no database supply arrives.
        engine.io_mut().replies.retain(|_, sender| !sender.is_closed());
        if engine.is_stopped() {
            break;
        }
    }
    engine.shutdown();
}
