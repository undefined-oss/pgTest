//! Tokio driver for the shared lifecycle actors.
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use pgtest_engine_backend::{AsyncDatabaseBackend, BackendError, PgTarget, ResourceId};
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::worker_engine::{
    core::{LeaseId, WorkerEngine, WorkerEngineConfig},
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId},
    errors::{AttachError, IOError, ReleaseError},
    messages::{ConsumerReply, EngineMessage, LeaseKey, RequestId, Tick},
    traits::EngineIO,
};
mod database_workers;
#[cfg(all(test, feature = "runtime-tests"))]
mod tests;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub template: String,
    pub engine: WorkerEngineConfig,
    pub creation_concurrency: NonZeroUsize,
    pub cleanup_concurrency: NonZeroUsize,
    pub stale_resources: Vec<ResourceId>,
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("initial database creation failed: {0}")]
    InitialDatabaseCreation(#[from] BackendError),
    #[error("actor runtime stopped during startup")]
    RuntimeStopped,
}
#[derive(Debug, thiserror::Error)]
#[error("actor runtime failed: {0}")]
pub struct RuntimeError(pub String);

enum Reply {
    Attached(LeaseSession),
    Engine(ConsumerReply),
}
struct Envelope {
    message: EngineMessage,
    reply: Option<(RequestId, oneshot::Sender<Reply>)>,
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
        self.shutdown.cancelled().await;
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

/// Runtime owns tasks; handles only submit requests. Dropping it cancels tasks.
pub struct TokioRuntime {
    manager: ManagerHandle,
    shutdown: CancellationToken,
    supervisor: Option<JoinHandle<Result<(), RuntimeError>>>,
}
impl TokioRuntime {
    pub async fn start<B: AsyncDatabaseBackend>(
        backend: Arc<B>,
        config: RuntimeConfig,
    ) -> Result<Self, StartError> {
        let shutdown = CancellationToken::new();
        let epoch = tokio::time::Instant::now();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (create_tx, create_rx) = mpsc::unbounded_channel();
        let (cleanup_tx, cleanup_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let tracker = TaskTracker::new();
        let manager = ManagerHandle {
            commands: commands_tx.clone(),
            next_request: Arc::new(AtomicU64::new(1)),
            epoch,
            claim_timeout: Duration::from_millis(config.engine.lease_claim_timeout_ms),
            shutdown: shutdown.clone(),
        };
        let ports = TokioPorts {
            creation: create_tx,
            cleanup: cleanup_tx,
            replies: HashMap::new(),
            cancellations: HashMap::new(),
            commands: commands_tx.downgrade(),
            events: events_tx.clone(),
            epoch,
            shutdown: shutdown.clone(),
            tracker: tracker.clone(),
        };
        drop(commands_tx);
        let engine = WorkerEngine::new(config.engine, config.template, ports);
        let mut actors = JoinSet::new();
        actors.spawn(run_manager(
            engine,
            commands_rx,
            events_rx,
            config.stale_resources,
            ready_tx,
            epoch,
            shutdown.clone(),
        ));
        actors.spawn(database_workers::run_creation(
            backend.clone(),
            config.creation_concurrency,
            create_rx,
            events_tx.clone(),
            shutdown.clone(),
        ));
        actors.spawn(database_workers::run_cleanup(
            backend,
            config.cleanup_concurrency,
            cleanup_rx,
            events_tx,
            shutdown.clone(),
        ));
        let cancellation = shutdown.clone();
        let supervisor = tokio::spawn(async move {
            let mut failure = None;
            while let Some(result) = actors.join_next().await {
                match result {
                    Err(error) => {
                        failure.get_or_insert(RuntimeError(error.to_string()));
                    }
                    Ok(Err(error)) => {
                        failure.get_or_insert(error);
                    }
                    Ok(Ok(())) if !cancellation.is_cancelled() => {
                        failure.get_or_insert(RuntimeError("actor exited unexpectedly".into()));
                    }
                    Ok(Ok(())) => {}
                }
                cancellation.cancel();
            }
            tracker.close();
            tracker.wait().await;
            failure.map_or(Ok(()), Err)
        });
        let runtime = Self { manager, shutdown, supervisor: Some(supervisor) };
        match ready_rx.await {
            Ok(Ok(())) if !runtime.shutdown.is_cancelled() => Ok(runtime),
            Ok(Err(error)) => {
                let _ = runtime.shutdown().await;
                Err(StartError::InitialDatabaseCreation(error))
            }
            _ => {
                let _ = runtime.shutdown().await;
                Err(StartError::RuntimeStopped)
            }
        }
    }

    pub fn handle(&self) -> ManagerHandle {
        self.manager.clone()
    }

    #[hotpath::measure]
    pub async fn shutdown(mut self) -> Result<(), RuntimeError> {
        self.shutdown.cancel();
        self.supervisor
            .take()
            .expect("runtime owns supervisor")
            .await
            .map_err(|e| RuntimeError(e.to_string()))?
    }
}
impl Drop for TokioRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct TokioPorts {
    creation: mpsc::UnboundedSender<CreateDatabases>,
    cleanup: mpsc::UnboundedSender<CleanupDatabase>,
    replies: HashMap<RequestId, oneshot::Sender<Reply>>,
    cancellations: HashMap<LeaseKey, CancellationToken>,
    commands: mpsc::WeakUnboundedSender<Envelope>,
    events: mpsc::UnboundedSender<EngineMessage>,
    epoch: tokio::time::Instant,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}
impl EngineIO for TokioPorts {
    fn request_creation(&mut self, request: CreateDatabases) -> Result<(), IOError> {
        self.creation.send(request).map_err(|_| IOError::FailedToSendTheMessage)
    }

    fn request_cleanup(&mut self, request: CleanupDatabase) -> Result<(), IOError> {
        self.cleanup.send(request).map_err(|_| IOError::FailedToSendTheMessage)
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
        let events = self.events.clone();
        let until = self.epoch + deadline.0;
        self.tracker.spawn(async move {
            tokio::select! { biased;
                _ = cancel.cancelled() => {},
                _ = tokio::time::sleep_until(until) => { let _ = events.send(message); }
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
    mut events: mpsc::UnboundedReceiver<EngineMessage>,
    stale: Vec<ResourceId>,
    ready: oneshot::Sender<Result<(), BackendError>>,
    epoch: tokio::time::Instant,
    shutdown: CancellationToken,
) -> Result<(), RuntimeError> {
    engine.initialize(stale);
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
                let Some(envelope) = command else { shutdown.cancel(); break; };
                if let Some((id, sender)) = envelope.reply { engine.io_mut().replies.insert(id, sender); }
                envelope.message
            },
            event = events.recv() => {
                let Some(message) = event else { break; }; message
            }
        };
        engine.handle(message, Tick(epoch.elapsed()));
        // Release dead response senders even if no database supply arrives.
        engine.io_mut().replies.retain(|_, sender| !sender.is_closed());
        if engine.is_stopped() {
            break;
        }
    }
    engine.shutdown();
    Ok(())
}
