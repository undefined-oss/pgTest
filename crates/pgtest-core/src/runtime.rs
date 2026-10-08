//! Initialization and shutdown ownership for the three Tokio actors.
use std::{collections::HashMap, time::Duration};

use pgtest_database_operations::{
    backend::{BootstrapError, PreparedPostgres},
    cleanup_worker_handle::CleanupHandle,
    config::PostgresConfig,
    creation_worker_handle::CreationHandle,
    errors::PostgresClientError,
};
use pgtest_engine_backend::{BackendError, jobs::DatabaseWorkerMessages};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    config::ManagerConfig,
    manager_handle::{
        ManagerHandle, TokioMessage,
        database_inventory::DatabaseInventory,
        manager_worker::{ManagerWorker, TokioPorts},
        messages::ManagerMessage,
    },
};

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

/// Owns explicit shutdown for three independent actors.
pub struct TokioRuntime {
    manager: ManagerHandle,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}
impl TokioRuntime {
    pub async fn start(
        postgres_config: PostgresConfig,
        manager_config: ManagerConfig,
    ) -> Result<Self, StartError> {
        let prepared = PreparedPostgres::prepare(postgres_config).await?;
        let (runtime, inbox) = Self::new(&manager_config);
        let workers = tokio::try_join!(
            CreationHandle::new(
                &prepared.config,
                prepared.metadata,
                runtime.manager.commands.clone(),
                runtime.tracker.clone(),
                runtime.shutdown.clone(),
            ),
            CleanupHandle::new(
                &prepared.config,
                runtime.manager.commands.clone(),
                runtime.tracker.clone(),
                runtime.shutdown.clone(),
            ),
        );
        let (creation, cleanup) = match workers {
            Ok(handles) => handles,
            Err(error) => {
                runtime.shutdown().await;
                return Err(error.into());
            }
        };
        runtime.start_manager(creation, cleanup, manager_config, inbox).await
    }

    fn new(config: &ManagerConfig) -> (Self, mpsc::UnboundedReceiver<TokioMessage>) {
        let shutdown = CancellationToken::new();
        let (commands, inbox) = mpsc::unbounded_channel();
        let manager = ManagerHandle {
            commands,
            epoch: tokio::time::Instant::now(),
            claim_timeout: Duration::from_nanos_u128(
                config
                    .lease_claim_timeout_ms
                    .checked_mul(1_000_000)
                    .expect("lease timeout is too large"),
            ),
            shutdown: shutdown.clone(),
        };
        (Self { manager, shutdown, tracker: TaskTracker::new() }, inbox)
    }

    async fn start_manager(
        self,
        creation: CreationHandle,
        cleanup: CleanupHandle,
        config: ManagerConfig,
        mut inbox: mpsc::UnboundedReceiver<TokioMessage>,
    ) -> Result<Self, StartError> {
        let inventory = match Self::create_initial(&creation, &mut inbox, config).await {
            Ok(inventory) => inventory,
            Err(error) => {
                self.shutdown().await;
                return Err(error);
            }
        };
        let epoch = self.manager.epoch;
        let ports = TokioPorts {
            creation,
            cleanup,
            cancellations: HashMap::new(),
            commands: self.manager.commands.clone(),
            epoch,
            shutdown: self.shutdown.clone(),
            tracker: self.tracker.clone(),
        };
        let manager = ManagerWorker::new(config, inventory, ports);
        tokio::spawn(self.tracker.track_future(manager.run(inbox, epoch)));
        Ok(self)
    }

    async fn create_initial(
        creation: &CreationHandle,
        inbox: &mut mpsc::UnboundedReceiver<TokioMessage>,
        config: ManagerConfig,
    ) -> Result<DatabaseInventory, StartError> {
        let mut inventory = DatabaseInventory::default();
        if let Some(request) = inventory.reserve_creations(usize::from(*config.initial_slots)) {
            creation.create(request).map_err(|_| StartError::RuntimeStopped)?;
        }
        while !inventory.creating().is_empty() {
            let message = inbox.recv().await.ok_or(StartError::RuntimeStopped)?;
            // The manager handle is not exposed until this batch completes.
            let ManagerMessage::DatabaseWorker(DatabaseWorkerMessages::CreationFinished {
                database_id,
                result,
            }) = message
            else {
                unreachable!("only creation results can arrive during startup");
            };
            if let Some(result) = inventory.complete_creation(database_id, result) {
                result?;
            }
        }
        Ok(inventory)
    }

    pub fn handle(&self) -> ManagerHandle {
        self.manager.clone()
    }

    #[hotpath::measure]
    pub async fn shutdown(self) {
        self.request_shutdown();
        self.tracker.close();
        self.tracker.wait().await;
    }

    fn request_shutdown(&self) {
        self.shutdown.cancel();
        let _ = self.manager.commands.send(ManagerMessage::Shutdown);
    }
}
impl Drop for TokioRuntime {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

#[cfg(all(test, feature = "runtime-tests"))]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use pgtest_engine_backend::{
        BackendError, DatabaseCleaner, DatabaseCreator, PgEndpoint, PgTarget, ProvisionedDatabase,
        ResourceId, jobs::DatabaseId,
    };
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::{
        manager_handle::{
            LeaseId, Reply,
            errors::AttachError,
            messages::{ConsumerReply, ElapsedTime, ManagerMessage},
        },
        runtime::{StartError, TokioRuntime},
    };

    struct RuntimeConfig {
        engine: ManagerConfig,
    }

    type CreateReply = oneshot::Sender<Result<ProvisionedDatabase, BackendError>>;
    type DeleteReply = oneshot::Sender<Result<(), BackendError>>;
    struct ControlledBackend {
        creates: mpsc::UnboundedSender<CreateReply>,
        deletes: mpsc::UnboundedSender<(ResourceId, DeleteReply)>,
    }
    impl DatabaseCreator for ControlledBackend {
        async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
            let (tx, rx) = oneshot::channel();
            self.creates.send(tx).unwrap();
            rx.await.unwrap()
        }
    }
    impl DatabaseCleaner for ControlledBackend {
        async fn delete_database(&self, id: ResourceId) -> Result<(), BackendError> {
            let (tx, rx) = oneshot::channel();
            self.deletes.send((id, tx)).unwrap();
            rx.await.unwrap()
        }
    }
    fn backend() -> (
        Arc<ControlledBackend>,
        mpsc::UnboundedReceiver<CreateReply>,
        mpsc::UnboundedReceiver<(ResourceId, DeleteReply)>,
    ) {
        let (creates, rx) = mpsc::unbounded_channel();
        let (deletes, drops) = mpsc::unbounded_channel();
        (Arc::new(ControlledBackend { creates, deletes }), rx, drops)
    }
    fn config(initial: u16, batch: u16) -> RuntimeConfig {
        RuntimeConfig {
            engine: ManagerConfig {
                initial_slots: initial.into(),
                starvation_threshold: 0.into(),
                grow_batch_size: batch.into(),
                ..ManagerConfig::default()
            },
        }
    }
    fn db(name: &str) -> ProvisionedDatabase {
        ProvisionedDatabase {
            resource_id: ResourceId(format!("provider/{name}")),
            target: PgTarget {
                database: name.into(),
                endpoint: PgEndpoint::Tcp { host: format!("{name}.example"), port: 5555 },
            },
        }
    }
    async fn next<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(3), rx.recv()).await.unwrap().unwrap()
    }
    async fn start(
        initial: u16,
    ) -> (
        TokioRuntime,
        mpsc::UnboundedReceiver<CreateReply>,
        mpsc::UnboundedReceiver<(ResourceId, DeleteReply)>,
    ) {
        let (backend, mut creates, drops) = backend();
        let pending = tokio::spawn(start_runtime(backend, config(initial, 0)));
        for n in 0..initial {
            next(&mut creates).await.send(Ok(db(&format!("db{n}")))).unwrap();
        }
        (pending.await.unwrap().unwrap(), creates, drops)
    }

    #[tokio::test]
    async fn sharing_release_and_provider_identity() {
        let (runtime, _, mut drops) = start(2).await;
        let manager = runtime.handle();
        let a = manager.attach(LeaseId::new("same").unwrap()).await.unwrap();
        let b = manager.attach(LeaseId::new("same").unwrap()).await.unwrap();
        assert_eq!(a.database_id, b.database_id);
        assert_eq!(a.target, b.target);
        assert_eq!(a.target.database, "db0");
        manager.release(LeaseId::new("same").unwrap()).await.unwrap();
        assert!(a.cancellation_token().is_cancelled());
        assert!(b.cancellation_token().is_cancelled());
        let (id, reply) = next(&mut drops).await;
        assert_eq!(id, ResourceId("provider/db0".into()));
        reply.send(Ok(())).unwrap();
        let fresh = manager.attach(LeaseId::new("same").unwrap()).await.unwrap();
        assert_ne!(fresh.database_id, a.database_id);
        assert_eq!(fresh.target.database, "db1");
        assert!(!fresh.cancellation_token().is_cancelled());
        runtime.shutdown().await;
    }
    #[tokio::test]
    async fn dropped_session_keeps_assignment_and_shutdown_cancels_session() {
        let (runtime, _, mut drops) = start(1).await;
        let manager = runtime.handle();
        let a = manager.attach(LeaseId::new("same").unwrap()).await.unwrap();
        let id = a.database_id;
        drop(a);
        let b = manager.attach(LeaseId::new("same").unwrap()).await.unwrap();
        assert_eq!(b.database_id, id);
        assert!(drops.try_recv().is_err());
        runtime.shutdown().await;
        assert!(b.cancellation_token().is_cancelled());
        assert!(matches!(
            manager.attach(LeaseId::new("same").unwrap()).await,
            Err(AttachError::EngineUnavailable)
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn claim_timeout_and_lifetime_use_runtime_clock() {
        let (runtime, _, mut drops) = start(1).await;
        let manager = runtime.handle();
        let session = manager.attach(LeaseId::new("held").unwrap()).await.unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        next(&mut drops).await.1.send(Ok(())).unwrap();
        assert!(session.cancellation_token().is_cancelled());
        let pending = manager.attach(LeaseId::new("waiting").unwrap());
        assert!(matches!(pending.await, Err(AttachError::TimedOut)));
        runtime.shutdown().await;
    }
    #[tokio::test]
    async fn cleanup_does_not_block_creation_or_release() {
        let (backend, mut creates, mut drops) = backend();
        let task = tokio::spawn(start_runtime(backend, config(1, 1)));
        next(&mut creates).await.send(Ok(db("first"))).unwrap();
        let runtime = task.await.unwrap().unwrap();
        let manager = runtime.handle();
        let first = manager.attach(LeaseId::new("first").unwrap()).await.unwrap();
        let replenishment = next(&mut creates).await;
        manager.release(LeaseId::new("first").unwrap()).await.unwrap();
        let (_, blocked_drop) = next(&mut drops).await;
        assert!(first.cancellation_token().is_cancelled());
        replenishment.send(Ok(db("second"))).unwrap();
        let second = manager.attach(LeaseId::new("second").unwrap()).await.unwrap();
        assert_eq!(second.target.database, "second");
        runtime.shutdown().await;
        assert!(blocked_drop.is_closed());
    }
    #[tokio::test]
    async fn startup_waits_for_all_successes_and_limits_concurrency() {
        let (backend, mut creates, _) = backend();
        let config = config(3, 0);
        let (runtime, inbox) = TokioRuntime::new(&config.engine);
        let tracker = runtime.tracker.clone();
        let creation = CreationHandle::new_with_client(
            backend.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime.manager.commands.clone(),
            tracker.clone(),
            runtime.shutdown.clone(),
        );
        let cleanup = CleanupHandle::new_with_client(
            backend,
            NonZeroUsize::new(2).unwrap(),
            runtime.manager.commands.clone(),
            tracker.clone(),
            runtime.shutdown.clone(),
        );
        let task = tokio::spawn(runtime.start_manager(creation, cleanup, config.engine, inbox));
        let first = next(&mut creates).await;
        let second = next(&mut creates).await;
        assert_eq!(tracker.len(), 2, "Manager must not spawn before initial creation finishes");
        assert!(creates.try_recv().is_err());
        second.send(Ok(db("second"))).unwrap();
        let third = next(&mut creates).await;
        assert!(!task.is_finished());
        third.send(Ok(db("third"))).unwrap();
        assert!(!task.is_finished());
        first.send(Ok(db("first"))).unwrap();
        let runtime = task.await.unwrap().unwrap();
        assert_eq!(tracker.len(), 3);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn startup_failure_cancels_remaining_creation_without_waiting_for_results() {
        let (backend, mut creates, _) = backend();
        let task = tokio::spawn(start_runtime(backend, config(2, 0)));
        let first = next(&mut creates).await;
        let pending = next(&mut creates).await;
        first.send(Err(BackendError::OperationFailed("creation failed".into()))).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("startup failure must not wait for the remaining creation")
            .unwrap();
        assert!(matches!(result, Err(StartError::InitialDatabaseCreation(_))));
        assert!(pending.is_closed());
    }

    #[tokio::test]
    async fn cancelled_startup_cancels_inflight_provider_future() {
        let (backend, mut creates, _) = backend();
        let task = tokio::spawn(start_runtime(backend, config(1, 0)));
        let mut reply = next(&mut creates).await;
        task.abort();
        let _ = task.await;
        tokio::time::timeout(Duration::from_secs(3), reply.closed()).await.unwrap();
    }
    #[tokio::test]
    async fn timed_out_reply_leaves_created_database_ready() {
        let (backend, mut creates, _) = backend();
        let mut cfg = config(0, 1);
        cfg.engine.lease_claim_timeout_ms = 10;
        let runtime = start_runtime(backend, cfg).await.unwrap();
        let handle = runtime.handle();
        assert!(matches!(
            handle.attach(LeaseId::new("expired").unwrap()).await,
            Err(AttachError::TimedOut)
        ));
        next(&mut creates).await.send(Ok(db("available"))).unwrap();
        let session = handle.attach(LeaseId::new("live").unwrap()).await.unwrap();
        assert_eq!(session.target.database, "available");
        runtime.shutdown().await;
    }
    #[tokio::test]
    async fn shutdown_cancels_queued_and_active_operations() {
        let (backend, mut creates, _) = backend();
        let runtime = start_runtime(backend, config(0, 16)).await.unwrap();
        let handle = runtime.handle();
        let attach = tokio::spawn(async move { handle.attach(LeaseId::new("a").unwrap()).await });
        let mut first = next(&mut creates).await;
        let mut second = next(&mut creates).await;
        runtime.shutdown().await;
        first.closed().await;
        second.closed().await;
        assert!(matches!(attach.await.unwrap(), Err(AttachError::EngineUnavailable)));
        assert!(creates.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_waiter_receives_timeout_and_preserves_supply() {
        let (backend, mut creates, _) = backend();
        let mut cfg = config(0, 1);
        cfg.engine.lease_claim_timeout_ms = 10;
        let runtime = start_runtime(backend, cfg).await.unwrap();
        let handle = runtime.handle();
        let (reply, response) = oneshot::channel();
        handle
            .commands
            .send(ManagerMessage::AttachOrJoin {
                lease: LeaseId::new("expired").unwrap(),
                reply,
                message_time: ElapsedTime::default(),
            })
            .unwrap();
        let creation = next(&mut creates).await;
        tokio::time::advance(Duration::from_millis(11)).await;
        creation.send(Ok(db("available"))).unwrap();
        assert!(matches!(
            response.await.unwrap(),
            Reply::Engine(ConsumerReply::AttachRejected(AttachError::TimedOut))
        ));
        let session = handle.attach(LeaseId::new("live").unwrap()).await.unwrap();
        assert_eq!(session.target.database, "available");
        runtime.shutdown().await;
    }
    #[tokio::test]
    async fn startup_future_fits_stack_budget() {
        let (backend, ..) = backend();
        let future = start_runtime(backend, config(16, 16));
        assert!(std::mem::size_of_val(&future) < 64 * 1024);
    }

    struct ImmediateBackend {
        sequence: Mutex<u64>,
    }
    impl DatabaseCreator for ImmediateBackend {
        async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
            let mut n = self.sequence.lock().unwrap();
            *n += 1;
            Ok(db(&format!("db{}", *n)))
        }
    }
    impl DatabaseCleaner for ImmediateBackend {
        async fn delete_database(&self, _: ResourceId) -> Result<(), BackendError> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn simulation_and_tokio_have_equivalent_lease_observations() {
        use crate::simulation::{ReplySlot, SimRuntime};
        let cfg = config(2, 0);
        let mut sim = SimRuntime::new(cfg.engine);
        sim.run_until_idle(100).unwrap();
        for id in sim.active_creations() {
            sim.complete_creation(id, Ok(db(&format!("db{}", id.0))));
        }
        sim.run_until_idle(100).unwrap();
        let a = sim.attach("a");
        let b = sim.attach("a");
        sim.run_until_idle(100).unwrap();
        let runtime = start_runtime(Arc::new(ImmediateBackend { sequence: Mutex::new(0) }), cfg)
            .await
            .unwrap();
        let h = runtime.handle();
        let real_a = h.attach(LeaseId::new("a").unwrap()).await.unwrap();
        let real_b = h.attach(LeaseId::new("a").unwrap()).await.unwrap();
        for (sim_id, real) in [(a, &real_a), (b, &real_b)] {
            let ReplySlot::Delivered(ConsumerReply::Attached { database_id, target, .. }) =
                sim.reply(sim_id)
            else {
                panic!()
            };
            assert_eq!(database_id, real.database_id);
            assert_eq!(target, real.target);
        }
        sim.release("a");
        sim.run_until_idle(100).unwrap();
        h.release(LeaseId::new("a").unwrap()).await.unwrap();
        assert!(real_a.cancellation_token().is_cancelled());
        let again = sim.attach("a");
        sim.run_until_idle(100).unwrap();
        let real_again = h.attach(LeaseId::new("a").unwrap()).await.unwrap();
        let ReplySlot::Delivered(ConsumerReply::Attached { database_id, target, .. }) =
            sim.reply(again)
        else {
            panic!("expected a fresh assignment after release")
        };
        assert_eq!(database_id, real_again.database_id);
        assert_eq!(target, real_again.target);
        assert_ne!(real_again.database_id, real_a.database_id);
        assert!(!real_again.cancellation_token().is_cancelled());
        runtime.shutdown().await;
    }

    async fn start_runtime<DatabaseClient: DatabaseCreator + DatabaseCleaner>(
        backend: Arc<DatabaseClient>,
        config: RuntimeConfig,
    ) -> Result<TokioRuntime, StartError> {
        let (runtime, inbox) = TokioRuntime::new(&config.engine);
        let creation = CreationHandle::new_with_client(
            backend.clone(),
            NonZeroUsize::new(2).unwrap(),
            runtime.manager.commands.clone(),
            runtime.tracker.clone(),
            runtime.shutdown.clone(),
        );
        let cleanup = CleanupHandle::new_with_client(
            backend,
            NonZeroUsize::new(2).unwrap(),
            runtime.manager.commands.clone(),
            runtime.tracker.clone(),
            runtime.shutdown.clone(),
        );
        runtime.start_manager(creation, cleanup, config.engine, inbox).await
    }

    #[tokio::test]
    async fn failed_replies_restore_supply_and_unread_replies_keep_assignment() {
        let runtime =
            start_runtime(Arc::new(ImmediateBackend { sequence: Mutex::new(0) }), config(1, 0))
                .await
                .unwrap();
        let handle = runtime.handle();
        let (tx, mut rx) = oneshot::channel();
        handle
            .commands
            .send(ManagerMessage::AttachOrJoin {
                lease: LeaseId::new("a").unwrap(),
                reply: tx,
                message_time: ElapsedTime::default(),
            })
            .unwrap();
        loop {
            match rx.try_recv() {
                Ok(reply) => {
                    drop(reply);
                    break;
                }
                Err(oneshot::error::TryRecvError::Empty) => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
        let session = handle.attach(LeaseId::new("a").unwrap()).await.unwrap();
        assert_eq!(session.database_id, DatabaseId(1));
        runtime.shutdown().await;
        let runtime =
            start_runtime(Arc::new(ImmediateBackend { sequence: Mutex::new(0) }), config(1, 0))
                .await
                .unwrap();
        let handle = runtime.handle();
        let (tx, rx) = oneshot::channel();
        drop(rx);
        handle
            .commands
            .send(ManagerMessage::AttachOrJoin {
                lease: LeaseId::new("lost").unwrap(),
                reply: tx,
                message_time: ElapsedTime::default(),
            })
            .unwrap();
        assert!(handle.attach(LeaseId::new("live").unwrap()).await.is_ok());
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_runtime_cancels_workers_and_closes_manager_inbox() {
        let (runtime, ..) = start(1).await;
        let manager = runtime.handle();
        let tracker = runtime.tracker.clone();
        drop(runtime);
        tracker.close();
        tokio::time::timeout(Duration::from_secs(3), tracker.wait()).await.unwrap();
        manager.stopped().await;
        assert!(matches!(
            manager.attach(LeaseId::new("a").unwrap()).await,
            Err(AttachError::EngineUnavailable)
        ));
    }

    #[tokio::test]
    async fn shutdown_waits_for_all_three_workers_and_timers() {
        let (runtime, ..) = start(1).await;
        let tracker = runtime.tracker.clone();
        assert_eq!(tracker.len(), 3);
        let manager = runtime.handle();
        let session = manager.attach(LeaseId::new("a").unwrap()).await.unwrap();
        assert!(tracker.len() > 3);
        runtime.shutdown().await;
        assert!(tracker.is_empty());
        assert!(session.cancellation_token().is_cancelled());
        manager.stopped().await;
    }
}
