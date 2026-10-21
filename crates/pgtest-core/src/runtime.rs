//! Initialization and shutdown ownership for the three Tokio actors.
use std::{collections::HashMap, time::Duration};

use hotpath::wrap::tokio::sync::mpsc::UnboundedReceiver;
use pgtest_database_operations::{
    backend::{BootstrapError, PreparedPostgres},
    cleanup_worker_handle::CleanupHandle,
    config::PostgresConfig,
    creation_worker_handle::CreationHandle,
    errors::PostgresClientError,
};
use pgtest_engine_backend::{BackendError, jobs::DatabaseWorkerMessages};
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

    fn new(config: &ManagerConfig) -> (Self, UnboundedReceiver<TokioMessage>) {
        let shutdown = CancellationToken::new();
        let (commands, inbox) =
            hotpath::channel!(tokio::sync::mpsc::unbounded_channel(), label = "manager");
        let manager = ManagerHandle {
            leases: Default::default(),
            waiting: Default::default(),
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
        mut inbox: UnboundedReceiver<TokioMessage>,
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
            leases: self.manager.leases.clone(),
            waiting: self.manager.waiting.clone(),
            creation,
            cleanup,
            cancellations: HashMap::new(),
            commands: self.manager.commands.clone(),
            epoch,
            shutdown: self.shutdown.clone(),
            tracker: self.tracker.clone(),
        };
        let mut manager = ManagerWorker::new(config, inventory, ports);
        manager.leases = self.manager.leases.clone();
        tokio::spawn(self.tracker.track_future(manager.run(inbox, epoch)));
        Ok(self)
    }

    async fn create_initial(
        creation: &CreationHandle,
        inbox: &mut UnboundedReceiver<TokioMessage>,
        config: ManagerConfig,
    ) -> Result<DatabaseInventory, StartError> {
        let mut inventory = DatabaseInventory::default();
        if let Some(request) = inventory.reserve_creations(usize::from(*config.initial_slots)) {
            creation.create(request).map_err(|_| StartError::RuntimeStopped)?;
        }
        while !inventory.creating().is_empty() {
            let message = inbox.recv().await.ok_or(StartError::RuntimeStopped)?;
            // The manager handle is not exposed until this batch completes.
            let ManagerMessage::DatabaseWorker(DatabaseWorkerMessages::CreationsFinished {
                results,
            }) = message
            else {
                unreachable!("only creation results can arrive during startup");
            };
            for result in results {
                if let Some(result) = inventory.complete_creation(result.database_id, result.result)
                {
                    result?;
                }
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
        future::Future,
        num::NonZeroUsize,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
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
        let joined = manager.attach(LeaseId::new("held").unwrap()).await.unwrap();
        tokio::time::advance(Duration::from_secs(30)).await;
        next(&mut drops).await.1.send(Ok(())).unwrap();
        assert!(session.cancellation_token().is_cancelled());
        assert!(joined.cancellation_token().is_cancelled());
        assert!(!manager.leases.pin().contains_key("held"));
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
    async fn startup_failure_waits_for_the_full_creation_batch() {
        let (backend, mut creates, _) = backend();
        let task = tokio::spawn(start_runtime(backend, config(3, 0)));
        let first = next(&mut creates).await;
        let pending = next(&mut creates).await;
        first.send(Err(BackendError::OperationFailed("creation failed".into()))).unwrap();
        let third = next(&mut creates).await;
        assert!(!task.is_finished(), "the first failure must not report a partial batch");
        third.send(Ok(db("third"))).unwrap();
        pending.send(Ok(db("second"))).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("startup must finish after all batch outcomes arrive")
            .unwrap();
        assert!(matches!(result, Err(StartError::InitialDatabaseCreation(_))));
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
        next(&mut creates).await.send(Ok(db("spare"))).unwrap();
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
        let expired = handle.attach(LeaseId::new("expired").unwrap());
        tokio::pin!(expired);
        assert!(poll_once(expired.as_mut()).is_pending());
        let creation = next(&mut creates).await;
        tokio::time::advance(Duration::from_millis(11)).await;
        assert!(matches!(expired.await, Err(AttachError::TimedOut)));
        assert_eq!(handle.waiting.len(), 0, "timeout cleans up without a database completion");
        creation.send(Ok(db("available"))).unwrap();
        next(&mut creates).await.send(Ok(db("spare"))).unwrap();
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

    // Drive the real Tokio adapter synchronously, leaving the inbox undrained
    // until the test explicitly handles a message.
    fn manual_manager(
        initial: u16,
        timeout_ms: u128,
    ) -> (TokioRuntime, ManagerWorker<TokioPorts>, UnboundedReceiver<TokioMessage>) {
        let mut cfg = config(initial, 0).engine;
        cfg.lease_claim_timeout_ms = timeout_ms;
        let (runtime, inbox) = TokioRuntime::new(&cfg);
        let backend = Arc::new(ImmediateBackend { sequence: Mutex::new(0) });
        let ports = TokioPorts {
            leases: runtime.manager.leases.clone(),
            waiting: runtime.manager.waiting.clone(),
            creation: CreationHandle::new_with_client(
                backend.clone(),
                NonZeroUsize::new(1).unwrap(),
                runtime.manager.commands.clone(),
                runtime.tracker.clone(),
                runtime.shutdown.clone(),
            ),
            cleanup: CleanupHandle::new_with_client(
                backend,
                NonZeroUsize::new(1).unwrap(),
                runtime.manager.commands.clone(),
                runtime.tracker.clone(),
                runtime.shutdown.clone(),
            ),
            cancellations: HashMap::new(),
            commands: runtime.manager.commands.clone(),
            epoch: runtime.manager.epoch,
            shutdown: runtime.shutdown.clone(),
            tracker: runtime.tracker.clone(),
        };
        let mut inventory = DatabaseInventory::default();
        if let Some(batch) = inventory.reserve_creations(usize::from(initial)) {
            for index in 0..batch.amount.get() {
                inventory
                    .complete_creation(batch.database_id(index), Ok(db(&format!("db{index}"))))
                    .unwrap()
                    .unwrap();
            }
        }
        let mut worker = ManagerWorker::new(cfg, inventory, ports);
        worker.leases = runtime.manager.leases.clone();
        (runtime, worker, inbox)
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[tokio::test(start_paused = true)]
    async fn shared_callers_keep_individual_deadlines_and_cleanup_without_manager_progress() {
        let (runtime, _worker, inbox) = manual_manager(0, 30);
        let handle = runtime.handle();
        let mut first = Box::pin(handle.attach(LeaseId::new("shared").unwrap()));
        assert!(poll_once(first.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(15)).await;
        let mut second = Box::pin(handle.attach(LeaseId::new("shared").unwrap()));
        assert!(poll_once(second.as_mut()).is_pending());
        assert_eq!(inbox.len(), 1);
        tokio::time::advance(Duration::from_millis(15)).await;
        assert!(matches!(first.await, Err(AttachError::TimedOut)));
        assert_eq!(handle.waiting.len(), 1);
        assert!(poll_once(second.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(15)).await;
        assert!(matches!(second.await, Err(AttachError::TimedOut)));
        assert_eq!(handle.waiting.len(), 0);
        assert_eq!(inbox.len(), 1, "cancellation must not send manager messages");
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_last_caller_cleans_registry_before_or_after_work_is_queued() {
        for process_work in [false, true] {
            let (runtime, mut worker, mut inbox) = manual_manager(0, 0);
            let handle = runtime.handle();
            let mut first = Box::pin(handle.attach(LeaseId::new("shared").unwrap()));
            let mut second = Box::pin(handle.attach(LeaseId::new("shared").unwrap()));
            assert!(poll_once(first.as_mut()).is_pending());
            assert!(poll_once(second.as_mut()).is_pending());
            if process_work {
                worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
            }
            drop(first);
            assert_eq!(worker.waiting_len(), 1);
            drop(second);
            assert_eq!(worker.waiting_len(), 0);
            assert_eq!(inbox.len(), usize::from(!process_work));
            worker.shutdown();
            runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn task_abort_removes_registration_and_stale_work_cannot_remove_replacement() {
        let (runtime, mut worker, mut inbox) = manual_manager(1, 0);
        let handle = runtime.handle();
        let aborted = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.attach(LeaseId::new("shared").unwrap()).await })
        };
        let old_work = inbox.recv().await.unwrap();
        assert_eq!(handle.waiting.len(), 1);
        aborted.abort();
        assert!(aborted.await.unwrap_err().is_cancelled());
        assert_eq!(handle.waiting.len(), 0);
        let mut replacement = Box::pin(handle.attach(LeaseId::new("shared").unwrap()));
        assert!(poll_once(replacement.as_mut()).is_pending());
        worker.handle(old_work, ElapsedTime::default());
        assert_eq!(handle.waiting.len(), 1);
        assert_eq!(worker.inventory.ready().len(), 1);
        worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
        assert!(replacement.await.is_ok());
        assert_eq!(handle.waiting.len(), 0);
        assert_eq!(worker.inventory.ready().len(), 0);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn unread_shared_delivery_keeps_assignment_and_late_registration_joins_it() {
        let (runtime, mut worker, mut inbox) = manual_manager(2, 0);
        let handle = runtime.handle();
        let lease = LeaseId::new("shared").unwrap();
        let mut first = Box::pin(handle.attach(lease.clone()));
        assert!(poll_once(first.as_mut()).is_pending());
        worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
        assert_eq!(handle.waiting.len(), 0);
        assert_eq!(worker.inventory.ready().len(), 1);

        // A caller may have missed the active registry just before publication,
        // then register a new pending generation just after publication.
        let (late, work) = handle.waiting.register(lease.clone());
        drop(first); // Unread success must not free the database or remove `late`.
        assert_eq!(handle.waiting.len(), 1);
        worker.handle(work.unwrap(), ElapsedTime::default());
        let Reply::Attached(late) = late.await.unwrap() else { panic!("expected session") };
        let direct = handle.attach(lease).await.unwrap();
        assert_eq!(late.database_id, direct.database_id);
        assert_eq!(late.target, direct.target);
        assert_eq!(worker.inventory.ready().len(), 1);
        assert_eq!(handle.waiting.len(), 0);
        runtime.shutdown().await;
        assert!(late.cancellation_token().is_cancelled());
        assert!(direct.cancellation_token().is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_pending_callers_share_one_work_item_and_one_database() {
        let (runtime, mut worker, mut inbox) = manual_manager(1, 0);
        let barrier = Arc::new(tokio::sync::Barrier::new(33));
        let mut callers = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let handle = runtime.handle();
            let barrier = barrier.clone();
            callers.spawn(async move {
                let mut future = Box::pin(handle.attach(LeaseId::new("shared").unwrap()));
                assert!(poll_once(future.as_mut()).is_pending());
                barrier.wait().await;
                future.await.unwrap()
            });
        }
        barrier.wait().await;
        assert_eq!(inbox.len(), 1);
        assert_eq!(worker.waiting_len(), 1);
        worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
        let sessions =
            tokio::time::timeout(Duration::from_secs(3), callers.join_all()).await.unwrap();
        assert_eq!(sessions.len(), 32);
        assert!(sessions.iter().all(|session| session.database_id == sessions[0].database_id));
        assert!(sessions.iter().all(|session| Arc::ptr_eq(&session.target, &sessions[0].target)));
        assert_eq!(worker.waiting_len(), 0);
        assert!(inbox.is_empty());
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn pending_attaches_share_one_message_and_published_attaches_bypass_inbox() {
        for timeout_ms in [0, 30_000] {
            let (runtime, mut worker, mut inbox) = manual_manager(2, timeout_ms);
            let handle = runtime.handle();
            let lease = LeaseId::new("shared").unwrap();
            let first = handle.attach(lease.clone());
            let queued = handle.attach(lease.clone());
            tokio::pin!(first, queued);
            assert!(poll_once(first.as_mut()).is_pending());
            assert!(poll_once(queued.as_mut()).is_pending());
            assert_eq!(inbox.len(), 1);
            assert_eq!(handle.waiting.len(), 1);
            worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
            let Poll::Ready(Ok(first)) = poll_once(first.as_mut()) else {
                panic!("first attach must finish")
            };
            let Poll::Ready(Ok(queued)) = poll_once(queued.as_mut()) else {
                panic!("one reply must resolve both shared callers")
            };
            assert_eq!(handle.waiting.len(), 0);

            // Published joins no longer need registration or a manager message.
            for _ in 0..100 {
                let direct = handle.attach(lease.clone());
                tokio::pin!(direct);
                let Poll::Ready(Ok(direct)) = poll_once(direct.as_mut()) else {
                    panic!("published attach must not wait for the worker")
                };
                assert_eq!(direct.database_id, first.database_id);
                assert!(Arc::ptr_eq(&direct.target, &first.target));
            }
            assert!(inbox.is_empty());
            assert_eq!(queued.database_id, first.database_id);
            assert_eq!(worker.inventory.ready().len(), 1);

            let release = handle.release(lease.clone());
            tokio::pin!(release);
            assert!(poll_once(release.as_mut()).is_pending());
            let direct = handle.attach(lease.clone());
            tokio::pin!(direct);
            let Poll::Ready(Ok(direct)) = poll_once(direct.as_mut()) else {
                panic!("queued release must not block a direct join")
            };
            assert_eq!(inbox.len(), 1);
            worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
            assert!(matches!(poll_once(release.as_mut()), Poll::Ready(Ok(()))));
            for session in [&first, &queued, &direct] {
                assert!(session.cancellation_token().is_cancelled());
            }

            let fresh = handle.attach(lease);
            tokio::pin!(fresh);
            assert!(poll_once(fresh.as_mut()).is_pending());
            worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
            let Poll::Ready(Ok(fresh)) = poll_once(fresh.as_mut()) else {
                panic!("released lease must get a fresh assignment")
            };
            assert_ne!(fresh.database_id, first.database_id);
            assert!(!fresh.cancellation_token().is_cancelled());
            assert!(inbox.is_empty());
            runtime.shutdown().await;
            assert!(fresh.cancellation_token().is_cancelled());
        }
    }

    #[tokio::test]
    async fn failed_first_reply_is_not_published_and_unread_success_is_published() {
        let (runtime, mut worker, mut inbox) = manual_manager(1, 0);
        let handle = runtime.handle();
        let lease = LeaseId::new("shared").unwrap();
        let (reply, response) = oneshot::channel();
        drop(response);
        worker.handle(
            ManagerMessage::AttachOrJoin {
                lease: lease.clone(),
                reply,
                waiting_generation: u64::MAX,
            },
            ElapsedTime::default(),
        );
        assert!(handle.leases.is_empty());
        assert_eq!(worker.inventory.ready().len(), 1);

        let (reply, response) = oneshot::channel();
        worker.handle(
            ManagerMessage::AttachOrJoin {
                lease: lease.clone(),
                reply,
                waiting_generation: u64::MAX,
            },
            ElapsedTime::default(),
        );
        // A delivered reply owns the assignment even if nobody reads it.
        drop(response);
        let direct = handle.attach(lease);
        tokio::pin!(direct);
        let Poll::Ready(Ok(direct)) = poll_once(direct.as_mut()) else {
            panic!("successful unread reply must publish")
        };
        assert_eq!(direct.database_id, DatabaseId(1));
        assert!(inbox.try_recv().is_err());
        assert!(worker.inventory.ready().is_empty());
        runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_readers_share_published_session_without_worker_progress() {
        let (runtime, mut worker, inbox) = manual_manager(1, 0);
        let handle = runtime.handle();
        let lease = LeaseId::new("shared").unwrap();
        let (reply, response) = oneshot::channel();
        worker.handle(
            ManagerMessage::AttachOrJoin {
                lease: lease.clone(),
                reply,
                waiting_generation: u64::MAX,
            },
            ElapsedTime::default(),
        );
        let Reply::Attached(first) = response.await.unwrap() else { panic!("expected attachment") };
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let handle = handle.clone();
            let lease = lease.clone();
            readers.spawn(async move { handle.attach(lease).await.unwrap() });
        }
        let sessions =
            tokio::time::timeout(Duration::from_secs(3), readers.join_all()).await.unwrap();
        assert!(inbox.is_empty());
        worker.shutdown();
        for session in sessions {
            assert_eq!(session.database_id, first.database_id);
            assert!(Arc::ptr_eq(&session.target, &first.target));
            assert!(session.cancellation_token().is_cancelled());
        }
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn unpublished_assignment_uses_message_path() {
        use crate::manager_handle::lease::LeaseEntry;

        let (runtime, mut worker, mut inbox) = manual_manager(1, 0);
        let handle = runtime.handle();
        let lease = LeaseId::new("shared").unwrap();
        // Model the interval between assignment and the first successful reply.
        let database = worker.inventory.take_ready().unwrap();
        handle.leases.pin().insert(
            lease.clone(),
            LeaseEntry { database, generation: 1, session: Default::default() },
        );
        let pending = handle.attach(lease);
        tokio::pin!(pending);
        assert!(poll_once(pending.as_mut()).is_pending());
        assert_eq!(inbox.len(), 1);
        worker.handle(inbox.try_recv().unwrap(), ElapsedTime::default());
        assert!(matches!(poll_once(pending.as_mut()), Poll::Ready(Ok(_))));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn direct_attach_checks_cancellation_shutdown_and_channel_closure() {
        for closed_channel in [false, true] {
            let (runtime, mut worker, mut inbox) = manual_manager(1, 0);
            let handle = runtime.handle();
            let lease = LeaseId::new("shared").unwrap();
            let (reply, response) = oneshot::channel();
            worker.handle(
                ManagerMessage::AttachOrJoin {
                    lease: lease.clone(),
                    reply,
                    waiting_generation: u64::MAX,
                },
                ElapsedTime::default(),
            );
            drop(response);
            if closed_channel {
                inbox.close();
            } else {
                worker.shutdown();
                assert!(matches!(
                    handle.attach(lease.clone()).await,
                    Err(AttachError::LeaseClosed)
                ));
                assert!(inbox.is_empty());
                runtime.shutdown.cancel();
            }
            assert!(matches!(handle.attach(lease).await, Err(AttachError::EngineUnavailable)));
            assert!(inbox.is_empty());
            runtime.shutdown().await;
        }
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
                waiting_generation: u64::MAX,
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
                waiting_generation: u64::MAX,
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
