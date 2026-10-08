use std::sync::Mutex;

use super::*;
use crate::backend::{PgEndpoint, ProvisionedDatabase};

type CreateReply = oneshot::Sender<Result<ProvisionedDatabase, BackendError>>;
type DeleteReply = oneshot::Sender<Result<(), BackendError>>;
struct ControlledBackend {
    creates: mpsc::UnboundedSender<CreateReply>,
    deletes: mpsc::UnboundedSender<(ResourceId, DeleteReply)>,
}
impl AsyncDatabaseBackend for ControlledBackend {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        let (tx, rx) = oneshot::channel();
        self.creates.send(tx).unwrap();
        rx.await.unwrap()
    }

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
        template: "template".into(),
        engine: WorkerEngineConfig {
            initial_slots: initial.into(),
            starvation_threshold: 0.into(),
            grow_batch_size: batch.into(),
            ..WorkerEngineConfig::default()
        },
        creation_concurrency: NonZeroUsize::new(2).unwrap(),
        cleanup_concurrency: NonZeroUsize::new(2).unwrap(),
        stale_resources: vec![],
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
    let pending = tokio::spawn(TokioRuntime::start(backend, config(initial, 0)));
    for n in 0..initial {
        next(&mut creates).await.send(Ok(db(&format!("db{n}")))).unwrap();
    }
    (pending.await.unwrap().unwrap(), creates, drops)
}

#[tokio::test]
async fn sharing_release_and_provider_identity() {
    let (runtime, _, mut drops) = start(1).await;
    let manager = runtime.handle();
    let a = manager.attach("template", LeaseId::new("same").unwrap()).await.unwrap();
    let b = manager.attach("template", LeaseId::new("same").unwrap()).await.unwrap();
    assert_eq!(a.database_id, b.database_id);
    assert_eq!(a.target, b.target);
    assert_eq!(a.target.database, "db0");
    manager.release(LeaseId::new("same").unwrap()).await.unwrap();
    assert!(a.cancellation_token().is_cancelled());
    assert!(b.cancellation_token().is_cancelled());
    let (id, reply) = next(&mut drops).await;
    assert_eq!(id, ResourceId("provider/db0".into()));
    reply.send(Ok(())).unwrap();
    assert!(matches!(
        manager.attach("template", LeaseId::new("same").unwrap()).await,
        Err(AttachError::LeaseClosed)
    ));
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn last_detach_keeps_assignment_and_shutdown_cancels_session() {
    let (runtime, _, mut drops) = start(1).await;
    let manager = runtime.handle();
    let a = manager.attach("template", LeaseId::new("same").unwrap()).await.unwrap();
    let id = a.database_id;
    drop(a);
    let b = manager.attach("template", LeaseId::new("same").unwrap()).await.unwrap();
    assert_eq!(b.database_id, id);
    assert!(drops.try_recv().is_err());
    runtime.shutdown().await.unwrap();
    assert!(b.cancellation_token().is_cancelled());
    assert!(matches!(
        manager.attach("template", LeaseId::new("same").unwrap()).await,
        Err(AttachError::EngineUnavailable)
    ));
}
#[tokio::test(start_paused = true)]
async fn claim_timeout_and_lifetime_use_runtime_clock() {
    let (runtime, _, mut drops) = start(1).await;
    let manager = runtime.handle();
    let session = manager.attach("template", LeaseId::new("held").unwrap()).await.unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    next(&mut drops).await.1.send(Ok(())).unwrap();
    assert!(session.cancellation_token().is_cancelled());
    let pending = manager.attach("template", LeaseId::new("waiting").unwrap());
    assert!(matches!(pending.await, Err(AttachError::TimedOut)));
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn cleanup_does_not_block_creation_or_release() {
    let (backend, mut creates, mut drops) = backend();
    let task = tokio::spawn(TokioRuntime::start(backend, config(1, 1)));
    next(&mut creates).await.send(Ok(db("first"))).unwrap();
    let runtime = task.await.unwrap().unwrap();
    let manager = runtime.handle();
    let first = manager.attach("template", LeaseId::new("first").unwrap()).await.unwrap();
    let replenishment = next(&mut creates).await;
    manager.release(LeaseId::new("first").unwrap()).await.unwrap();
    let (_, blocked_drop) = next(&mut drops).await;
    assert!(first.cancellation_token().is_cancelled());
    replenishment.send(Ok(db("second"))).unwrap();
    let second = manager.attach("template", LeaseId::new("second").unwrap()).await.unwrap();
    assert_eq!(second.target.database, "second");
    runtime.shutdown().await.unwrap();
    assert!(blocked_drop.is_closed());
}
#[tokio::test]
async fn startup_waits_for_all_results_and_limits_concurrency() {
    let (backend, mut creates, _) = backend();
    let task = tokio::spawn(TokioRuntime::start(backend, config(3, 0)));
    let first = next(&mut creates).await;
    let second = next(&mut creates).await;
    assert!(creates.try_recv().is_err());
    second.send(Err(BackendError::OperationFailed("failure".into()))).unwrap();
    let third = next(&mut creates).await;
    assert!(!task.is_finished());
    third.send(Ok(db("third"))).unwrap();
    assert!(!task.is_finished());
    first.send(Ok(db("first"))).unwrap();
    assert!(matches!(task.await.unwrap(), Err(StartError::InitialDatabaseCreation(_))));
}
#[tokio::test]
async fn startup_cleanup_uses_cleanup_actor_before_creation() {
    let (backend, mut creates, mut drops) = backend();
    let mut cfg = config(1, 0);
    cfg.stale_resources = vec![ResourceId("stale".into())];
    let task = tokio::spawn(TokioRuntime::start(backend, cfg));
    let (id, reply) = next(&mut drops).await;
    assert_eq!(id.0, "stale");
    assert!(creates.try_recv().is_err());
    reply.send(Err(BackendError::OperationFailed("cannot delete".into()))).unwrap();
    next(&mut creates).await.send(Ok(db("fresh"))).unwrap();
    task.await.unwrap().unwrap().shutdown().await.unwrap();
}
#[tokio::test]
async fn cancelled_startup_cancels_inflight_provider_future() {
    let (backend, mut creates, _) = backend();
    let task = tokio::spawn(TokioRuntime::start(backend, config(1, 0)));
    let mut reply = next(&mut creates).await;
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(3), reply.closed()).await.unwrap();
}
struct PanicBackend;
impl AsyncDatabaseBackend for PanicBackend {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        panic!("injected worker panic")
    }

    async fn delete_database(&self, _: ResourceId) -> Result<(), BackendError> {
        Ok(())
    }
}
#[tokio::test]
async fn worker_panic_during_startup_does_not_hang() {
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        TokioRuntime::start(Arc::new(PanicBackend), config(1, 0)),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(StartError::RuntimeStopped)));
}
#[tokio::test]
async fn worker_panic_after_startup_stops_pending_callers_and_is_reported() {
    let runtime = TokioRuntime::start(Arc::new(PanicBackend), config(0, 1)).await.unwrap();
    let handle = runtime.handle();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        handle.attach("template", LeaseId::new("a").unwrap()),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(AttachError::EngineUnavailable)));
    assert!(runtime.shutdown().await.is_err());
}
#[tokio::test]
async fn timed_out_reply_leaves_created_database_ready() {
    let (backend, mut creates, _) = backend();
    let mut cfg = config(0, 1);
    cfg.engine.lease_claim_timeout_ms = 10;
    let runtime = TokioRuntime::start(backend, cfg).await.unwrap();
    let handle = runtime.handle();
    assert!(matches!(
        handle.attach("template", LeaseId::new("expired").unwrap()).await,
        Err(AttachError::TimedOut)
    ));
    next(&mut creates).await.send(Ok(db("available"))).unwrap();
    let session = handle.attach("template", LeaseId::new("live").unwrap()).await.unwrap();
    assert_eq!(session.target.database, "available");
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn shutdown_cancels_queued_and_active_operations() {
    let (backend, mut creates, _) = backend();
    let runtime = TokioRuntime::start(backend, config(0, 16)).await.unwrap();
    let handle = runtime.handle();
    let attach =
        tokio::spawn(async move { handle.attach("template", LeaseId::new("a").unwrap()).await });
    let mut first = next(&mut creates).await;
    let mut second = next(&mut creates).await;
    runtime.shutdown().await.unwrap();
    first.closed().await;
    second.closed().await;
    assert!(matches!(attach.await.unwrap(), Err(AttachError::EngineUnavailable)));
    assert!(creates.recv().await.is_none());
}
#[tokio::test]
async fn failed_and_unread_reply_delivery_account_for_sessions_once() {
    let (runtime, ..) = start(1).await;
    let handle = runtime.handle();
    // Keep the receiver until the manager has sent a session, then discard it.
    let id = handle.request_id();
    let (tx, mut rx) = oneshot::channel();
    handle
        .commands
        .send(Envelope {
            message: EngineMessage::AttachOrJoin {
                template: "template".into(),
                lease: LeaseId::new("a").unwrap(),
                reply: id,
                message_time: Tick::default(),
            },
            reply: Some((id, tx)),
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
    let session = handle.attach("template", LeaseId::new("a").unwrap()).await.unwrap();
    assert_eq!(session.database_id, DatabaseId(1));
    // Failed delivery of a fresh attachment must restore its slot.
    runtime.shutdown().await.unwrap();
    let (runtime, ..) = start(1).await;
    let handle = runtime.handle();
    let id = handle.request_id();
    let (tx, rx) = oneshot::channel();
    drop(rx);
    handle
        .commands
        .send(Envelope {
            message: EngineMessage::AttachOrJoin {
                template: "template".into(),
                lease: LeaseId::new("lost").unwrap(),
                reply: id,
                message_time: Tick::default(),
            },
            reply: Some((id, tx)),
        })
        .unwrap();
    assert!(handle.attach("template", LeaseId::new("live").unwrap()).await.is_ok());
    runtime.shutdown().await.unwrap();
}
#[test]
fn startup_future_fits_stack_budget() {
    let (backend, ..) = backend();
    let future = TokioRuntime::start(backend, config(16, 16));
    assert!(std::mem::size_of_val(&future) < 64 * 1024);
}

struct ImmediateBackend {
    sequence: Mutex<u64>,
}
impl AsyncDatabaseBackend for ImmediateBackend {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        let mut n = self.sequence.lock().unwrap();
        *n += 1;
        Ok(db(&format!("db{}", *n)))
    }

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
    let a = sim.attach("template", "a");
    let b = sim.attach("template", "a");
    sim.run_until_idle(100).unwrap();
    let runtime = TokioRuntime::start(Arc::new(ImmediateBackend { sequence: Mutex::new(0) }), cfg)
        .await
        .unwrap();
    let h = runtime.handle();
    let real_a = h.attach("template", LeaseId::new("a").unwrap()).await.unwrap();
    let real_b = h.attach("template", LeaseId::new("a").unwrap()).await.unwrap();
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
    let again = sim.attach("template", "a");
    sim.run_until_idle(100).unwrap();
    assert!(matches!(
        sim.reply(again),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
    ));
    assert!(matches!(
        h.attach("template", LeaseId::new("a").unwrap()).await,
        Err(AttachError::LeaseClosed)
    ));
    runtime.shutdown().await.unwrap();
}
