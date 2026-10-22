use std::{num::NonZeroUsize, sync::Mutex};

use pgtest_engine_backend::{
    BackendError, DatabaseCleaner, DatabaseCreator, PgEndpoint, PgTarget, ProvisionedDatabase,
    ResourceId, jobs::DatabaseId,
};

use super::*;
use crate::{config::PoolConfig, inventory::Database};

type CreateReply = oneshot::Sender<Result<ProvisionedDatabase, BackendError>>;
type DeleteReply = oneshot::Sender<Result<(), BackendError>>;
struct Backend {
    creates: mpsc::UnboundedSender<CreateReply>,
    deletes: mpsc::UnboundedSender<(ResourceId, DeleteReply)>,
    threads: Mutex<Vec<(String, tokio::runtime::RuntimeFlavor)>>,
}
impl DatabaseCreator for Backend {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        self.threads.lock().unwrap().push((
            std::thread::current().name().unwrap().into(),
            tokio::runtime::Handle::current().runtime_flavor(),
        ));
        let (reply, receiver) = oneshot::channel();
        self.creates.send(reply).unwrap();
        receiver.await.unwrap()
    }
}
impl DatabaseCleaner for Backend {
    async fn delete_database(&self, resource: ResourceId) -> Result<(), BackendError> {
        self.threads.lock().unwrap().push((
            std::thread::current().name().unwrap().into(),
            tokio::runtime::Handle::current().runtime_flavor(),
        ));
        let (reply, receiver) = oneshot::channel();
        self.deletes.send((resource, reply)).unwrap();
        receiver.await.unwrap()
    }
}
fn backend() -> (
    Arc<Backend>,
    mpsc::UnboundedReceiver<CreateReply>,
    mpsc::UnboundedReceiver<(ResourceId, DeleteReply)>,
) {
    let (creates, cr) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let (deletes, dr) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    (Arc::new(Backend { creates, deletes, threads: Mutex::new(Vec::new()) }), cr, dr)
}
fn config(initial: u16, batch: u16, timeout: u128) -> PoolConfig {
    PoolConfig {
        initial_slots: initial.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: batch.into(),
        lease_claim_timeout_ms: timeout,
    }
}
fn lease() -> LeaseId {
    LeaseId::new("shared").unwrap()
}
fn db(name: &str) -> ProvisionedDatabase {
    ProvisionedDatabase {
        resource_id: ResourceId(format!("provider/{name}")),
        target: PgTarget {
            database: name.into(),
            endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
        },
    }
}
fn database(n: u64) -> Database {
    Database { database_id: DatabaseId(n), resource: db(&format!("db{n}")) }
}
async fn next<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await.unwrap().unwrap()
}
#[tokio::test]
async fn separate_connection_runtimes_share_one_assignment() {
    let (backend, mut creates, mut deletes) = backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(1, 0, 0), vec![db("only")], stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = backend.clone();
    let creation_task =
        tokio::spawn(creation.run(async move { Ok(creation_client) }, NonZeroUsize::MIN, ready));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    let (sent, mut received) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let mut threads = Vec::new();
    for _ in 0..2 {
        let service = service.clone();
        let sent = sent.clone();
        threads.push(std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
                async move {
                    for _ in 0..16 {
                        sent.send(service.attach(lease()).await.unwrap()).unwrap();
                    }
                },
            );
        }));
    }
    let first = next(&mut received).await;
    for _ in 1..32 {
        assert!(Arc::ptr_eq(&first.target, &next(&mut received).await.target));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(creates.try_recv().is_err(), "one global inventory, no per-core refill");
    service.release(lease()).await.unwrap();
    assert!(first.cancellation_token().is_cancelled());
    let (resource, reply) = next(&mut deletes).await;
    assert_eq!(resource, ResourceId("provider/only".into()));
    reply.send(Ok(())).unwrap();
    stop.cancel();
    creation_task.await.unwrap();
    cleanup_task.await.unwrap();
    let threads = backend.threads.lock().unwrap();
    assert_eq!(threads.len(), 1, "only cleanup uses the backend for prepopulated inventory");
    assert!(threads.iter().all(|(name, _)| name == std::thread::current().name().unwrap()));
    assert!(
        threads.iter().all(|(_, flavor)| *flavor == tokio::runtime::RuntimeFlavor::CurrentThread)
    );
}

#[tokio::test]
async fn release_pending_wakes_every_waiter_and_late_creation_supplies_new_identity() {
    let (backend, mut creates, mut deletes) = backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 1, 0), Vec::new(), stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = backend.clone();
    let creation_task =
        tokio::spawn(creation.run(async move { Ok(creation_client) }, NonZeroUsize::MIN, ready));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    let mut callers = Vec::new();
    for _ in 0..16 {
        let mut caller = Box::pin(service.attach(lease()));
        assert!(futures_util::poll!(&mut caller).is_pending());
        callers.push(caller);
    }
    let late = next(&mut creates).await;
    assert!(service.registry.get(&lease()).is_none(), "pending leases must stay out of Papaya");
    service.release(lease()).await.unwrap();
    for caller in callers {
        assert!(matches!(caller.await, Err(AttachError::LeaseClosed)));
    }
    assert!(service.registry.get(&lease()).is_none());
    let replacement = tokio::spawn({
        let service = service.clone();
        async move { service.attach(lease()).await.unwrap() }
    });
    late.send(Ok(db("late"))).unwrap();
    next(&mut creates).await.send(Ok(db("spare"))).unwrap();
    let session = replacement.await.unwrap();
    assert_eq!(session.target.database, "late");
    assert!(!session.cancellation_token().is_cancelled());
    assert!(deletes.try_recv().is_err());
    stop.cancel();
    creation_task.await.unwrap();
    cleanup_task.await.unwrap();
}

#[tokio::test]
async fn timeout_is_per_caller_and_abandoned_supply_is_reused() {
    let (backend, mut creates, _deletes) = backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 1, 50), Vec::new(), stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = backend.clone();
    let creation_task =
        tokio::spawn(creation.run(async move { Ok(creation_client) }, NonZeroUsize::MIN, ready));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    let first = tokio::spawn({
        let service = service.clone();
        async move { service.attach(lease()).await }
    });
    let late = next(&mut creates).await;
    assert!(matches!(first.await.unwrap(), Err(AttachError::TimedOut)));
    tokio::time::timeout(Duration::from_secs(3), async {
        while service.registry.get(&lease()).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    late.send(Ok(db("unused"))).unwrap();
    next(&mut creates).await.send(Ok(db("spare"))).unwrap();
    let next_lease = LeaseId::new("next").unwrap();
    let session = service.attach(next_lease).await.unwrap();
    assert_eq!(session.target.database, "unused");
    stop.cancel();
    creation_task.await.unwrap();
    cleanup_task.await.unwrap();
}

#[tokio::test]
async fn dropping_one_subscription_preserves_other_and_shutdown_closes_everything() {
    let (backend, mut creates, _deletes) = backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 1, 0), Vec::new(), stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = backend.clone();
    let creation_task =
        tokio::spawn(creation.run(async move { Ok(creation_client) }, NonZeroUsize::MIN, ready));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    let mut first = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut first).is_pending());
    let late = next(&mut creates).await;
    let mut second = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut second).is_pending());
    assert!(service.registry.get(&lease()).is_none());
    drop(first);
    late.send(Ok(db("kept"))).unwrap();
    next(&mut creates).await.send(Ok(db("spare"))).unwrap();
    let session = second.await.unwrap();
    service.attach(LeaseId::new("consume-spare").unwrap()).await.unwrap();
    let mut inflight = next(&mut creates).await;
    let mut waiting = Box::pin(service.attach(LeaseId::new("pending").unwrap()));
    assert!(futures_util::poll!(&mut waiting).is_pending());
    stop.cancel();
    creation_task.await.unwrap();
    cleanup_task.await.unwrap();
    inflight.closed().await;
    assert!(session.cancellation_token().is_cancelled());
    assert!(matches!(waiting.await, Err(AttachError::EngineUnavailable)));
    assert!(matches!(service.attach(lease()).await, Err(AttachError::EngineUnavailable)));
}

#[tokio::test]
async fn cancelled_startup_and_worker_panic_stop_siblings() {
    let (backend, _creates, _deletes) = backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 0, 0), Vec::new(), stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let (entered, entering) = oneshot::channel();
    let (blocked, blocker) = oneshot::channel::<()>();
    let creation_client = backend.clone();
    let creation_task = tokio::spawn(creation.run(
        async move {
            entered.send(()).unwrap();
            let _ = blocker.await;
            Ok(creation_client)
        },
        NonZeroUsize::MIN,
        ready,
    ));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    entering.await.unwrap();
    let mut blocked = blocked;
    // The host cancels the shared token when it abandons startup.
    drop(created);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), creation_task).await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(3), cleanup_task).await.unwrap().unwrap();
    assert!(stop.is_cancelled());
    tokio::time::timeout(Duration::from_secs(3), blocked.closed()).await.unwrap();
    assert!(matches!(service.attach(lease()).await, Err(AttachError::EngineUnavailable)));
    struct Panics;
    impl DatabaseCreator for Panics {
        async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
            panic!("injected worker panic")
        }
    }
    let (backend, _creates, _deletes) = self::backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 1, 0), Vec::new(), stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = Arc::new(Panics);
    let creation_task =
        tokio::spawn(creation.run(async move { Ok(creation_client) }, NonZeroUsize::MIN, ready));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    assert!(matches!(service.attach(lease()).await, Err(AttachError::EngineUnavailable)));
    assert!(creation_task.await.unwrap_err().is_panic());
    tokio::time::timeout(Duration::from_secs(3), cleanup_task).await.unwrap().unwrap();
    assert!(stop.is_cancelled());
}

#[test]
fn ready_lookup_and_stale_cleanup_do_not_need_worker_progress() {
    let (reader, publisher, remover) = registry::split();
    let stop = CancellationToken::new();
    let old = publisher.publish(Entry::new(lease(), database(1), &stop));
    let session = reader.get(&lease()).unwrap().ready().unwrap();
    assert_eq!(remover.close(&old).unwrap().database_id, DatabaseId(1));
    let new = publisher.publish(Entry::new(lease(), database(2), &stop));
    assert!(remover.close(&old).is_none());
    assert!(session.cancellation_token().is_cancelled());
    assert_eq!(reader.get(&lease()).unwrap().ready().unwrap().database_id, DatabaseId(2));
    assert!(!new.cancellation().is_cancelled());
}

#[test]
fn stale_release_racing_republication_preserves_the_new_assignment() {
    for _ in 0..64 {
        let (reader, publisher, remover) = registry::split();
        let stop = CancellationToken::new();
        let old = publisher.publish(Entry::new(lease(), database(1), &stop));
        assert_eq!(remover.close(&old).unwrap().database_id, DatabaseId(1));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let publish = {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                publisher.publish(Entry::new(lease(), database(2), &stop))
            })
        };
        barrier.wait();
        assert!(remover.close(&old).is_none());
        let new = publish.join().unwrap();
        assert!(Arc::ptr_eq(&reader.get(&lease()).unwrap(), &new));
        assert!(!new.cancellation().is_cancelled());
        assert_eq!(remover.close(&new).unwrap().database_id, DatabaseId(2));
        assert!(remover.close(&new).is_none());
    }
}

#[tokio::test]
async fn release_that_misses_publication_is_forwarded_to_cleanup() {
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(1, 0, 0), vec![db("only")], stop);
    let (backend, mut creates, mut deletes) = backend();
    let mut worker = Box::pin(creation.run_initialized(backend.clone(), NonZeroUsize::MIN));
    let mut cleaner = Box::pin(cleanup.run_initialized(backend, NonZeroUsize::MIN));
    let mut attached = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut attached).is_pending());
    // Both calls observe an empty registry. Creation processes attach first,
    // then must forward release to cleanup using the published identity.
    let mut released = Box::pin(service.release(lease()));
    assert!(futures_util::poll!(&mut released).is_pending());
    assert!(service.registry.get(&lease()).is_none());
    assert!(futures_util::poll!(&mut worker).is_pending());
    let session = attached.await.unwrap();
    assert!(
        service.registry.get(&lease()).is_some(),
        "publish before handling the next creation request"
    );
    assert!(futures_util::poll!(&mut released).is_pending(), "cleanup must acknowledge removal");
    assert!(futures_util::poll!(&mut cleaner).is_pending());
    released.await.unwrap();
    assert!(session.cancellation_token().is_cancelled());
    assert!(service.registry.get(&lease()).is_none());
    let (resource, deleted) = deletes.try_recv().unwrap();
    assert_eq!(resource, ResourceId("provider/only".into()));
    deleted.send(Ok(())).unwrap();
    assert!(futures_util::poll!(&mut cleaner).is_pending());
    assert!(deletes.try_recv().is_err());
    assert!(creates.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn expiry_and_failed_deletion_preserve_identity_and_do_not_block_new_leases() {
    let stop = CancellationToken::new();
    let (reader, publisher, remover) = registry::split();
    let (commands, inbox) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let (backend, _, mut deletes) = backend();
    let cleanup = cleanup::CleanupWorker::new(remover, inbox, stop.clone());
    let task = tokio::spawn(cleanup.run_initialized(backend, NonZeroUsize::MIN));
    let old = publisher.publish(Entry::new(lease(), database(1), &stop));
    commands
        .send(Cleanup::Expire {
            entry: old.clone(),
            at: tokio::time::Instant::now() + Duration::from_secs(1),
        })
        .ok()
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    let (_, failed) = next(&mut deletes).await;
    assert!(reader.get(&lease()).is_none());
    let new = publisher.publish(Entry::new(lease(), database(2), &stop));
    failed.send(Err(BackendError::OperationFailed("cannot delete".into()))).unwrap();
    commands.send(Cleanup::Release { entry: old, reply: None }).ok().unwrap();
    let (ack, response) = oneshot::channel();
    commands.send(Cleanup::Release { entry: new, reply: Some(ack) }).ok().unwrap();
    response.await.unwrap();
    let (resource, reply) = next(&mut deletes).await;
    assert_eq!(resource, ResourceId("provider/db2".into()));
    reply.send(Ok(())).unwrap();
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn releasing_an_unknown_lease_does_not_start_growth() {
    let LifecycleWorkers { service, creation, cleanup: _cleanup } =
        LifecycleWorkers::new(config(0, 4, 0), Vec::new(), CancellationToken::new());
    let (backend, mut creates, _deletes) = backend();
    let mut worker = Box::pin(creation.run_initialized(backend, NonZeroUsize::MIN));
    let mut released = Box::pin(service.release(lease()));
    assert!(futures_util::poll!(&mut released).is_pending());
    assert!(futures_util::poll!(&mut worker).is_pending());
    released.await.unwrap();
    assert!(creates.try_recv().is_err());
    assert!(service.registry.get(&lease()).is_none());
}

#[tokio::test]
async fn pending_waiters_share_one_assignment_published_only_after_the_full_batch() {
    let LifecycleWorkers { service, creation, cleanup: _cleanup } =
        LifecycleWorkers::new(config(0, 2, 0), Vec::new(), CancellationToken::new());
    let (backend, mut creates, _deletes) = backend();
    let mut worker = Box::pin(creation.run_initialized(backend, NonZeroUsize::new(2).unwrap()));
    let mut first = Box::pin(service.attach(lease()));
    let mut second = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    assert!(futures_util::poll!(&mut worker).is_pending());
    assert!(service.registry.get(&lease()).is_none());
    creates.try_recv().unwrap().send(Ok(db("assigned"))).unwrap();
    let last = creates.try_recv().unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    assert!(service.registry.get(&lease()).is_none(), "partial batches must not publish");
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());

    last.send(Ok(db("spare"))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    let published = service
        .registry
        .get(&lease())
        .expect("publish accepted assignments before dispatch returns");
    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert_eq!(first.database_id, second.database_id);
    assert!(Arc::ptr_eq(&first.target, &second.target));
    assert!(Arc::ptr_eq(&first.target, &published.ready().unwrap().target));
    assert_eq!(first.target.database, "assigned");
    assert!(creates.try_recv().is_err(), "pending waiters must share demand for one database");
}

#[tokio::test]
async fn shared_growth_respects_batch_and_operation_limits_while_cleanup_is_blocked() {
    let (backend, mut creates, mut deletes) = backend();
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 4, 0), Vec::new(), stop.clone());
    let (ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = backend.clone();
    let creation_task = tokio::spawn(creation.run(
        async move { Ok(creation_client) },
        NonZeroUsize::new(2).unwrap(),
        ready,
    ));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move { Ok(cleanup_client) },
        NonZeroUsize::new(2).unwrap(),
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    let mut callers = tokio::task::JoinSet::new();
    for name in ["one", "two"] {
        let service = service.clone();
        callers.spawn(async move { service.attach(LeaseId::new(name).unwrap()).await.unwrap() });
    }
    let slow = next(&mut creates).await;
    let second = next(&mut creates).await;
    assert!(creates.try_recv().is_err());
    second.send(Ok(db("second"))).unwrap();
    next(&mut creates).await.send(Ok(db("third"))).unwrap();
    next(&mut creates).await.send(Ok(db("fourth"))).unwrap();
    assert!(creates.try_recv().is_err());
    assert!(callers.try_join_next().is_none(), "partial batches are not published");
    slow.send(Ok(db("first"))).unwrap();
    assert_eq!(callers.join_all().await.len(), 2);
    service.release(LeaseId::new("one").unwrap()).await.unwrap();
    let (_, blocked_delete) = next(&mut deletes).await;
    service.attach(LeaseId::new("three").unwrap()).await.unwrap();
    service.attach(LeaseId::new("four").unwrap()).await.unwrap();
    let mut refill_one = next(&mut creates).await;
    let mut refill_two = next(&mut creates).await;
    assert!(creates.try_recv().is_err());
    assert!(!blocked_delete.is_closed());
    stop.cancel();
    creation_task.await.unwrap();
    cleanup_task.await.unwrap();
    refill_one.closed().await;
    refill_two.closed().await;
    assert!(blocked_delete.is_closed());
}

#[tokio::test(start_paused = true)]
async fn pending_subscribers_keep_independent_deadlines() {
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 1, 100), Vec::new(), stop.clone());
    let (backend, mut creates, _deletes) = backend();
    let create_task = tokio::spawn(creation.run_initialized(backend.clone(), NonZeroUsize::MIN));
    let cleanup_task = tokio::spawn(cleanup.run_initialized(backend, NonZeroUsize::MIN));
    let mut first = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut first).is_pending());
    let late = next(&mut creates).await;
    tokio::time::advance(Duration::from_millis(50)).await;
    let mut second = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut second).is_pending());
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    assert!(matches!(first.await, Err(AttachError::TimedOut)));
    assert!(futures_util::poll!(&mut second).is_pending());
    late.send(Ok(db("live"))).unwrap();
    next(&mut creates).await.send(Ok(db("spare"))).unwrap();
    assert_eq!(second.await.unwrap().target.database, "live");
    stop.cancel();
    create_task.await.unwrap();
    cleanup_task.await.unwrap();
}

#[tokio::test]
async fn queued_creation_batches_wait_for_all_results_including_failures() {
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup: _cleanup } =
        LifecycleWorkers::new(config(0, 2, 0), Vec::new(), stop);
    let (backend, mut creates, _deletes) = backend();
    let mut worker = Box::pin(creation.run_initialized(backend, NonZeroUsize::new(2).unwrap()));
    let mut first = Box::pin(service.attach(LeaseId::new("first").unwrap()));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut worker).is_pending());
    let slow = creates.try_recv().unwrap();
    let failed = creates.try_recv().unwrap();
    assert!(creates.try_recv().is_err());

    // Demand reserves another batch while the first one is still running.
    let mut second = Box::pin(service.attach(LeaseId::new("second").unwrap()));
    assert!(futures_util::poll!(&mut second).is_pending());
    assert!(futures_util::poll!(&mut worker).is_pending());
    failed.send(Err(BackendError::OperationFailed("injected".into()))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    assert!(creates.try_recv().is_err(), "a freed slot must not start the next batch early");
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());

    slow.send(Ok(db("first"))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    let assigned = first.await.unwrap();
    assert_eq!(assigned.target.database, "first");
    assert_eq!(assigned.database_id, DatabaseId(1));
    let next = creates.try_recv().unwrap();
    let last = creates.try_recv().unwrap();
    assert!(creates.try_recv().is_err());

    next.send(Ok(db("second"))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending(), "partial batches stay unpublished");
    last.send(Ok(db("spare"))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    let assigned = second.await.unwrap();
    assert_eq!(assigned.target.database, "second");
    assert_eq!(assigned.database_id, DatabaseId(3));
    assert!(creates.try_recv().is_err(), "queued supply must count toward growth");
}

#[tokio::test]
async fn cancelled_pending_lease_reuses_idle_supply_on_a_new_attach() {
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup: _cleanup } =
        LifecycleWorkers::new(config(0, 1, 0), Vec::new(), stop.clone());
    let (backend, mut creates, _deletes) = backend();
    // Creation alone owns pending cancellation; cleanup need not run.
    let mut worker = Box::pin(creation.run_initialized(backend, NonZeroUsize::MIN));
    assert!(futures_util::poll!(&mut worker).is_pending());

    let mut first = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut worker).is_pending());
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(service.registry.get(&lease()).is_none());
    drop(first);

    // Finish the whole batch while the entry has no demand. Both databases
    // remain in inventory and creation has no outstanding I/O to wake it.
    creates.try_recv().unwrap().send(Ok(db("unused"))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    creates.try_recv().unwrap().send(Ok(db("spare"))).unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    assert!(service.registry.get(&lease()).is_none());
    assert!(creates.try_recv().is_err());

    let mut revived = Box::pin(service.attach(lease()));
    assert!(futures_util::poll!(&mut revived).is_pending());
    // A fresh attach wakes creation even though no database I/O remains.
    assert!(futures_util::poll!(&mut worker).is_pending());
    let std::task::Poll::Ready(Ok(session)) = futures_util::poll!(&mut revived) else {
        panic!("attach must assign available inventory without another event")
    };
    assert_eq!(session.target.database, "unused");
    assert_eq!(
        service.registry.get(&lease()).unwrap().ready().unwrap().database_id,
        session.database_id
    );
    assert!(creates.try_recv().is_err());
    stop.cancel();
    worker.await;
}

#[tokio::test]
async fn ready_joins_bypass_creation_and_disconnect_does_not_release() {
    let (reader, publisher, _remover) = registry::split();
    let (creation, mut requests) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let (cleanup, mut removals) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let stop = CancellationToken::new();
    let service = LeaseService {
        registry: reader,
        creation,
        cleanup,
        timeout: Duration::ZERO,
        stop: stop.clone(),
    };
    let entry = publisher.publish(Entry::new(lease(), database(1), &stop));
    let session = service.attach(lease()).await.unwrap();
    assert_eq!(session.database_id, DatabaseId(1));
    assert!(requests.try_recv().is_err());
    drop(session);
    assert!(removals.try_recv().is_err());
    assert!(Arc::ptr_eq(&entry, &service.registry.get(&lease()).unwrap()));
}

#[tokio::test]
async fn externally_hosted_workers_initialize_and_run_on_the_host_runtime() {
    let stop = CancellationToken::new();
    let LifecycleWorkers { service, creation, cleanup } =
        LifecycleWorkers::new(config(0, 1, 0), Vec::new(), stop.clone());
    let (backend, mut creates, mut deletes) = backend();
    let host_thread = std::thread::current().id();
    let host_name = std::thread::current().name().unwrap().to_owned();
    let (creation_ready, created) = oneshot::channel();
    let (cleanup_ready, cleaned) = oneshot::channel();
    let creation_client = backend.clone();
    let creation_task = tokio::spawn(creation.run(
        async move {
            assert_eq!(std::thread::current().id(), host_thread);
            Ok(creation_client)
        },
        NonZeroUsize::MIN,
        creation_ready,
    ));
    let cleanup_client = backend.clone();
    let cleanup_task = tokio::spawn(cleanup.run(
        async move {
            assert_eq!(std::thread::current().id(), host_thread);
            Ok(cleanup_client)
        },
        NonZeroUsize::MIN,
        cleanup_ready,
    ));
    cleaned.await.unwrap().unwrap();
    created.await.unwrap().unwrap();
    assert!(creates.try_recv().is_err(), "readiness must not create initial inventory");
    let attached = tokio::spawn({
        let service = service.clone();
        async move { service.attach(lease()).await }
    });
    next(&mut creates).await.send(Ok(db("hosted"))).unwrap();
    next(&mut creates).await.send(Ok(db("spare"))).unwrap();
    let session = attached.await.unwrap().unwrap();
    assert_eq!(session.target.database, "hosted");
    service.release(lease()).await.unwrap();
    let (resource, deleted) = next(&mut deletes).await;
    assert_eq!(resource, ResourceId("provider/hosted".into()));
    deleted.send(Ok(())).unwrap();
    {
        let threads = backend.threads.lock().unwrap();
        assert_eq!(threads.len(), 3);
        assert!(threads.iter().all(|(name, flavor)| {
            name == &host_name && *flavor == tokio::runtime::RuntimeFlavor::CurrentThread
        }));
    }
    // The host cancelling one task must stop its sibling and reject new leases.
    creation_task.abort();
    assert!(creation_task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(3), cleanup_task).await.unwrap().unwrap();
    assert!(stop.is_cancelled());
    assert!(matches!(service.attach(lease()).await, Err(AttachError::EngineUnavailable)));
}

#[tokio::test]
async fn externally_hosted_initialization_failure_cancels_sibling_initialization() {
    for creation_fails in [true, false] {
        let stop = CancellationToken::new();
        let LifecycleWorkers { service, creation, cleanup } =
            LifecycleWorkers::new(config(0, 0, 0), Vec::new(), stop.clone());
        let (creation_ready, created) = oneshot::channel();
        let (cleanup_ready, cleaned) = oneshot::channel();
        let initialize = |fails| async move {
            if fails {
                Err::<Arc<Backend>, _>(StartError::Thread(std::io::Error::other("injected")))
            } else {
                std::future::pending().await
            }
        };
        let creation_task = tokio::spawn(creation.run(
            initialize(creation_fails),
            NonZeroUsize::MIN,
            creation_ready,
        ));
        let cleanup_task = tokio::spawn(cleanup.run(
            initialize(!creation_fails),
            NonZeroUsize::MIN,
            cleanup_ready,
        ));
        let (created, cleaned) = tokio::time::timeout(Duration::from_secs(3), async {
            creation_task.await.unwrap();
            cleanup_task.await.unwrap();
            (created.await, cleaned.await)
        })
        .await
        .unwrap();
        let (failed, cancelled) =
            if creation_fails { (created, cleaned) } else { (cleaned, created) };
        assert!(
            matches!(failed, Ok(Err(StartError::Thread(error))) if error.to_string() == "injected")
        );
        assert!(cancelled.is_err());
        assert!(stop.is_cancelled());
        assert!(matches!(service.attach(lease()).await, Err(AttachError::EngineUnavailable)));
    }
}
