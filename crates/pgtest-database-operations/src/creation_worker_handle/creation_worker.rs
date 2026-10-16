//! Creation worker receive loop and operation scheduling.
use std::sync::Arc;

use futures_util::{StreamExt, stream::FuturesUnordered};
use hotpath::wrap::tokio::sync::mpsc;
use pgtest_engine_backend::{
    DatabaseCreator,
    jobs::{CreateDatabases, DatabaseWorkerMessages},
    workers::{CreationAction, CreationState},
};
use tokio_util::sync::CancellationToken;

pub(super) struct CreationActor<DatabaseClient, ManagerMessage> {
    pub(super) client: Arc<DatabaseClient>,
    pub(super) state: CreationState,
    pub(super) receiver: mpsc::UnboundedReceiver<CreateDatabases>,
    pub(super) manager: mpsc::UnboundedSender<ManagerMessage>,
    pub(super) shutdown: CancellationToken,
}
impl<DatabaseClient, ManagerMessage> CreationActor<DatabaseClient, ManagerMessage>
where
    DatabaseClient: DatabaseCreator,
    ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static,
{
    pub(super) async fn run(self) {
        let Self { client: backend, mut state, receiver: mut inbox, manager, shutdown } = self;
        let mut active = FuturesUnordered::new();
        let mut open = true;
        loop {
            let actions = tokio::select! { biased;
                _ = shutdown.cancelled() => break,
                _ = manager.closed() => break,
                completion = active.next(), if !active.is_empty() => {
                    let (id, result) = completion.expect("active future");
                    state.complete(id, result)
                },
                request = inbox.recv(), if open => {
                    match request { Some(request) => state.enqueue(request), None => { open = false; Vec::new() } }
                },
                else => break,
            };
            for action in actions {
                match action {
                    CreationAction::Begin(id) => {
                        let backend = backend.clone();
                        active.push(async move { (id, backend.create_database().await) });
                    }
                    CreationAction::Report(results) => {
                        let sent = hotpath::measure_block!("creation_worker::send_results", {
                            manager
                                .send(DatabaseWorkerMessages::CreationsFinished { results }.into())
                        });
                        if sent.is_err() {
                            tracing::warn!(
                                "unable to deliver creation results: manager inbox closed"
                            );
                            return;
                        }
                    }
                }
            }
            if !open && state.is_idle() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod creation_worker_test {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use hotpath::wrap::tokio::sync::mpsc::UnboundedReceiver;
    use pgtest_engine_backend::{
        BackendError, DatabaseCreator, PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId,
        jobs::{CreateDatabases, CreationResult, DatabaseId, DatabaseWorkerMessages},
    };
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    use crate::creation_worker_handle::CreationHandle;

    type Completion = oneshot::Sender<Result<ProvisionedDatabase, BackendError>>;

    struct Backend(mpsc::UnboundedSender<Completion>);

    impl DatabaseCreator for Backend {
        async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
            let (tx, rx) = oneshot::channel();
            self.0.send(tx).unwrap();
            rx.await.unwrap()
        }
    }

    struct Harness {
        creation: CreationHandle,
        started: mpsc::UnboundedReceiver<Completion>,
        inbox: UnboundedReceiver<DatabaseWorkerMessages>,
        shutdown: CancellationToken,
        tracker: TaskTracker,
    }

    impl Harness {
        fn new(limit: usize, amount: usize) -> Self {
            let (started, receiver) = mpsc::unbounded_channel();
            let (manager, inbox) = hotpath::channel!(mpsc::unbounded_channel());
            let shutdown = CancellationToken::new();
            let tracker = TaskTracker::new();
            let creation = CreationHandle::new_with_client(
                Arc::new(Backend(started)),
                NonZeroUsize::new(limit).unwrap(),
                manager,
                tracker.clone(),
                shutdown.clone(),
            );
            creation
                .create(CreateDatabases {
                    first_database_id: DatabaseId(1),
                    amount: NonZeroUsize::new(amount).unwrap(),
                })
                .unwrap();
            Self { creation, started: receiver, inbox, shutdown, tracker }
        }

        async fn next_creation(&mut self) -> Completion {
            tokio::time::timeout(Duration::from_secs(3), self.started.recv())
                .await
                .unwrap()
                .unwrap()
        }

        async fn next_results(&mut self) -> Vec<CreationResult> {
            let message = tokio::time::timeout(Duration::from_secs(3), self.inbox.recv())
                .await
                .unwrap()
                .unwrap();
            let DatabaseWorkerMessages::CreationsFinished { results } = message else {
                panic!("expected creation results")
            };
            assert!(!results.is_empty());
            results
        }

        async fn stop(self) {
            self.shutdown.cancel();
            self.tracker.close();
            tokio::time::timeout(Duration::from_secs(3), self.tracker.wait()).await.unwrap();
        }
    }

    fn db(name: &str) -> ProvisionedDatabase {
        ProvisionedDatabase {
            resource_id: ResourceId(name.into()),
            target: PgTarget {
                database: name.into(),
                endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
            },
        }
    }

    #[tokio::test]
    async fn full_batch_waits_for_slow_creation_and_keeps_queued_batches_separate() {
        let mut h = Harness::new(3, 5);
        h.creation
            .create(CreateDatabases {
                first_database_id: DatabaseId(6),
                amount: NonZeroUsize::new(2).unwrap(),
            })
            .unwrap();
        let slow = h.next_creation().await;
        let second = h.next_creation().await;
        let third = h.next_creation().await;
        assert!(h.started.try_recv().is_err());
        let failure = BackendError::OperationFailed("injected failure".into());
        second.send(Err(failure.clone())).unwrap();
        third.send(Ok(db("third"))).unwrap();

        // Slots refill without reporting the partial batch, even after a
        // failure.
        let fourth = h.next_creation().await;
        let fifth = h.next_creation().await;
        assert!(h.inbox.is_empty());
        assert!(h.started.try_recv().is_err());
        fourth.send(Ok(db("fourth"))).unwrap();
        fifth.send(Ok(db("fifth"))).unwrap();
        slow.send(Ok(db("first"))).unwrap();
        let mut results = h.next_results().await;
        results.sort_by_key(|result| result.database_id.0);
        assert_eq!(results.len(), 5);
        for (index, result) in results.iter().enumerate() {
            assert_eq!(result.database_id, DatabaseId(u64::try_from(index + 1).unwrap()));
        }
        assert_eq!(results[0].result, Ok(db("first")));
        assert_eq!(results[1].result, Err(failure));
        assert_eq!(results[2].result, Ok(db("third")));
        assert_eq!(results[3].result, Ok(db("fourth")));
        assert_eq!(results[4].result, Ok(db("fifth")));

        let sixth = h.next_creation().await;
        let seventh = h.next_creation().await;
        sixth.send(Ok(db("sixth"))).unwrap();
        assert!(h.inbox.is_empty());
        seventh.send(Ok(db("seventh"))).unwrap();
        let results = h.next_results().await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].database_id, DatabaseId(6));
        assert_eq!(results[1].database_id, DatabaseId(7));
        assert!(h.inbox.is_empty());
        h.stop().await;
    }

    #[tokio::test]
    async fn large_batch_sends_one_message_across_many_concurrency_windows() {
        let amount = 130;
        let mut h = Harness::new(4, amount);
        for _ in 0..amount {
            let completion = h.next_creation().await;
            assert!(h.inbox.is_empty(), "must not report before the full batch finishes");
            completion.send(Ok(db("ready"))).unwrap();
        }
        let results = h.next_results().await;
        assert_eq!(results.len(), amount);
        let mut ids: Vec<_> = results.into_iter().map(|result| result.database_id.0).collect();
        ids.sort_unstable();
        assert_eq!(ids, (1..=u64::try_from(amount).unwrap()).collect::<Vec<_>>());
        assert!(h.inbox.is_empty());
        h.stop().await;
    }

    #[tokio::test]
    async fn shutdown_discards_partial_batch_and_cancels_remaining_creations() {
        let mut h = Harness::new(2, 3);
        let first = h.next_creation().await;
        let mut second = h.next_creation().await;
        first.send(Ok(db("first"))).unwrap();
        let mut third = h.next_creation().await;
        assert!(h.inbox.is_empty());
        h.shutdown.cancel();
        h.tracker.close();
        tokio::time::timeout(Duration::from_secs(3), h.tracker.wait()).await.unwrap();
        second.closed().await;
        third.closed().await;
        assert!(h.inbox.recv().await.is_none());
    }

    #[tokio::test]
    async fn closed_manager_stops_creation_and_drops_pending_operations() {
        let mut h = Harness::new(2, 3);
        let first = h.next_creation().await;
        let mut pending = h.next_creation().await;
        h.inbox.close();
        first.send(Ok(db("first"))).unwrap();
        tokio::time::timeout(Duration::from_secs(3), pending.closed()).await.unwrap();
        h.tracker.close();
        tokio::time::timeout(Duration::from_secs(3), h.tracker.wait()).await.unwrap();
        assert!(
            h.creation
                .create(CreateDatabases {
                    first_database_id: DatabaseId(4),
                    amount: NonZeroUsize::MIN
                })
                .is_err()
        );
        assert!(h.started.try_recv().is_err(), "replacement must not run after reporting fails");
    }
}
