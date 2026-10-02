use pgtest_utils::read_string::ReadString;
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::worker_engine::{
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId, DatabaseWorkerMessages},
    errors::{IOError, PostgresDDLClientError},
    traits::{EngineIO, PostgresClient},
};

type CreateResult = Result<ReadString, PostgresDDLClientError>;
type DropResult = Result<(), PostgresDDLClientError>;

struct DropRequest {
    database_name: String,
    finish: oneshot::Sender<DropResult>,
}

struct ControlledPostgres {
    creates: mpsc::UnboundedSender<oneshot::Sender<CreateResult>>,
    drops: mpsc::UnboundedSender<DropRequest>,
}

impl PostgresClient for ControlledPostgres {
    async fn create_database(&self) -> CreateResult {
        let (finish, result) = oneshot::channel();
        self.creates.send(finish).unwrap();
        result.await.expect("test must finish or cancel every creation")
    }

    async fn drop_database(&self, database_name: &str) -> DropResult {
        let (finish, result) = oneshot::channel();
        self.drops.send(DropRequest { database_name: database_name.into(), finish }).unwrap();
        result.await.expect("test must finish or cancel every drop")
    }

    async fn create_databases(
        &self,
        amount: usize,
        mut on_finished: impl FnMut(usize, CreateResult) + Send,
    ) {
        use futures_util::{StreamExt, stream::FuturesUnordered};

        let mut pending: FuturesUnordered<_> =
            (0..amount).map(|index| async move { (index, self.create_database().await) }).collect();
        while let Some((index, result)) = pending.next().await {
            on_finished(index, result);
        }
    }

    async fn drop_databases(
        &self,
        names: &[ReadString],
        mut on_finished: impl FnMut(usize, DropResult) + Send,
    ) {
        use futures_util::{StreamExt, stream::FuturesUnordered};

        let mut pending: FuturesUnordered<_> = names
            .iter()
            .enumerate()
            .map(|(index, name)| async move { (index, self.drop_database(name).await) })
            .collect();
        while let Some((index, result)) = pending.next().await {
            on_finished(index, result);
        }
    }
}

struct Fixture {
    senders: Option<DatabaseWorkerSenders>,
    results: hotpath::wrap::tokio::sync::mpsc::UnboundedReceiver<EngineMessage<ConsumerWorker>>,
    creates: mpsc::UnboundedReceiver<oneshot::Sender<CreateResult>>,
    drops: mpsc::UnboundedReceiver<DropRequest>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
}

impl Fixture {
    fn new() -> Self {
        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let (creates_tx, creates) = mpsc::unbounded_channel();
        let (drops_tx, drops) = mpsc::unbounded_channel();
        let client = Arc::new(ControlledPostgres { creates: creates_tx, drops: drops_tx });
        let (engine_tx, results) = hotpath::channel!(mpsc::unbounded_channel());
        let (senders, creation_rx, cleanup_rx) =
            DatabaseWorkerSenders::init_database_worker_channels();
        let creation = DatabaseCreationWorker::new(
            engine_tx.clone(),
            tracker.clone(),
            shutdown.clone(),
            client.clone(),
            creation_rx,
        );
        let cleanup = DatabaseCleanupWorker::new(
            engine_tx,
            tracker.clone(),
            shutdown.clone(),
            client,
            cleanup_rx,
        );
        tracker.spawn(creation.run());
        tracker.spawn(cleanup.run());
        Self { senders: Some(senders), results, creates, drops, tracker, shutdown }
    }

    fn create(&self, id: u64) {
        self.senders
            .as_ref()
            .unwrap()
            .creation_tx
            .send(CreateDatabases {
                first_database_id: DatabaseId(id),
                amount: std::num::NonZeroUsize::MIN,
            })
            .unwrap();
    }

    fn cleanup(&self, id: u64, name: &str) {
        self.senders
            .as_ref()
            .unwrap()
            .cleanup_tx
            .send(CleanupDatabase {
                database_id: DatabaseId(id),
                database_name: ReadString::from(name),
            })
            .unwrap();
    }

    async fn next_creation(&mut self) -> oneshot::Sender<CreateResult> {
        tokio::time::timeout(Duration::from_secs(1), self.creates.recv())
            .await
            .expect("creation should start")
            .expect("creation request channel must stay open")
    }

    async fn next_drop(&mut self) -> DropRequest {
        tokio::time::timeout(Duration::from_secs(1), self.drops.recv())
            .await
            .expect("drop should start")
            .expect("drop request channel must stay open")
    }

    async fn next_result(&mut self) -> DatabaseWorkerMessages {
        match tokio::time::timeout(Duration::from_secs(1), self.results.recv())
            .await
            .expect("worker should report its result")
            .expect("engine inbox must stay open")
        {
            EngineMessage::DatabaseWorker(message) => message,
            _ => panic!("workers must report through DatabaseWorker messages"),
        }
    }

    async fn finish(&mut self) {
        // Closing inputs lets the long-lived receiver loops exit after
        // accepting queued jobs.
        self.senders.take();
        self.tracker.close();
        tokio::time::timeout(Duration::from_secs(1), self.tracker.wait())
            .await
            .expect("worker loops and DDL tasks should finish");
        assert!(self.tracker.is_empty());
    }
}

#[tokio::test]
async fn creation_and_cleanup_send_results_to_the_same_engine_inbox() {
    let mut fixture = Fixture::new();
    fixture.create(41);
    fixture.cleanup(17, "retired");
    let create = fixture.next_creation().await;
    let drop = fixture.next_drop().await;
    assert_eq!(drop.database_name, "retired");
    assert!(fixture.results.try_recv().is_err(), "unfinished DDL cannot report success");

    drop.finish.send(Ok(())).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CleanupFinished { database_id: DatabaseId(17), result: Ok(()) }
    ));
    create.send(Ok(ReadString::from("fresh"))).unwrap();
    assert!(matches!(fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished { database_id: DatabaseId(41), result: Ok(name) }
            if name.as_ref() == "fresh"));
    fixture.finish().await;
    assert!(fixture.results.recv().await.is_none(), "all result senders should close");
}

#[tokio::test]
async fn blocked_cleanup_does_not_delay_creation() {
    let mut fixture = Fixture::new();
    let mut blocked = Vec::new();
    for id in 1..=32 {
        fixture.cleanup(id, &format!("retired_{id}"));
        blocked.push(fixture.next_drop().await);
    }
    fixture.create(100);
    fixture.next_creation().await.send(Ok(ReadString::from("fresh"))).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished { database_id: DatabaseId(100), result: Ok(_) }
    ));

    assert!(blocked.iter().all(|request| !request.finish.is_closed()));
    for request in blocked {
        request.finish.send(Ok(())).unwrap();
    }
    let mut cleaned = rustc_hash::FxHashSet::default();
    for _ in 0..32 {
        let DatabaseWorkerMessages::CleanupFinished { database_id, result: Ok(()) } =
            fixture.next_result().await
        else {
            panic!("expected successful cleanup");
        };
        assert!(cleaned.insert(database_id));
    }
    assert_eq!(cleaned, (1..=32).map(DatabaseId).collect());
    fixture.finish().await;
}

#[tokio::test]
async fn later_jobs_can_complete_while_earlier_jobs_are_blocked() {
    let mut fixture = Fixture::new();
    fixture.create(1);
    let first_create = fixture.next_creation().await;
    fixture.create(2);
    let second_create = fixture.next_creation().await;
    fixture.cleanup(10, "slow");
    let first_drop = fixture.next_drop().await;
    fixture.cleanup(11, "fast");
    let second_drop = fixture.next_drop().await;

    second_create.send(Ok(ReadString::from("second"))).unwrap();
    assert!(matches!(fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished { database_id: DatabaseId(2), result: Ok(name) }
            if name.as_ref() == "second"));
    second_drop.finish.send(Ok(())).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CleanupFinished { database_id: DatabaseId(11), result: Ok(()) }
    ));
    assert!(!first_create.is_closed());
    assert!(!first_drop.finish.is_closed());
    first_create.send(Ok(ReadString::from("first"))).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished { database_id: DatabaseId(1), result: Ok(_) }
    ));
    first_drop.finish.send(Ok(())).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CleanupFinished { database_id: DatabaseId(10), result: Ok(()) }
    ));
    fixture.finish().await;
}

#[tokio::test]
async fn ddl_failures_are_reported_and_workers_accept_later_jobs() {
    let mut fixture = Fixture::new();
    fixture.create(1);
    fixture
        .next_creation()
        .await
        .send(Err(PostgresDDLClientError::OperationFailed("create failed".into())))
        .unwrap();
    assert!(matches!(fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished {
            database_id: DatabaseId(1),
            result: Err(PostgresDDLClientError::OperationFailed(reason)),
        } if reason == "create failed"));

    fixture.cleanup(2, "retired");
    fixture
        .next_drop()
        .await
        .finish
        .send(Err(PostgresDDLClientError::OperationFailed("drop retired".into())))
        .unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CleanupFinished {
            database_id: DatabaseId(2),
            result: Err(PostgresDDLClientError::OperationFailed(_)),
        }
    ));

    fixture.create(3);
    fixture.next_creation().await.send(Ok(ReadString::from("healthy"))).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished { database_id: DatabaseId(3), result: Ok(_) }
    ));
    fixture.cleanup(4, "healthy_retired");
    fixture.next_drop().await.finish.send(Ok(())).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CleanupFinished { database_id: DatabaseId(4), result: Ok(()) }
    ));
    // These workers do not retry failed DDL jobs.
    fixture.finish().await;
    assert!(fixture.creates.try_recv().is_err());
    assert!(fixture.drops.try_recv().is_err());
}

#[tokio::test]
async fn closing_request_channels_drains_queued_jobs_and_exits_worker_loops() {
    let mut fixture = Fixture::new();
    fixture.create(1);
    fixture.cleanup(2, "old");
    fixture.senders.take();
    fixture.next_creation().await.send(Ok(ReadString::from("new"))).unwrap();
    fixture.next_drop().await.finish.send(Ok(())).unwrap();
    fixture.finish().await;

    let mut created = false;
    let mut cleaned = false;
    while let Some(message) = fixture.results.recv().await {
        match message {
            EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CreationFinished {
                database_id: DatabaseId(1),
                result: Ok(_),
            }) => {
                assert!(!created);
                created = true;
            }
            EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CleanupFinished {
                database_id: DatabaseId(2),
                result: Ok(()),
            }) => {
                assert!(!cleaned);
                cleaned = true;
            }
            _ => panic!("unexpected result"),
        }
    }
    assert!(created && cleaned);
}

#[tokio::test]
async fn shutdown_cancels_in_flight_ddl_and_discards_queued_work() {
    let mut fixture = Fixture::new();
    fixture.create(1);
    fixture.cleanup(2, "active");
    let active_create = fixture.next_creation().await;
    let active_drop = fixture.next_drop().await;
    fixture.create(3);
    fixture.cleanup(4, "queued");
    // No await between enqueueing and cancellation: queued work cannot start
    // first.
    fixture.shutdown.cancel();
    fixture.finish().await;
    assert!(active_create.is_closed());
    assert!(active_drop.finish.is_closed());
    assert!(fixture.creates.try_recv().is_err());
    assert!(fixture.drops.try_recv().is_err());
    assert!(fixture.results.recv().await.is_none(), "cancelled DDL must not report success");
}

#[tokio::test]
async fn closing_one_worker_queue_does_not_close_the_other() {
    let (engine_tx, _engine_rx) = hotpath::channel!(mpsc::unbounded_channel());
    let (senders, creation_rx, mut cleanup_rx) =
        DatabaseWorkerSenders::init_database_worker_channels();
    let io = WorkerEngineIO::new(engine_tx, TaskTracker::new(), CancellationToken::new(), senders);
    drop(creation_rx);
    assert!(matches!(
        io.request_creation(CreateDatabases {
            first_database_id: DatabaseId(1),
            amount: std::num::NonZeroUsize::MIN
        }),
        Err(IOError::FailedToSendTheMessage)
    ));
    io.request_cleanup(CleanupDatabase {
        database_id: DatabaseId(2),
        database_name: ReadString::from("old"),
    })
    .unwrap();
    assert_eq!(cleanup_rx.recv().await.unwrap().database_id, DatabaseId(2));
    drop(cleanup_rx);
    assert!(matches!(
        io.request_cleanup(CleanupDatabase {
            database_id: DatabaseId(3),
            database_name: ReadString::from("another"),
        }),
        Err(IOError::FailedToSendTheMessage)
    ));
}

#[tokio::test]
async fn creation_batch_maps_out_of_order_results_to_reserved_ids() {
    let mut fixture = Fixture::new();
    fixture
        .senders
        .as_ref()
        .unwrap()
        .creation_tx
        .send(CreateDatabases {
            first_database_id: DatabaseId(40),
            amount: std::num::NonZeroUsize::new(3).unwrap(),
        })
        .unwrap();
    let first = fixture.next_creation().await;
    let second = fixture.next_creation().await;
    let third = fixture.next_creation().await;

    second.send(Err(PostgresDDLClientError::OperationFailed("create failed".into()))).unwrap();
    assert!(matches!(
        fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished {
            database_id: DatabaseId(41),
            result: Err(PostgresDDLClientError::OperationFailed(_)),
        }
    ));
    third.send(Ok(ReadString::from("third"))).unwrap();
    assert!(matches!(fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished {
            database_id: DatabaseId(42), result: Ok(name),
        } if name.as_ref() == "third"));
    first.send(Ok(ReadString::from("first"))).unwrap();
    assert!(matches!(fixture.next_result().await,
        DatabaseWorkerMessages::CreationFinished {
            database_id: DatabaseId(40), result: Ok(name),
        } if name.as_ref() == "first"));
    fixture.finish().await;
    assert!(fixture.creates.try_recv().is_err());
    assert!(fixture.results.recv().await.is_none());
}
