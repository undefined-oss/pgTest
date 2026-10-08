use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

use pgtest_database_operations::manager::config::PostgresConfig;
use pgtest_utils::read_string::ReadString;
use rustc_hash::FxHashSet;
use tokio_util::sync::CancellationToken;

use super::{
    core::{LeaseId, WorkerEngine, WorkerEngineConfig},
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId, DatabaseWorkerMessages},
    errors::{AttachError, IOError, PostgresDDLClientError, ReleaseError},
    messages::{ConsumerReply, EngineMessage},
    test_support::{ConsumerWorker, PostgresConnection, WorkerInboxImpl, past_instant},
    traits::EngineIO,
};

type Inbox = Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>;
type Engine = WorkerEngine<ConsumerWorker, DeferredIO, WorkerInboxImpl, PostgresConnection>;

#[derive(Default)]
struct Operations {
    // Histories include attempts whose submission was rejected.
    creates: Vec<DatabaseId>,
    creation_batches: Vec<CreateDatabases>,
    cleanups: Vec<CleanupDatabase>,
    timers: Vec<(EngineMessage<ConsumerWorker>, CancellationToken)>,
    create_results: VecDeque<Result<(), IOError>>,
    cleanup_results: VecDeque<Result<(), IOError>>,
}

/// Records work without completing it, so tests can submit an entire burst
/// first.
#[derive(Clone)]
struct DeferredIO {
    inbox: Inbox,
    operations: Arc<Mutex<Operations>>,
}

impl EngineIO<ConsumerWorker> for DeferredIO {
    fn request_creation(&self, request: CreateDatabases) -> Result<(), IOError> {
        let mut operations = self.operations.lock().unwrap();
        operations
            .creates
            .extend((0..request.amount.get()).map(|index| request.database_id(index)));
        operations.creation_batches.push(request);
        operations.create_results.pop_front().unwrap_or(Ok(()))
    }

    fn request_cleanup(&self, request: CleanupDatabase) -> Result<(), IOError> {
        let mut operations = self.operations.lock().unwrap();
        operations.cleanups.push(request);
        operations.cleanup_results.pop_front().unwrap_or(Ok(()))
    }

    fn send_delayed_message(
        &self,
        message: EngineMessage<ConsumerWorker>,
        _: u32,
        cancellation: CancellationToken,
    ) -> Result<(), IOError> {
        self.operations.lock().unwrap().timers.push((message, cancellation));
        Ok(())
    }

    fn send_message(&self, message: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        self.inbox.lock().unwrap().push_back(message);
        Ok(())
    }
}

struct Fixture {
    engine: Engine,
    io: DeferredIO,
    consumer: ConsumerWorker,
}

impl Fixture {
    fn creation_ids(&self) -> Vec<DatabaseId> {
        self.io.operations.lock().unwrap().creates.clone()
    }

    fn release(&self, lease: &str) -> EngineMessage<ConsumerWorker> {
        EngineMessage::ReleaseLease {
            lease: LeaseId::new(lease).unwrap(),
            reply: self.consumer.clone(),
        }
    }

    async fn new(config: WorkerEngineConfig) -> Self {
        let inbox = Arc::new(Mutex::new(VecDeque::new()));
        let io = DeferredIO { inbox: inbox.clone(), operations: Arc::default() };
        let pg = Arc::new(PostgresConnection::start(PostgresConfig::default()));
        let mut engine = Engine::new(config, pg, io.clone(), WorkerInboxImpl::new(inbox));
        engine.try_init().await.unwrap();
        Self { engine, io, consumer: ConsumerWorker::new(Arc::default()) }
    }

    fn attach(&self, lease: &str) -> EngineMessage<ConsumerWorker> {
        EngineMessage::AttachOrJoin {
            lease: LeaseId::new(lease).unwrap(),
            reply: self.consumer.clone(),
            message_time: Instant::now(),
        }
    }

    async fn process(&mut self, messages: Vec<EngineMessage<ConsumerWorker>>) {
        self.io.inbox.lock().unwrap().extend(messages);
        self.engine.process_messages().await;

        let inventory = &self.engine.inventory;
        let mut seen = FxHashSet::default();

        let ids = inventory
            .creating()
            .iter()
            .copied()
            .chain(inventory.ready().iter().map(|database| database.database_id))
            .chain(self.engine.leases.values().map(|entry| entry.database.database_id))
            .chain(inventory.retiring().keys().copied());

        for database_id in ids {
            assert!(
                seen.insert(database_id),
                "database {database_id:?} appears more than once across lifecycle states"
            );
        }
    }

    async fn finish_creation(
        &mut self,
        database_id: DatabaseId,
        result: Result<ReadString, PostgresDDLClientError>,
    ) {
        self.process(vec![EngineMessage::DatabaseWorker(
            DatabaseWorkerMessages::CreationFinished { database_id, result },
        )])
        .await;
    }

    async fn finish_cleanup(
        &mut self,
        database_id: DatabaseId,
        result: Result<(), PostgresDDLClientError>,
    ) {
        self.process(vec![EngineMessage::DatabaseWorker(
            DatabaseWorkerMessages::CleanupFinished { database_id, result },
        )])
        .await;
    }
}

#[tokio::test]
async fn release_allows_new_attachments_while_cleanup_is_pending() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("test")]).await;
    let old = fixture.engine.leases["test"].clone();
    assert!(!old.cancellation.is_cancelled());

    fixture.process(vec![fixture.release("test"), fixture.release("test")]).await;
    assert!(old.cancellation.is_cancelled());
    assert!(!fixture.engine.leases.contains_key("test"));
    {
        let operations = fixture.io.operations.lock().unwrap();
        assert_eq!(operations.cleanups.len(), 1);
        assert_eq!(operations.cleanups[0].database_id, old.database.database_id);
        assert_eq!(operations.cleanups[0].database_name, old.database.database_name);
    }
    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(reply, ConsumerReply::ReleaseResult(Ok(()))))
            .count(),
        2
    );

    fixture.process(vec![fixture.attach("test"), fixture.attach("next")]).await;
    assert!(!fixture.engine.leases.contains_key("next"));
    let next_id = fixture.creation_ids()[0];
    assert_ne!(next_id, old.database.database_id);
    fixture.finish_creation(next_id, Ok(ReadString::from("fresh_database"))).await;

    let next = fixture.engine.leases["next"].clone();
    assert_eq!(next.database.database_id, next_id);
    assert_eq!(next.conns, 1);
    assert!(!next.cancellation.is_cancelled());
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(
        fixture.engine.inventory.retiring().get(&old.database.database_id),
        Some(&old.database.database_name)
    );

    fixture.finish_cleanup(old.database.database_id, Ok(())).await;
    fixture.process(vec![fixture.attach("test")]).await;
    assert!(fixture.engine.inventory.retiring().is_empty());
    assert!(!fixture.engine.leases.contains_key("test"));
    assert_eq!(fixture.engine.leases["next"].database.database_id, next_id);
    assert_eq!(fixture.engine.leases["next"].generation, next.generation);
    assert_eq!(fixture.engine.leases["next"].conns, 1);
    assert!(!fixture.engine.leases["next"].cancellation.is_cancelled());
    assert_eq!(fixture.io.operations.lock().unwrap().cleanups.len(), 1);
    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(
                reply,
                ConsumerReply::AttachRejected(AttachError::LeaseClosed)
            ))
            .count(),
        2
    );
}

#[tokio::test]
async fn releasing_an_unseen_id_does_not_allocate_a_database() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    let original = fixture.engine.inventory.ready()[0].clone();
    fixture.process(vec![fixture.release("late"), fixture.attach("late")]).await;

    assert!(fixture.engine.leases.is_empty());
    assert_eq!(fixture.engine.inventory.ready().len(), 1);
    assert_eq!(fixture.engine.inventory.ready()[0].database_id, original.database_id);
    assert!(fixture.engine.inventory.creating().is_empty());
    assert!(fixture.engine.inventory.retiring().is_empty());
    assert!(fixture.creation_ids().is_empty());
    assert!(fixture.io.operations.lock().unwrap().cleanups.is_empty());
    assert!(matches!(
        fixture.consumer.messages().back(),
        Some(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
    ));
}

#[tokio::test]
async fn release_fails_all_waiters_without_consuming_the_shared_creation() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("waiting"), fixture.attach("waiting")]).await;
    let requested = fixture.creation_ids();
    fixture.process(vec![fixture.release("waiting"), fixture.attach("survivor")]).await;

    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(
                reply,
                ConsumerReply::AttachRejected(AttachError::LeaseClosed)
            ))
            .count(),
        2
    );
    assert_eq!(fixture.creation_ids(), requested);
    assert!(fixture.io.operations.lock().unwrap().cleanups.is_empty());
    fixture.finish_creation(requested[0], Ok(ReadString::from("survivor_database"))).await;
    assert_eq!(fixture.engine.leases["survivor"].database.database_id, requested[0]);
    assert_eq!(fixture.engine.leases["survivor"].conns, 1);
    assert!(!fixture.engine.leases.contains_key("waiting"));
    assert!(fixture.engine.waiters.is_empty());
}

#[tokio::test]
async fn record_limit_reserves_room_for_closing_existing_leases() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        max_lease_records: std::num::NonZeroUsize::new(2).unwrap(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("active"), fixture.attach("pending")]).await;
    fixture.process(vec![fixture.release("unseen"), fixture.attach("unseen")]).await;
    assert!(fixture.consumer.messages().iter().any(|reply| matches!(
        reply,
        ConsumerReply::ReleaseResult(Err(ReleaseError::LeaseRecordLimitReached))
    )));
    assert!(fixture.consumer.messages().iter().any(|reply| matches!(
        reply,
        ConsumerReply::AttachRejected(AttachError::LeaseRecordLimitReached)
    )));
    fixture
        .process(vec![
            fixture.release("active"),
            fixture.release("pending"),
            fixture.release("active"),
        ])
        .await;
    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(reply, ConsumerReply::ReleaseResult(Ok(()))))
            .count(),
        3
    );
    assert!(fixture.engine.leases.is_empty());
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(fixture.io.operations.lock().unwrap().cleanups.len(), 1);
}

#[tokio::test]
async fn lost_release_reply_does_not_undo_closure_or_cleanup() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("test")]).await;
    let old = fixture.engine.leases["test"].clone();
    assert!(!old.cancellation.is_cancelled());
    fixture
        .process(vec![EngineMessage::ReleaseLease {
            lease: LeaseId::new("test").unwrap(),
            reply: ConsumerWorker::failing(Arc::default()),
        }])
        .await;
    fixture.process(vec![fixture.release("test"), fixture.attach("test")]).await;

    assert!(old.cancellation.is_cancelled());
    assert!(!fixture.engine.leases.contains_key("test"));
    assert_eq!(
        fixture.engine.inventory.retiring().get(&old.database.database_id),
        Some(&old.database.database_name)
    );
    let operations = fixture.io.operations.lock().unwrap();
    assert_eq!(operations.cleanups.len(), 1);
    assert_eq!(operations.cleanups[0].database_id, old.database.database_id);
    assert!(matches!(
        fixture.consumer.messages().back(),
        Some(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
    ));
}

#[tokio::test]
async fn old_generation_events_and_cleanup_cannot_retire_a_reused_lease_id() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("test")]).await;
    let old = fixture.engine.leases["test"].clone();
    fixture
        .process(vec![EngineMessage::LeaseMaxTimeReached {
            lease: LeaseId::new("test").unwrap(),
            generation: old.generation,
        }])
        .await;
    assert!(old.cancellation.is_cancelled());

    let new_id = fixture.creation_ids()[0];
    fixture.finish_creation(new_id, Ok(ReadString::from("new_generation"))).await;
    fixture.process(vec![fixture.attach("test")]).await;
    let new = fixture.engine.leases["test"].clone();
    assert_ne!(new.generation, old.generation);
    assert_ne!(new.database.database_id, old.database.database_id);
    assert!(!new.cancellation.is_cancelled());

    fixture
        .process(vec![
            EngineMessage::Detach {
                lease: LeaseId::new("test").unwrap(),
                generation: old.generation,
            },
            EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("test").unwrap(),
                generation: old.generation,
            },
        ])
        .await;
    fixture.finish_cleanup(old.database.database_id, Ok(())).await;
    fixture.finish_cleanup(old.database.database_id, Ok(())).await;
    assert_eq!(fixture.engine.leases["test"].database.database_id, new_id);
    assert_eq!(fixture.engine.leases["test"].generation, new.generation);
    assert_eq!(fixture.engine.leases["test"].conns, 1);
    assert!(!new.cancellation.is_cancelled());
    assert!(fixture.engine.inventory.retiring().is_empty());
    assert_eq!(fixture.io.operations.lock().unwrap().cleanups.len(), 1);
}

#[tokio::test]
async fn duplicate_expiry_schedules_cleanup_only_once() {
    let mut fixture = Fixture::new(WorkerEngineConfig::default()).await;
    fixture.process(vec![fixture.attach("test")]).await;
    let old = fixture.engine.leases["test"].clone();
    fixture
        .process(vec![
            EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("test").unwrap(),
                generation: old.generation,
            },
            EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("test").unwrap(),
                generation: old.generation,
            },
        ])
        .await;
    assert!(fixture.engine.leases.is_empty());
    assert!(old.cancellation.is_cancelled());
    assert_eq!(fixture.engine.inventory.retiring().len(), 1);
    assert_eq!(fixture.io.operations.lock().unwrap().cleanups.len(), 1);
    assert_eq!(fixture.engine.counters.rejected_attach_max_lifetime, 1);
}

#[tokio::test]
async fn last_disconnect_keeps_the_database_for_reconnect() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        lease_claim_timeout_ms: 0,
        ..WorkerEngineConfig::default()
    })
    .await;
    let lease = LeaseId::new("reconnecting").unwrap();
    fixture.process(vec![fixture.attach(&lease)]).await;
    let original = fixture.engine.leases[&lease].clone();
    let creates = fixture.creation_ids();
    fixture
        .process(vec![EngineMessage::Detach {
            lease: lease.clone(),
            generation: original.generation,
        }])
        .await;
    assert_eq!(fixture.engine.leases[&lease].conns, 0);
    assert!(!original.cancellation.is_cancelled());
    assert!(fixture.io.operations.lock().unwrap().cleanups.is_empty());
    assert!(fixture.io.operations.lock().unwrap().timers.is_empty());

    fixture.process(vec![fixture.attach(&lease)]).await;
    let reconnected = &fixture.engine.leases[&lease];
    assert_eq!(reconnected.conns, 1);
    assert_eq!(reconnected.database.database_id, original.database.database_id);
    assert_eq!(reconnected.database.database_name, original.database.database_name);
    assert_eq!(reconnected.generation, original.generation);
    assert_eq!(fixture.creation_ids(), creates, "joining must not request more supply");
}

#[tokio::test]
async fn startup_does_not_prefill_beyond_initial_size() {
    let fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        starvation_threshold: 8.into(),
        grow_batch_size: 4.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    assert_eq!(fixture.engine.inventory.ready().len(), 1);
    assert!(fixture.engine.inventory.creating().is_empty());
    assert!(fixture.engine.inventory.retiring().is_empty());
    assert!(fixture.creation_ids().is_empty());
}

#[tokio::test]
async fn covered_burst_does_not_schedule_redundant_batches() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 4.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder"), fixture.attach("a"), fixture.attach("b")]).await;
    assert_eq!(fixture.engine.inventory.creating().len(), 4);
    assert_eq!(fixture.creation_ids(), (2..=5).map(DatabaseId).collect::<Vec<_>>());
    let batches = &fixture.io.operations.lock().unwrap().creation_batches;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].first_database_id, DatabaseId(2));
    assert_eq!(batches[0].amount.get(), 4);
    assert!(fixture.engine.inventory.ready().is_empty());
    assert_eq!(fixture.consumer.messages().len(), 1);
}

#[tokio::test]
async fn excess_demand_grows_in_full_batches_before_any_completion() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 4.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder")]).await;
    assert_eq!(fixture.engine.inventory.creating().len(), 4);
    let burst = (0..10).map(|n| fixture.attach(&format!("waiting_{n}"))).collect();
    fixture.process(burst).await;
    assert_eq!(fixture.engine.inventory.creating().len(), 12, "ten waiting leases plus reserve");
    assert_eq!(fixture.creation_ids(), (2..=13).map(DatabaseId).collect::<Vec<_>>());
    assert_eq!(fixture.engine.waiters.len(), 10);
    assert_eq!(fixture.consumer.messages().len(), 1, "no creation has completed yet");
}

#[tokio::test]
async fn connections_for_one_lease_share_demand_and_one_completion() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 4.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    let burst = (0..20).map(|_| fixture.attach("shared")).collect();
    fixture.process(burst).await;
    assert_eq!(fixture.engine.inventory.creating().len(), 4);
    assert_eq!(fixture.engine.waiters.len(), 1);
    let id = fixture.creation_ids()[0];
    fixture.finish_creation(id, Ok(ReadString::from("shared_database"))).await;
    assert_eq!(fixture.consumer.messages().len(), 20);
    assert_eq!(fixture.engine.leases["shared"].conns, 20);
    assert_eq!(fixture.engine.leases["shared"].database.database_id, id);
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(fixture.engine.inventory.creating().len(), 3);
}

#[tokio::test]
async fn one_creation_serves_only_one_distinct_waiting_lease() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("first"), fixture.attach("second")]).await;
    let ids = fixture.creation_ids();
    fixture.finish_creation(ids[0], Ok(ReadString::from("first_database"))).await;
    assert_eq!(fixture.engine.leases["first"].database.database_id, ids[0]);
    assert!(!fixture.engine.leases.contains_key("second"));
    assert_eq!(
        fixture.engine.waiters.iter().cloned().collect::<Vec<_>>(),
        vec![LeaseId::new("second").unwrap()]
    );
    assert_eq!(fixture.consumer.messages().len(), 1);
    fixture.finish_creation(ids[1], Ok(ReadString::from("second_database"))).await;
    assert_eq!(fixture.engine.leases["second"].database.database_id, ids[1]);
    assert!(fixture.engine.waiters.is_empty());
}

#[tokio::test]
async fn settled_success_is_not_counted_as_both_ready_and_pending() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 2.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a")]).await;
    let ids = fixture.creation_ids();
    fixture.finish_creation(ids[0], Ok(ReadString::from("a_database"))).await;
    fixture.finish_creation(ids[1], Ok(ReadString::from("spare"))).await;
    fixture.finish_creation(ids[1], Ok(ReadString::from("duplicate"))).await;
    assert!(fixture.engine.inventory.creating().is_empty());
    assert_eq!(fixture.engine.inventory.ready().len(), 1);
    assert_eq!(fixture.engine.inventory.ready()[0].database_name, ReadString::from("spare"));
    fixture.process(vec![fixture.attach("b")]).await;
    assert_eq!(fixture.engine.leases["b"].database.database_id, ids[1]);
    assert_eq!(fixture.engine.inventory.creating().len(), 2);
    assert_eq!(fixture.creation_ids().len(), 4);
}

#[tokio::test]
async fn failed_creation_keeps_waiter_and_replenishes_without_reusing_failed_ids() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a")]).await;
    let ids = fixture.creation_ids();
    fixture.finish_creation(ids[0], Err(creation_failure())).await;
    assert_eq!(fixture.engine.inventory.creating().len(), 2);
    assert!(!fixture.engine.inventory.creating().contains(&ids[0]));
    assert!(fixture.engine.leases.is_empty());
    assert_eq!(fixture.engine.waiters.len(), 1);
    assert!(fixture.consumer.messages().is_empty());

    fixture.finish_creation(ids[0], Err(creation_failure())).await;
    assert_eq!(fixture.engine.counters.template_create_failures, 1);
    assert_eq!(fixture.creation_ids().len(), 3, "duplicate failure must not schedule more work");
    fixture.finish_creation(ids[1], Err(creation_failure())).await;
    let all_ids = fixture.creation_ids();
    assert_eq!(all_ids, (1..=4).map(DatabaseId).collect::<Vec<_>>());
    fixture.finish_creation(all_ids[2], Ok(ReadString::from("recovered"))).await;
    assert_eq!(fixture.engine.leases["a"].database.database_id, all_ids[2]);
    assert_eq!(fixture.consumer.messages().len(), 1);
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(fixture.engine.inventory.creating().len(), 1, "a spare is still being created");
}

fn creation_failure() -> PostgresDDLClientError {
    PostgresDDLClientError::OperationFailed("injected creation failure".into())
}

#[tokio::test]
async fn retiring_databases_do_not_count_as_supply_for_waiters() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 2.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder"), fixture.attach("other")]).await;
    let old = fixture.engine.leases["holder"].clone();
    fixture
        .process(vec![
            EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("holder").unwrap(),
                generation: old.generation,
            },
            fixture.attach("waiting"),
        ])
        .await;
    assert_eq!(fixture.engine.inventory.retiring().len(), 1);
    assert_eq!(
        fixture.engine.inventory.creating().len(),
        2,
        "waiter plus spare, excluding cleanup"
    );
    assert_eq!(fixture.creation_ids(), vec![DatabaseId(3), DatabaseId(4)]);
    fixture.finish_creation(DatabaseId(3), Ok(ReadString::from("waiting_database"))).await;
    assert_eq!(fixture.engine.leases["waiting"].database.database_id, DatabaseId(3));
    assert!(fixture.engine.inventory.retiring().contains_key(&old.database.database_id));
}

#[tokio::test]
async fn expired_and_undeliverable_groups_do_not_block_live_groups() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .process(vec![
            fixture.attach("holder"),
            EngineMessage::AttachOrJoin {
                lease: LeaseId::new("expired").unwrap(),
                reply: fixture.consumer.clone(),
                message_time: past_instant(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::new("gone").unwrap(),
                reply: ConsumerWorker::failing(Arc::default()),
                message_time: Instant::now(),
            },
            fixture.attach("live"),
        ])
        .await;
    let id = fixture.creation_ids()[0];
    fixture.finish_creation(id, Ok(ReadString::from("live_database"))).await;
    assert_eq!(fixture.consumer.messages().len(), 2);
    assert_eq!(fixture.engine.leases["live"].database.database_id, id);
    assert!(!fixture.engine.leases.contains_key("expired"));
    assert!(!fixture.engine.leases.contains_key("gone"));
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(fixture.engine.counters.waiter_timeouts, 1);
}

#[tokio::test]
async fn expired_groups_do_not_inflate_growth_demand() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .process(vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::new("expired").unwrap(),
                reply: fixture.consumer.clone(),
                message_time: past_instant(),
            },
            fixture.attach("live"),
        ])
        .await;
    assert_eq!(fixture.engine.inventory.creating().len(), 2, "one live lease plus one spare");
    let id = fixture.creation_ids()[0];
    fixture.finish_creation(id, Ok(ReadString::from("live_database"))).await;
    assert!(fixture.engine.leases.contains_key("live"));
}

#[tokio::test]
async fn failed_creation_submission_releases_reservation_without_retrying_inline() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .io
        .operations
        .lock()
        .unwrap()
        .create_results
        .push_back(Err(IOError::FailedToSendTheMessage));
    fixture.process(vec![fixture.attach("a")]).await;
    let rejected_id = fixture.creation_ids()[0];
    assert!(fixture.engine.inventory.creating().is_empty());
    assert_eq!(fixture.creation_ids().len(), 2);
    assert_eq!(fixture.engine.counters.unable_to_start_database_slots, 2);
    assert_eq!(fixture.engine.waiters.len(), 1);

    fixture.process(vec![fixture.attach("a")]).await;
    let ids = fixture.creation_ids();
    assert_eq!(ids.len(), 4);
    assert!(ids[2].0 > rejected_id.0);
    fixture.finish_creation(ids[2], Ok(ReadString::from("recovered"))).await;
    assert_eq!(fixture.engine.leases["a"].conns, 2);
    assert_eq!(fixture.engine.leases["a"].database.database_id, ids[2]);
}

#[tokio::test]
async fn repeated_submission_failure_does_not_loop_or_accumulate_reservations() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .io
        .operations
        .lock()
        .unwrap()
        .create_results
        .extend((0..3).map(|_| Err(IOError::FailedToSendTheMessage)));
    for expected in 1..=3 {
        fixture.process(vec![fixture.attach("a")]).await;
        assert!(fixture.engine.inventory.creating().is_empty());
        assert_eq!(fixture.creation_ids().len(), expected * 2);
        assert_eq!(fixture.engine.waiters.len(), 1);
    }
    assert_eq!(fixture.engine.counters.unable_to_start_database_slots, 6);
    assert_eq!(fixture.creation_ids(), (1..=6).map(DatabaseId).collect::<Vec<_>>());
    assert!(fixture.consumer.messages().is_empty());
}

#[tokio::test]
async fn unknown_creation_completion_cannot_supply_a_database() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a")]).await;
    let requested = fixture.creation_ids();
    fixture.finish_creation(DatabaseId(999), Ok(ReadString::from("unrequested"))).await;
    assert!(fixture.engine.leases.is_empty());
    assert!(fixture.engine.inventory.ready().is_empty());
    assert_eq!(fixture.creation_ids(), requested);
    assert_eq!(fixture.engine.inventory.creating().len(), requested.len());
    assert_eq!(fixture.engine.waiters.len(), 1);
}

#[tokio::test]
async fn duplicate_creation_completions_cannot_restore_an_assigned_database() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 1.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder")]).await;
    let database_id = fixture.creation_ids()[0];
    fixture.finish_creation(database_id, Ok(ReadString::from("assigned"))).await;
    let requested = fixture.creation_ids();

    fixture.finish_creation(database_id, Ok(ReadString::from("duplicate"))).await;
    fixture
        .finish_creation(
            database_id,
            Err(PostgresDDLClientError::OperationFailed("stale failure".into())),
        )
        .await;

    let assigned = &fixture.engine.leases["holder"].database;
    assert_eq!(assigned.database_id, database_id);
    assert_eq!(assigned.database_name, ReadString::from("assigned"));
    assert!(fixture.engine.inventory.ready().is_empty());
    assert_eq!(fixture.engine.inventory.creating().len(), 1, "only the spare is pending");
    assert_eq!(fixture.creation_ids(), requested);
    assert_eq!(fixture.engine.counters.template_create_failures, 0);
}

#[tokio::test]
async fn cleanup_completions_cannot_remove_ready_or_assigned_databases() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1.into(),
        grow_batch_size: 0.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    let database_id = fixture.engine.inventory.ready()[0].database_id;
    fixture.finish_cleanup(database_id, Ok(())).await;
    assert_eq!(fixture.engine.inventory.ready()[0].database_id, database_id);

    fixture.process(vec![fixture.attach("holder")]).await;
    fixture.finish_cleanup(database_id, Ok(())).await;
    assert_eq!(fixture.engine.leases["holder"].database.database_id, database_id);

    fixture.process(vec![fixture.release("holder")]).await;
    assert!(fixture.engine.inventory.retiring().contains_key(&database_id));
    fixture.finish_cleanup(database_id, Ok(())).await;
    fixture.finish_cleanup(database_id, Ok(())).await;
    assert!(fixture.engine.inventory.retiring().is_empty());
    assert_eq!(fixture.engine.inventory.supply_len(), 0);
    assert_eq!(fixture.io.operations.lock().unwrap().cleanups.len(), 1);
}

#[tokio::test]
async fn cleanup_failure_retains_database_identity_without_blocking_creation() {
    for submission_fails in [false, true] {
        let mut fixture = Fixture::new(WorkerEngineConfig {
            initial_slots: 1.into(),
            starvation_threshold: 0.into(),
            grow_batch_size: 1.into(),
            ..WorkerEngineConfig::default()
        })
        .await;
        fixture.process(vec![fixture.attach("old")]).await;
        let old = fixture.engine.leases["old"].database.clone();
        if submission_fails {
            fixture
                .io
                .operations
                .lock()
                .unwrap()
                .cleanup_results
                .push_back(Err(IOError::FailedToSendTheMessage));
        }
        fixture.process(vec![fixture.release("old"), fixture.attach("next")]).await;
        if !submission_fails {
            fixture
                .finish_cleanup(
                    old.database_id,
                    Err(PostgresDDLClientError::OperationFailed("injected drop failure".into())),
                )
                .await;
        }
        assert_eq!(
            fixture.engine.inventory.retiring().get(&old.database_id),
            Some(&old.database_name)
        );
        let new_id = fixture.creation_ids()[0];
        fixture.finish_creation(new_id, Ok(ReadString::from("next_database"))).await;
        assert_eq!(fixture.engine.leases["next"].database.database_id, new_id);
        assert_ne!(new_id, old.database_id);
        // The current implementation retains failed cleanup, but does not
        // reschedule it.
        assert_eq!(fixture.io.operations.lock().unwrap().cleanups.len(), 1);
    }
}

#[tokio::test]
async fn zero_batch_size_disables_growth() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0.into(),
        starvation_threshold: 0.into(),
        grow_batch_size: 0.into(),
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a"), fixture.attach("b")]).await;
    assert!(fixture.engine.inventory.ready().is_empty());
    assert!(fixture.engine.inventory.creating().is_empty());
    assert!(fixture.creation_ids().is_empty());
    assert_eq!(fixture.engine.waiters.len(), 2);
}
