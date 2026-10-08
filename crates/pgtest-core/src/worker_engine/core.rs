use std::{
    collections::VecDeque, marker::PhantomData, num::NonZeroUsize, sync::Arc, time::Instant,
};

use derive_more::{Deref, Display, From, FromStr, Into};
use envconfig::Envconfig;
use pgtest_utils::read_string::ReadString;
use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::worker_engine::{
    database_inventory::{Database, DatabaseInventory},
    database_jobs::DatabaseWorkerMessages,
    errors::{AttachError, PostgresDDLClientError, ReleaseError},
    messages::{ConsumerReply, EngineMessage},
    traits::{ConsumerIO, EngineIO, EngineInbox, PostgresClient},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct InitialSlots(u16);

impl Default for InitialSlots {
    fn default() -> Self {
        Self(16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct StarvationThreshold(u16);

impl Default for StarvationThreshold {
    fn default() -> Self {
        Self(8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct GrowBatchSize(u16);

impl Default for GrowBatchSize {
    fn default() -> Self {
        Self(16)
    }
}

#[derive(Envconfig, Debug, Clone, Copy)]
pub struct WorkerEngineConfig {
    #[envconfig(from = "PGTEST_POOL_INITIAL_SIZE", default = "16")]
    pub initial_slots: InitialSlots,
    #[envconfig(from = "PGTEST_POOL_STARVATION_THRESHOLD", default = "8")]
    pub starvation_threshold: StarvationThreshold,
    #[envconfig(from = "PGTEST_POOL_GROW_BATCH_SIZE", default = "16")]
    pub grow_batch_size: GrowBatchSize,

    #[envconfig(from = "PGTEST_LEASE_CLAIM_TIMEOUT_MS", default = "30000")]
    pub lease_claim_timeout_ms: u64,
    /// Counts every distinct admitted ID, including pending and closed leases.
    #[envconfig(from = "PGTEST_MAX_LEASE_RECORDS", default = "100000")]
    pub max_lease_records: NonZeroUsize,
}

pub use super::lease_id::LeaseId;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LeaseStatus {
    Open,
    Closed,
}

#[cfg_attr(test, derive(Clone, Debug))]
pub(crate) struct LeaseEntry {
    pub(crate) database: Database,
    pub(crate) conns: u16,
    pub(crate) generation: u64,
    pub(crate) cancellation: CancellationToken,
}

pub(crate) struct WorkerEngine<Consumer, IO, Inbox, Postgres>
where
    Consumer: ConsumerIO,
    IO: EngineIO<Consumer>,
    Inbox: EngineInbox<Consumer>,
    Postgres: PostgresClient,
{
    pub(crate) leases: FxHashMap<LeaseId, LeaseEntry>,
    // Identity records survive physical cleanup. Slots and connection counts do not.
    lease_records: FxHashMap<LeaseId, LeaseStatus>,
    next_generation: u64,
    config: WorkerEngineConfig,
    pg_client: Arc<Postgres>,
    pub(crate) waiters: VecDeque<LeaseId>,
    group_waiters: FxHashMap<LeaseId, Vec<(Consumer, Instant)>>,
    pub(crate) counters: EngineCounters,
    engine_io: IO,
    inbox: Inbox,
    consumer: PhantomData<Consumer>,
    root_cancellation_token: CancellationToken,
    pub inventory: DatabaseInventory,
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug))]
pub struct EngineCounters {
    pub rejected_attach_max_lifetime: u64,
    pub waiter_timeouts: u64,
    pub template_create_failures: u64,
    pub detach_on_zero: u64,
    pub non_ready_slots: u64,
    pub unable_to_start_database_slots: u64,
}

impl<Consumer, IO, Inbox, Postgres> WorkerEngine<Consumer, IO, Inbox, Postgres>
where
    Consumer: ConsumerIO,
    IO: EngineIO<Consumer>,
    Inbox: EngineInbox<Consumer>,
    Postgres: PostgresClient,
{
    pub fn new(
        pool_worker_config: WorkerEngineConfig,
        postgres_manager: Arc<Postgres>,
        engine_io: IO,
        inbox: Inbox,
    ) -> WorkerEngine<Consumer, IO, Inbox, Postgres> {
        let leases = FxHashMap::default();
        let root_cancellation_token = CancellationToken::new();

        WorkerEngine::<Consumer, IO, Inbox, Postgres> {
            leases,
            lease_records: FxHashMap::default(),
            next_generation: 0,
            config: pool_worker_config.clone(),
            pg_client: postgres_manager,
            waiters: VecDeque::new(),
            group_waiters: FxHashMap::default(),
            counters: EngineCounters::default(),
            engine_io,
            inbox,
            consumer: PhantomData,
            root_cancellation_token,
            inventory: DatabaseInventory::default(),
        }
    }

    #[hotpath::measure]
    pub async fn try_init(&mut self) -> Result<(), PostgresDDLClientError> {
        let Some(request) =
            self.inventory.reserve_creations(usize::from(*self.config.initial_slots))
        else {
            return Ok(());
        };
        let mut first_error = None;
        let inventory = &mut self.inventory;
        self.pg_client
            .create_databases(request.amount.get(), |index, result| {
                let database_id = request.database_id(index);
                if let Ok(database_name) = &result {
                    tracing::info!(?database_id, %database_name, "created initial database");
                }
                if let Err(error) = inventory
                    .complete_creation(database_id, result)
                    .expect("initial creation must have a pending reservation")
                {
                    tracing::error!(?database_id, %error, "initial database creation failed");
                    first_error.get_or_insert(error);
                }
            })
            .await;
        first_error.map_or(Ok(()), Err)
    }

    #[hotpath::measure]
    pub async fn run(&mut self) {
        self.process_messages().await;
        self.root_cancellation_token.cancel();
    }

    pub(super) async fn process_messages(&mut self) {
        while let Some(msg) = self.inbox.wait_for_message().await {
            match msg {
                EngineMessage::AttachOrJoin { lease, reply, message_time } => {
                    self.attach_or_join(lease, reply, message_time);
                }
                EngineMessage::ReleaseLease { lease, reply } => {
                    let result = self.release_lease(&lease);
                    // Closure and cleanup belong to the engine even if this
                    // reply is lost.
                    let _ = reply.reply(ConsumerReply::ReleaseResult(result));
                }
                EngineMessage::Detach { lease, generation } => match self.leases.get_mut(&lease) {
                    Some(leased_worker) if leased_worker.generation == generation => {
                        leased_worker.conns = if leased_worker.conns == 0 {
                            self.counters.detach_on_zero += 1;
                            leased_worker.conns
                        } else {
                            leased_worker.conns - 1
                        };
                    }
                    _ => {}
                },
                EngineMessage::LeaseMaxTimeReached { lease, generation } => {
                    if self.leases.get(&lease).is_some_and(|entry| entry.generation == generation) {
                        self.counters.rejected_attach_max_lifetime += 1;
                        self.retire_lease(&lease);
                    }
                }
                EngineMessage::DatabaseWorker(message) => {
                    self.handle_database_worker_message(message);
                }
                #[cfg(test)]
                EngineMessage::Barrier { reply } => {
                    let _ = reply.send(());
                }
                EngineMessage::Shutdown => {
                    self.root_cancellation_token.cancel();
                    break;
                }
            }
        }
    }

    #[hotpath::measure]
    fn handle_database_worker_message(&mut self, message: DatabaseWorkerMessages) {
        match message {
            DatabaseWorkerMessages::CreationFinished { database_id, result } => {
                let Some(result) = self.inventory.complete_creation(database_id, result) else {
                    tracing::warn!(?database_id, "creation result without a pending reservation");
                    return;
                };

                if let Err(error) = result {
                    self.counters.template_create_failures += 1;
                    tracing::error!(?database_id, %error, "database creation failed");
                }

                self.dispatch_waiters();
                self.grow();
            }
            DatabaseWorkerMessages::CleanupFinished { database_id, result } => {
                let Some(result) = self.inventory.complete_cleanup(database_id, result) else {
                    tracing::warn!(?database_id, "cleanup result without a retirement record");
                    return;
                };

                if let Err(error) = result {
                    tracing::error!(?database_id, %error, "database cleanup failed; retaining retirement record");
                }
            }
        }
    }

    fn admit_lease(&mut self, lease: &LeaseId) -> Result<LeaseStatus, ReleaseError> {
        if let Some(status) = self.lease_records.get(lease) {
            return Ok(*status);
        }
        if self.lease_records.len() >= self.config.max_lease_records.get() {
            return Err(ReleaseError::LeaseRecordLimitReached);
        }
        self.lease_records.insert(lease.clone(), LeaseStatus::Open);
        Ok(LeaseStatus::Open)
    }

    fn release_lease(&mut self, lease: &LeaseId) -> Result<(), ReleaseError> {
        if self.admit_lease(lease)? == LeaseStatus::Closed {
            return Ok(());
        }

        self.lease_records.insert(lease.clone(), LeaseStatus::Closed);

        if let Some(waiters) = self.group_waiters.remove(lease) {
            for (reply, _) in waiters {
                let _ = reply.reply(ConsumerReply::AttachRejected(AttachError::LeaseClosed));
            }
        }

        self.waiters.retain(|waiting| waiting != lease);
        self.retire_lease(lease);

        Ok(())
    }

    fn attach_or_join(&mut self, lease: LeaseId, reply: Consumer, message_time: Instant) {
        match self.admit_lease(&lease) {
            Ok(LeaseStatus::Closed) => {
                let _ = reply.reply(ConsumerReply::AttachRejected(AttachError::LeaseClosed));
                return;
            }
            Err(error) => {
                let error = match error {
                    ReleaseError::InvalidLeaseId => AttachError::InvalidLeaseId,
                    _ => AttachError::LeaseRecordLimitReached,
                };
                let _ = reply.reply(ConsumerReply::AttachRejected(error));
                return;
            }
            Ok(LeaseStatus::Open) => {}
        }

        if let Some(entry) = self.leases.get(&lease) {
            let database_name = entry.database.database_name.clone();
            self.reply_attached(&lease, database_name, reply);
            return;
        }

        let Some(database) = self.inventory.take_ready() else {
            if !self.group_waiters.contains_key(&lease) {
                self.waiters.push_back(lease.clone());
            }

            self.group_waiters.entry(lease).or_default().push((reply, message_time));
            self.grow();
            return;
        };

        let database_name = self.assign_database(&lease, database);
        self.reply_attached(&lease, database_name, reply);

        if self.restore_unclaimed_database(&lease) {
            self.dispatch_waiters();
        }
        self.grow();
    }

    #[hotpath::measure]
    fn dispatch_waiters(&mut self) {
        while !self.inventory.ready().is_empty() {
            let Some(lease) = self.waiters.pop_front() else {
                break;
            };

            let Some(replies) = self.group_waiters.remove(&lease) else {
                continue;
            };

            if self.lease_records.get(&lease) == Some(&LeaseStatus::Closed) {
                for (reply, _) in replies {
                    let _ = reply.reply(ConsumerReply::AttachRejected(AttachError::LeaseClosed));
                }
                continue;
            }

            let already_assigned = self.leases.contains_key(&lease);

            for (reply, message_time) in replies {
                if self.config.lease_claim_timeout_ms > 0
                    && message_time.elapsed().as_millis()
                        > u128::from(self.config.lease_claim_timeout_ms)
                {
                    self.counters.waiter_timeouts += 1;
                    continue;
                }

                let database_name = match self.leases.get(&lease) {
                    Some(entry) => entry.database.database_name.clone(),
                    None => {
                        let database = self
                            .inventory
                            .take_ready()
                            .expect("dispatch requires a ready database");

                        self.assign_database(&lease, database)
                    }
                };

                self.reply_attached(&lease, database_name, reply);
            }

            if !already_assigned {
                self.restore_unclaimed_database(&lease);
            }
        }
    }

    fn reply_attached(&mut self, lease: &LeaseId, database_name: ReadString, reply: Consumer) {
        let entry = self.leases.get_mut(lease).expect("reply requires an assigned lease");
        let Some(conns) = entry.conns.checked_add(1) else {
            let _ = reply.reply(ConsumerReply::FailedToAttach);
            return;
        };
        entry.conns = conns;
        if reply
            .reply(ConsumerReply::Attached {
                database_name,
                generation: entry.generation,
                cancellation: entry.cancellation.clone(),
            })
            .is_err()
        {
            entry.conns -= 1;
        }
    }

    fn restore_unclaimed_database(&mut self, lease: &LeaseId) -> bool {
        if !self.leases.get(lease).is_some_and(|entry| entry.conns == 0) {
            return false;
        }
        let entry = self.leases.remove(lease).unwrap();
        entry.cancellation.cancel();
        self.inventory.return_ready(entry.database);

        true
    }

    #[hotpath::measure]
    fn assign_database(&mut self, lease: &LeaseId, database: Database) -> ReadString {
        debug_assert!(!self.leases.contains_key(lease));

        let database_name = database.database_name.clone();

        self.next_generation =
            self.next_generation.checked_add(1).expect("lease generation exhausted");

        let generation = self.next_generation;
        let cancellation = self.root_cancellation_token.child_token();

        self.leases.insert(
            lease.clone(),
            LeaseEntry { database, conns: 0, generation, cancellation: cancellation.clone() },
        );

        if self.config.lease_claim_timeout_ms > 0 {
            if self
                .engine_io
                .send_delayed_message(
                    EngineMessage::LeaseMaxTimeReached { lease: lease.clone(), generation },
                    self.config.lease_claim_timeout_ms as u32,
                    cancellation,
                )
                .is_err()
            {
                tracing::error!(%lease, "unable to schedule lease expiration")
            }
        }

        database_name
    }

    pub(crate) fn grow(&mut self) {
        let batch_size = usize::from(*self.config.grow_batch_size);
        if batch_size == 0 {
            return;
        }

        let waiting = self
            .group_waiters
            .iter()
            .filter(|(lease, replies)| {
                !self.leases.contains_key(*lease)
                    && replies.iter().any(|(_, queued_at)| {
                        self.config.lease_claim_timeout_ms == 0
                            || queued_at.elapsed().as_millis()
                                <= u128::from(self.config.lease_claim_timeout_ms)
                    })
            })
            .count();
        // Preserve the inclusive starvation threshold after covering live
        // leases.
        let required = waiting + usize::from(*self.config.starvation_threshold) + 1;

        let deficit = required.saturating_sub(self.inventory.supply_len());

        if deficit == 0 {
            return;
        }

        let count = deficit.div_ceil(batch_size) * batch_size;

        let request = self.inventory.reserve_creations(count).expect("growth count is nonzero");
        if let Err(error) = self.engine_io.request_creation(request) {
            for index in 0..request.amount.get() {
                self.inventory.cancel_creation(request.database_id(index));
            }
            self.counters.unable_to_start_database_slots +=
                u64::try_from(count).expect("creation count fits database identity");
            tracing::error!(?request, %error, "unable to enqueue database creation batch");
        }
    }

    #[hotpath::measure]
    fn retire_lease(&mut self, lease: &LeaseId) {
        let Some(entry) = self.leases.remove(lease) else {
            return;
        };

        entry.cancellation.cancel();

        let request = self.inventory.retire(entry.database);
        let database_id = request.database_id;

        if let Err(error) = self.engine_io.request_cleanup(request) {
            tracing::error!(?database_id, %lease, %error, "unable to enqueue cleanup; retaining retirement record")
        }

        self.grow();
    }

    #[cfg(test)]
    pub fn snapshot<'a>(&'a self) -> &'a Self {
        self
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Default for WorkerEngineConfig {
    fn default() -> Self {
        Self {
            initial_slots: 4.into(),
            starvation_threshold: 2.into(),
            grow_batch_size: 4.into(),

            lease_claim_timeout_ms: 30_000,
            max_lease_records: std::num::NonZeroUsize::new(100_000).unwrap(),
        }
    }
}

#[cfg(test)]
mod config_tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn pool_settings_defaults_and_bounds_match_configuration() {
        let config = WorkerEngineConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(config.initial_slots, InitialSlots::default());
        assert_eq!(config.starvation_threshold, StarvationThreshold::default());
        assert_eq!(config.grow_batch_size, GrowBatchSize::default());

        for variable in [
            "PGTEST_POOL_INITIAL_SIZE",
            "PGTEST_POOL_STARVATION_THRESHOLD",
            "PGTEST_POOL_GROW_BATCH_SIZE",
        ] {
            for invalid in ["-1", "65536", "invalid"] {
                let vars = HashMap::from([(variable.to_owned(), invalid.to_owned())]);
                assert!(
                    WorkerEngineConfig::init_from_hashmap(&vars).is_err(),
                    "{variable}={invalid}"
                );
            }
            let vars = HashMap::from([(variable.to_owned(), "65535".to_owned())]);
            assert!(WorkerEngineConfig::init_from_hashmap(&vars).is_ok(), "{variable}=65535");
        }
    }

    #[test]
    fn lease_record_limit_must_be_positive() {
        for invalid in ["0", "-1", "invalid", "184467440737095516160"] {
            let vars = HashMap::from([("PGTEST_MAX_LEASE_RECORDS".to_owned(), invalid.to_owned())]);
            assert!(WorkerEngineConfig::init_from_hashmap(&vars).is_err(), "{invalid}");
        }
        let vars = HashMap::from([("PGTEST_MAX_LEASE_RECORDS".to_owned(), "1".to_owned())]);
        assert_eq!(
            WorkerEngineConfig::init_from_hashmap(&vars).unwrap().max_lease_records.get(),
            1
        );
    }

    #[test]
    fn zero_settings_keep_their_existing_meaning() {
        let vars: HashMap<_, _> = [
            "PGTEST_POOL_INITIAL_SIZE",
            "PGTEST_POOL_STARVATION_THRESHOLD",
            "PGTEST_POOL_GROW_BATCH_SIZE",
            "PGTEST_LEASE_CLAIM_TIMEOUT_MS",
        ]
        .into_iter()
        .map(|key| (key.to_owned(), "0".to_owned()))
        .collect();
        let config = WorkerEngineConfig::init_from_hashmap(&vars).unwrap();
        assert_eq!(*config.initial_slots, 0);
        assert_eq!(*config.starvation_threshold, 0);
        assert_eq!(*config.grow_batch_size, 0);
        assert_eq!(config.lease_claim_timeout_ms, 0);
        assert_eq!(config.max_lease_records.get(), 100_000);
    }
}
