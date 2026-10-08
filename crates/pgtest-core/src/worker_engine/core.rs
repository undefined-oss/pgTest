use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

use derive_more::{Deref, Display, From, FromStr, Into};
use envconfig::Envconfig;
use pgtest_engine_backend::{BackendError, ResourceId};
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    database_inventory::{Database, DatabaseInventory},
    database_jobs::{DatabaseId, DatabaseWorkerMessages},
    errors::{AttachError, ReleaseError},
    messages::{ConsumerReply, EngineMessage, LeaseKey, RequestId, Tick},
    traits::EngineIO,
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

#[derive(Clone, Debug)]
pub struct LeaseEntry {
    pub database: Database,
    pub conns: u16,
    pub generation: u64,
}

pub struct WorkerEngine<IO: EngineIO> {
    pub(crate) leases: FxHashMap<LeaseId, LeaseEntry>,
    lease_records: FxHashMap<LeaseId, LeaseStatus>,
    next_generation: u64,
    config: WorkerEngineConfig,
    template: String,
    pub(crate) waiters: VecDeque<LeaseId>,
    group_waiters: FxHashMap<LeaseId, Vec<(RequestId, Tick)>>,
    pub counters: EngineCounters,
    engine_io: IO,
    now: Tick,
    pub inventory: DatabaseInventory,
    initial_cleanup: FxHashSet<DatabaseId>,
    initial_creation: FxHashSet<DatabaseId>,
    startup: Option<Result<(), BackendError>>,
    first_error: Option<BackendError>,
    stopped: bool,
    initialized: bool,
}

#[derive(Default, Clone, Debug)]
pub struct EngineCounters {
    pub rejected_attach_max_lifetime: u64,
    pub waiter_timeouts: u64,
    pub template_create_failures: u64,
    pub detach_on_zero: u64,
    pub non_ready_slots: u64,
    pub unable_to_start_database_slots: u64,
}

impl<IO: EngineIO> WorkerEngine<IO> {
    pub fn new(config: WorkerEngineConfig, template: String, engine_io: IO) -> Self {
        Self {
            leases: FxHashMap::default(),
            lease_records: FxHashMap::default(),
            next_generation: 0,
            config,
            template,
            waiters: VecDeque::new(),
            group_waiters: FxHashMap::default(),
            counters: EngineCounters::default(),
            engine_io,
            now: Tick::default(),
            inventory: DatabaseInventory::default(),
            initial_cleanup: FxHashSet::default(),
            initial_creation: FxHashSet::default(),
            startup: None,
            first_error: None,
            stopped: false,
            initialized: false,
        }
    }

    /// Initialization uses exactly the same worker messages as replenishment.
    pub fn initialize(&mut self, stale: Vec<ResourceId>) {
        assert!(!self.initialized, "manager may only initialize once");
        self.initialized = true;
        for resource_id in stale {
            let request = self.inventory.retire_resource(resource_id);
            self.initial_cleanup.insert(request.database_id);
            let id = request.database_id;
            if self.engine_io.request_cleanup(request).is_err() {
                self.initial_cleanup.remove(&id);
            }
        }
        if self.initial_cleanup.is_empty() {
            self.create_initial();
        }
    }

    fn create_initial(&mut self) {
        let Some(request) =
            self.inventory.reserve_creations(usize::from(*self.config.initial_slots))
        else {
            self.startup = Some(Ok(()));
            return;
        };
        for index in 0..request.amount.get() {
            self.initial_creation.insert(request.database_id(index));
        }
        if self.engine_io.request_creation(request).is_err() {
            for id in self.initial_creation.drain() {
                self.inventory.cancel_creation(id);
            }
            self.startup =
                Some(Err(BackendError::OperationFailed("creation worker unavailable".into())));
        }
    }

    #[cfg(feature = "tokio-runtime")]
    pub(crate) fn io_mut(&mut self) -> &mut IO {
        &mut self.engine_io
    }

    pub fn startup_result(&self) -> Option<&Result<(), BackendError>> {
        self.startup.as_ref()
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    pub fn leases(&self) -> &FxHashMap<LeaseId, LeaseEntry> {
        &self.leases
    }

    pub fn waiters(&self) -> &VecDeque<LeaseId> {
        &self.waiters
    }

    pub fn shutdown(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        for (lease, entry) in &self.leases {
            self.engine_io
                .cancel_sessions(&LeaseKey { lease: lease.clone(), generation: entry.generation });
        }
        for (_, replies) in self.group_waiters.drain() {
            for (reply, _) in replies {
                let _ = self
                    .engine_io
                    .reply(reply, ConsumerReply::AttachRejected(AttachError::EngineUnavailable));
            }
        }
        self.waiters.clear();
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    pub fn handle(&mut self, msg: EngineMessage, now: Tick) {
        if self.stopped {
            return;
        }
        self.now = now;
        match msg {
            EngineMessage::AttachOrJoin { template, lease, reply, message_time } => {
                if template != self.template {
                    let _ = self
                        .engine_io
                        .reply(reply, ConsumerReply::AttachRejected(AttachError::TemplateMismatch));
                } else {
                    self.attach_or_join(lease, reply, message_time);
                }
            }
            EngineMessage::ReleaseLease { lease, reply } => {
                let result = self.release_lease(&lease);
                // Closure and cleanup belong to the engine even if this
                // reply is lost.
                let _ = self.engine_io.reply(reply, ConsumerReply::ReleaseResult(result));
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
            EngineMessage::Shutdown => {
                self.shutdown();
            }
        }
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn handle_database_worker_message(&mut self, message: DatabaseWorkerMessages) {
        match message {
            DatabaseWorkerMessages::CreationFinished { database_id, result } => {
                let Some(result) = self.inventory.complete_creation(database_id, result) else {
                    tracing::warn!(?database_id, "creation result without a pending reservation");
                    return;
                };

                if self.initial_creation.remove(&database_id) {
                    if let Err(error) = result {
                        self.first_error.get_or_insert(error);
                    }
                    if self.initial_creation.is_empty() {
                        self.startup = Some(self.first_error.take().map_or(Ok(()), Err));
                    }
                    return;
                }
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
                if self.initial_cleanup.remove(&database_id) && self.initial_cleanup.is_empty() {
                    self.create_initial();
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
                let _ = self
                    .engine_io
                    .reply(reply, ConsumerReply::AttachRejected(AttachError::LeaseClosed));
            }
        }

        self.waiters.retain(|waiting| waiting != lease);
        self.retire_lease(lease);

        Ok(())
    }

    fn attach_or_join(&mut self, lease: LeaseId, reply: RequestId, message_time: Tick) {
        if !matches!(self.startup, Some(Ok(()))) {
            let _ = self
                .engine_io
                .reply(reply, ConsumerReply::AttachRejected(AttachError::EngineUnavailable));
            return;
        }
        match self.admit_lease(&lease) {
            Ok(LeaseStatus::Closed) => {
                let _ = self
                    .engine_io
                    .reply(reply, ConsumerReply::AttachRejected(AttachError::LeaseClosed));
                return;
            }
            Err(error) => {
                let error = match error {
                    ReleaseError::InvalidLeaseId => AttachError::InvalidLeaseId,
                    _ => AttachError::LeaseRecordLimitReached,
                };
                let _ = self.engine_io.reply(reply, ConsumerReply::AttachRejected(error));
                return;
            }
            Ok(LeaseStatus::Open) => {}
        }

        if let Some(entry) = self.leases.get(&lease) {
            let database = entry.database.clone();
            self.reply_attached(&lease, database, reply);
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

        let database = self.assign_database(&lease, database);
        self.reply_attached(&lease, database, reply);

        if self.restore_unclaimed_database(&lease) {
            self.dispatch_waiters();
        }
        self.grow();
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
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
                    let _ = self
                        .engine_io
                        .reply(reply, ConsumerReply::AttachRejected(AttachError::LeaseClosed));
                }
                continue;
            }

            let already_assigned = self.leases.contains_key(&lease);

            for (reply, message_time) in replies {
                if self.config.lease_claim_timeout_ms > 0
                    && self.now.elapsed_since(message_time).as_millis()
                        > u128::from(self.config.lease_claim_timeout_ms)
                {
                    self.counters.waiter_timeouts += 1;
                    continue;
                }

                let database = match self.leases.get(&lease) {
                    Some(entry) => entry.database.clone(),
                    None => {
                        let database = self
                            .inventory
                            .take_ready()
                            .expect("dispatch requires a ready database");

                        self.assign_database(&lease, database)
                    }
                };

                self.reply_attached(&lease, database, reply);
            }

            if !already_assigned {
                self.restore_unclaimed_database(&lease);
            }
        }
    }

    fn reply_attached(&mut self, lease: &LeaseId, database: Database, reply: RequestId) {
        let entry = self.leases.get_mut(lease).expect("reply requires an assigned lease");
        let Some(conns) = entry.conns.checked_add(1) else {
            let _ = self.engine_io.reply(reply, ConsumerReply::FailedToAttach);
            return;
        };
        entry.conns = conns;
        if self
            .engine_io
            .reply(
                reply,
                ConsumerReply::Attached {
                    database_id: database.database_id,
                    target: std::sync::Arc::new(database.resource.target),
                    key: LeaseKey { lease: lease.clone(), generation: entry.generation },
                },
            )
            .is_err()
        {
            entry.conns -= 1;
        }
    }

    fn restore_unclaimed_database(&mut self, lease: &LeaseId) -> bool {
        if self.leases.get(lease).is_none_or(|entry| entry.conns != 0) {
            return false;
        }
        let entry = self.leases.remove(lease).unwrap();
        self.engine_io
            .cancel_sessions(&LeaseKey { lease: lease.clone(), generation: entry.generation });
        self.inventory.return_ready(entry.database);

        true
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn assign_database(&mut self, lease: &LeaseId, database: Database) -> Database {
        debug_assert!(!self.leases.contains_key(lease));

        let assigned = database.clone();

        self.next_generation =
            self.next_generation.checked_add(1).expect("lease generation exhausted");

        let generation = self.next_generation;
        self.leases.insert(lease.clone(), LeaseEntry { database, conns: 0, generation });
        if self.config.lease_claim_timeout_ms > 0 {
            self.engine_io.schedule(
                LeaseKey { lease: lease.clone(), generation },
                Tick(self.now.0 + Duration::from_millis(self.config.lease_claim_timeout_ms)),
                EngineMessage::LeaseMaxTimeReached { lease: lease.clone(), generation },
            );
        }

        assigned
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
                            || self.now.elapsed_since(*queued_at).as_millis()
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

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn retire_lease(&mut self, lease: &LeaseId) {
        let Some(entry) = self.leases.remove(lease) else {
            return;
        };

        self.engine_io
            .cancel_sessions(&LeaseKey { lease: lease.clone(), generation: entry.generation });

        let request = self.inventory.retire(entry.database);
        let database_id = request.database_id;

        if let Err(error) = self.engine_io.request_cleanup(request) {
            tracing::error!(?database_id, %lease, %error, "unable to enqueue cleanup; retaining retirement record")
        }

        self.grow();
    }

    pub fn snapshot(&self) -> &Self {
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
