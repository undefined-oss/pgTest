//! Manager state and synchronous message handling, with its Tokio adapter and
//! receive loop.

use std::{collections::VecDeque, time::Duration};

use pgtest_engine_backend::jobs::{
    CleanupDatabase, CreateDatabases, CreationResult, DatabaseWorkerMessages,
};
use rustc_hash::FxHashMap;

use super::{
    database_inventory::{Database, DatabaseInventory},
    errors::{AttachError, IOError},
    lease::{LeaseEntry, LeaseId, LeaseKey, SharedLeases},
    messages::{ConsumerReply, ElapsedTime, ManagerMessage},
};
use crate::config::ManagerConfig;

/// Concrete worker messaging, replies, and time effects supplied by Tokio or
/// the simulator.
pub trait ManagerIO {
    type ReplyHandle;

    fn request_creation(&mut self, request: CreateDatabases) -> Result<(), IOError>;
    fn request_cleanup(&mut self, request: CleanupDatabase) -> Result<(), IOError>;
    fn reply(
        &mut self,
        request: Self::ReplyHandle,
        reply: ConsumerReply,
    ) -> Result<(), ConsumerReply>;
    fn schedule(
        &mut self,
        key: LeaseKey,
        deadline: ElapsedTime,
        message: ManagerMessage<Self::ReplyHandle>,
    );
    fn cancel_sessions(&mut self, key: &LeaseKey);
}

pub struct ManagerWorker<IO: ManagerIO> {
    pub(crate) leases: SharedLeases,
    next_generation: u64,
    config: ManagerConfig,
    pub(crate) waiters: VecDeque<LeaseId>,
    group_waiters: FxHashMap<LeaseId, Vec<(IO::ReplyHandle, ElapsedTime)>>,
    io: IO,
    now: ElapsedTime,
    pub inventory: DatabaseInventory,
}

impl<IO: ManagerIO> ManagerWorker<IO> {
    pub fn new(config: ManagerConfig, inventory: DatabaseInventory, io: IO) -> Self {
        Self {
            leases: SharedLeases::default(),
            next_generation: 0,
            config,
            waiters: VecDeque::new(),
            group_waiters: FxHashMap::default(),
            io,
            now: ElapsedTime::default(),
            inventory,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn leases(&self) -> FxHashMap<LeaseId, LeaseEntry> {
        self.leases.pin().iter().map(|(lease, entry)| (lease.clone(), entry.clone())).collect()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn waiters(&self) -> &VecDeque<LeaseId> {
        &self.waiters
    }

    pub fn shutdown(&mut self) {
        for (lease, entry) in self.leases.pin().iter() {
            self.io
                .cancel_sessions(&LeaseKey { lease: lease.clone(), generation: entry.generation });
        }
        for (_, replies) in self.group_waiters.drain() {
            for (reply, _) in replies {
                let _ = self
                    .io
                    .reply(reply, ConsumerReply::AttachRejected(AttachError::EngineUnavailable));
            }
        }
        self.waiters.clear();
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    pub fn handle(&mut self, msg: ManagerMessage<IO::ReplyHandle>, now: ElapsedTime) {
        self.now = now;
        match msg {
            ManagerMessage::AttachOrJoin { lease, reply, message_time } => {
                self.attach_or_join(lease, reply, message_time);
            }
            ManagerMessage::ReleaseLease { lease, reply } => {
                self.retire_lease(&lease);
                let _ = self.io.reply(reply, ConsumerReply::ReleaseResult(Ok(())));
            }
            ManagerMessage::LeaseMaxTimeReached { lease, generation } => {
                let current = self.leases.pin().get(&lease).map(|entry| entry.generation);
                if current == Some(generation) {
                    self.retire_lease(&lease);
                }
            }
            ManagerMessage::DatabaseWorker(message) => {
                self.handle_database_worker_message(message);
            }
            ManagerMessage::Shutdown => {
                self.shutdown();
            }
        }
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn handle_database_worker_message(&mut self, message: DatabaseWorkerMessages) {
        match message {
            DatabaseWorkerMessages::CreationsFinished { results } => {
                self.handle_creation_results(results);
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

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn handle_creation_results(&mut self, results: Vec<CreationResult>) {
        if self.settle_creation_results(results) {
            self.dispatch_waiters();
            self.grow();
        }
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn settle_creation_results(&mut self, results: Vec<CreationResult>) -> bool {
        let mut settled = false;
        for result in results {
            let database_id = result.database_id;
            let Some(result) = self.inventory.complete_creation(database_id, result.result) else {
                tracing::warn!(?database_id, "creation result without a pending reservation");
                continue;
            };
            settled = true;
            if let Err(error) = result {
                tracing::error!(?database_id, %error, "database creation failed");
            }
        }
        settled
    }

    fn attach_or_join(
        &mut self,
        lease: LeaseId,
        reply: IO::ReplyHandle,
        message_time: ElapsedTime,
    ) {
        let database = self.leases.pin().get(&lease).map(|entry| entry.database.clone());
        if let Some(database) = database {
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
        if !self.reply_attached(&lease, database, reply) {
            self.restore_unclaimed_database(&lease);
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

            let mut claimed = self.leases.pin().contains_key(&lease);

            for (reply, message_time) in replies {
                if self.config.lease_claim_timeout_ms > 0
                    && self.now.elapsed_since(message_time).as_millis()
                        > self.config.lease_claim_timeout_ms
                {
                    let _ =
                        self.io.reply(reply, ConsumerReply::AttachRejected(AttachError::TimedOut));
                    continue;
                }

                let database = self.leases.pin().get(&lease).map(|entry| entry.database.clone());
                let database = match database {
                    Some(database) => database,
                    None => {
                        let database = self
                            .inventory
                            .take_ready()
                            .expect("dispatch requires a ready database");

                        self.assign_database(&lease, database)
                    }
                };

                claimed |= self.reply_attached(&lease, database, reply);
            }

            if !claimed {
                self.restore_unclaimed_database(&lease);
            }
        }
    }

    fn reply_attached(
        &mut self,
        lease: &LeaseId,
        database: Database,
        reply: IO::ReplyHandle,
    ) -> bool {
        let generation =
            self.leases.pin().get(lease).expect("reply requires an assigned lease").generation;
        self.io
            .reply(
                reply,
                ConsumerReply::Attached {
                    database_id: database.database_id,
                    target: std::sync::Arc::new(database.resource.target),
                    key: LeaseKey { lease: lease.clone(), generation },
                },
            )
            .is_ok()
    }

    fn restore_unclaimed_database(&mut self, lease: &LeaseId) {
        let Some(entry) = self.leases.pin().remove(lease).cloned() else {
            return;
        };
        self.io.cancel_sessions(&LeaseKey { lease: lease.clone(), generation: entry.generation });
        self.inventory.return_ready(entry.database);
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn assign_database(&mut self, lease: &LeaseId, database: Database) -> Database {
        debug_assert!(!self.leases.pin().contains_key(lease));

        let assigned = database.clone();

        self.next_generation =
            self.next_generation.checked_add(1).expect("lease generation exhausted");

        let generation = self.next_generation;
        self.leases.pin().insert(
            lease.clone(),
            LeaseEntry {
                database,
                generation,
                #[cfg(feature = "tokio-runtime")]
                session: std::sync::OnceLock::new(),
            },
        );
        if self.config.lease_claim_timeout_ms > 0 {
            self.io.schedule(
                LeaseKey { lease: lease.clone(), generation },
                ElapsedTime(
                    self.now.0
                        + Duration::from_nanos_u128(
                            self.config
                                .lease_claim_timeout_ms
                                .checked_mul(1_000_000)
                                .expect("lease timeout is too large"),
                        ),
                ),
                ManagerMessage::LeaseMaxTimeReached { lease: lease.clone(), generation },
            );
        }

        assigned
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    pub(crate) fn grow(&mut self) {
        let batch_size = usize::from(*self.config.grow_batch_size);
        if batch_size == 0 {
            return;
        }

        let waiting = self
            .group_waiters
            .iter()
            .filter(|(lease, replies)| {
                !self.leases.pin().contains_key(*lease)
                    && replies.iter().any(|(_, queued_at)| {
                        self.config.lease_claim_timeout_ms == 0
                            || self.now.elapsed_since(*queued_at).as_millis()
                                <= self.config.lease_claim_timeout_ms
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
        if let Err(error) = self.io.request_creation(request) {
            for index in 0..request.amount.get() {
                self.inventory.cancel_creation(request.database_id(index));
            }
            tracing::error!(?request, %error, "unable to enqueue database creation batch");
        }
    }

    #[cfg_attr(feature = "tokio-runtime", hotpath::measure)]
    fn retire_lease(&mut self, lease: &LeaseId) {
        let Some(entry) = self.leases.pin().remove(lease).cloned() else {
            return;
        };

        self.io.cancel_sessions(&LeaseKey { lease: lease.clone(), generation: entry.generation });

        let request = self.inventory.retire(entry.database);
        let database_id = request.database_id;

        if let Err(error) = self.io.request_cleanup(request) {
            tracing::error!(?database_id, %lease, %error, "unable to enqueue cleanup; retaining retirement record")
        }

        self.grow();
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn snapshot(&self) -> &Self {
        self
    }
}

#[cfg(feature = "tokio-runtime")]
pub(crate) use tokio_io::TokioPorts;

#[cfg(feature = "tokio-runtime")]
mod tokio_io {
    use std::collections::HashMap;

    use hotpath::wrap::tokio::sync::mpsc;
    use pgtest_database_operations::{
        cleanup_worker_handle::CleanupHandle, creation_worker_handle::CreationHandle,
    };
    use tokio::sync::oneshot;
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    use super::*;
    use crate::manager_handle::{LeaseSession, Reply, TokioMessage};
    pub(crate) struct TokioPorts {
        pub(crate) leases: SharedLeases,
        pub(crate) creation: CreationHandle,
        pub(crate) cleanup: CleanupHandle,
        pub(crate) cancellations: HashMap<LeaseKey, CancellationToken>,
        pub(crate) commands: mpsc::UnboundedSender<TokioMessage>,
        pub(crate) epoch: tokio::time::Instant,
        pub(crate) shutdown: CancellationToken,
        pub(crate) tracker: TaskTracker,
    }
    impl ManagerIO for TokioPorts {
        type ReplyHandle = oneshot::Sender<Reply>;

        fn request_creation(&mut self, request: CreateDatabases) -> Result<(), IOError> {
            self.creation.create(request).map_err(|_| IOError::FailedToSendTheMessage)
        }

        fn request_cleanup(&mut self, request: CleanupDatabase) -> Result<(), IOError> {
            self.cleanup.delete(request).map_err(|_| IOError::FailedToSendTheMessage)
        }

        fn reply(
            &mut self,
            sender: Self::ReplyHandle,
            reply: ConsumerReply,
        ) -> Result<(), ConsumerReply> {
            match &reply {
                ConsumerReply::Attached { database_id, target, key } => {
                    let cancellation = self
                        .cancellations
                        .entry(key.clone())
                        .or_insert_with(|| self.shutdown.child_token())
                        .clone();
                    let session = LeaseSession {
                        database_id: *database_id,
                        target: target.clone(),
                        cancellation,
                    };
                    if sender.send(Reply::Attached(session.clone())).is_err() {
                        return Err(reply);
                    }
                    // The worker is the only assignment writer. Publish after
                    // delivery succeeds so readers cannot claim a database
                    // that a failed first reply would return to the ready pool.
                    let leases = self.leases.pin();
                    let entry = leases.get(&key.lease).expect("reply requires an assigned lease");
                    debug_assert_eq!(entry.generation, key.generation);
                    let _ = entry.session.set(session);
                    Ok(())
                }
                _ => sender.send(Reply::Engine(reply.clone())).map_err(|_| reply),
            }
        }

        fn schedule(&mut self, key: LeaseKey, deadline: ElapsedTime, message: TokioMessage) {
            let cancel = self
                .cancellations
                .entry(key)
                .or_insert_with(|| self.shutdown.child_token())
                .clone();
            let commands = self.commands.clone();
            let until = self.epoch + deadline.0;
            self.tracker.spawn(async move {
                tokio::select! { biased;
                    _ = cancel.cancelled() => {},
                    _ = tokio::time::sleep_until(until) => {
                        let _ = commands.send(message);
                    }
                }
            });
        }

        fn cancel_sessions(&mut self, key: &LeaseKey) {
            if let Some(token) = self.cancellations.remove(key) {
                token.cancel();
            }
        }
    }

    impl ManagerWorker<TokioPorts> {
        pub(crate) async fn run(
            mut self,
            mut commands: mpsc::UnboundedReceiver<TokioMessage>,
            epoch: tokio::time::Instant,
        ) {
            while let Some(message) = commands.recv().await {
                if matches!(message, ManagerMessage::Shutdown) {
                    break;
                }
                self.handle(message, ElapsedTime(epoch.elapsed()));
            }
            self.shutdown();
        }
    }
}
