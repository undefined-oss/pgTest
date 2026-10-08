use std::{sync::Arc, time::Duration};

use pgtest_engine_backend::PgTarget;

use super::{
    core::LeaseId,
    database_jobs::{DatabaseId, DatabaseWorkerMessages},
    errors::{AttachError, ReleaseError},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tick(pub Duration);
impl Tick {
    pub fn elapsed_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub u64);
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseKey {
    pub lease: LeaseId,
    pub generation: u64,
}

#[derive(Clone, Debug)]
pub enum EngineMessage {
    AttachOrJoin { template: String, lease: LeaseId, reply: RequestId, message_time: Tick },
    ReleaseLease { lease: LeaseId, reply: RequestId },
    Detach { lease: LeaseId, generation: u64 },
    LeaseMaxTimeReached { lease: LeaseId, generation: u64 },
    DatabaseWorker(DatabaseWorkerMessages),
    Shutdown,
}
#[derive(Clone, Debug)]
pub enum ConsumerReply {
    Attached { database_id: DatabaseId, target: Arc<PgTarget>, key: LeaseKey },
    FailedToAttach,
    AttachRejected(AttachError),
    ReleaseResult(Result<(), ReleaseError>),
}
