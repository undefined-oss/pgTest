use std::{sync::Arc, time::Duration};

use pgtest_engine_backend::{
    PgTarget,
    jobs::{DatabaseId, DatabaseWorkerMessages},
};

use super::{
    errors::{AttachError, ReleaseError},
    lease::{LeaseId, LeaseKey},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ElapsedTime(pub Duration);
impl ElapsedTime {
    pub fn elapsed_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}
#[derive(Clone, Debug)]
pub enum ManagerMessage<ReplyHandle> {
    AttachOrJoin { lease: LeaseId, reply: ReplyHandle, message_time: ElapsedTime },
    ReleaseLease { lease: LeaseId, reply: ReplyHandle },
    LeaseMaxTimeReached { lease: LeaseId, generation: u64 },
    DatabaseWorker(DatabaseWorkerMessages),
    Shutdown,
}

impl<ReplyHandle> From<DatabaseWorkerMessages> for ManagerMessage<ReplyHandle> {
    fn from(message: DatabaseWorkerMessages) -> Self {
        Self::DatabaseWorker(message)
    }
}
#[derive(Clone, Debug)]
pub enum ConsumerReply {
    Attached { database_id: DatabaseId, target: Arc<PgTarget>, key: LeaseKey },
    AttachRejected(AttachError),
    ReleaseResult(Result<(), ReleaseError>),
}
