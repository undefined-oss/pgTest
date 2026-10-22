use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{LeaseId, LeaseSession, errors::AttachError, inventory::Database};

type Entries = Arc<papaya::HashMap<LeaseId, Arc<Entry>>>;

#[derive(Clone)]
pub(super) struct Reader(Entries);
pub(super) struct Publisher(Entries);
pub(super) struct Remover(Entries);

pub(super) fn split() -> (Reader, Publisher, Remover) {
    let entries = Arc::new(papaya::HashMap::new());
    (Reader(entries.clone()), Publisher(entries.clone()), Remover(entries))
}
impl Reader {
    pub(super) fn get(&self, lease: &LeaseId) -> Option<Arc<Entry>> {
        self.0.pin().get(lease).cloned()
    }
}
impl Publisher {
    pub(super) fn get(&self, lease: &LeaseId) -> Option<Arc<Entry>> {
        self.0.pin().get(lease).cloned()
    }

    /// Only creation publishes, after assigning a fully provisioned database.
    /// At least one waiter has accepted the assignment before publication.
    pub(super) fn publish(&self, entry: Entry) -> Arc<Entry> {
        let entry = Arc::new(entry);
        debug_assert!(self.0.pin().get(&entry.lease).is_none());
        self.0.pin().insert(entry.lease.clone(), entry.clone());
        entry
    }
}
impl Remover {
    /// Only cleanup removes. A delayed release or timer cannot remove a newer
    /// assignment using the same lease ID or retire a database twice.
    pub(super) fn close(&self, entry: &Arc<Entry>) -> Option<Database> {
        let guard = self.0.pin();
        let (_, removed) =
            guard.remove_if(&entry.lease, |_, current| Arc::ptr_eq(current, entry)).ok()??;
        removed.session.cancellation.cancel();
        Some(removed.database.clone())
    }
}

/// A published assignment. Pending requests and waiters never enter Papaya.
pub(crate) struct Entry {
    lease: LeaseId,
    database: Database,
    session: LeaseSession,
}
impl Entry {
    pub(super) fn new(lease: LeaseId, database: Database, stop: &CancellationToken) -> Self {
        let session = LeaseSession {
            database_id: database.database_id,
            target: Arc::new(database.resource.target.clone()),
            cancellation: stop.child_token(),
        };
        Self { lease, database, session }
    }

    pub(super) fn into_database(self) -> Database {
        self.database
    }

    pub(super) fn ready(&self) -> Result<LeaseSession, AttachError> {
        if self.session.cancellation.is_cancelled() {
            Err(AttachError::LeaseClosed)
        } else {
            Ok(self.session.clone())
        }
    }

    pub(super) fn cancellation(&self) -> CancellationToken {
        self.session.cancellation.clone()
    }
}
