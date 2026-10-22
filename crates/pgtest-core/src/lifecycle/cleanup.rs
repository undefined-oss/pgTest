//! The only registry remover; owns expiry and physical retirement records.
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use hotpath::wrap::tokio::sync::mpsc;
use pgtest_engine_backend::{BackendError, DatabaseCleaner, ResourceId, jobs::DatabaseId};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    Cleanup, StartError,
    registry::{Entry, Remover},
};

/// Owns lease removal and database cleanup on the runtime polling `run`.
pub struct CleanupWorker {
    registry: Remover,
    inbox: mpsc::UnboundedReceiver<Cleanup>,
    stop: CancellationToken,
    // Failed deletions stay here, with their provider identity. No automatic retry.
    retired: HashMap<DatabaseId, ResourceId>,
}
impl CleanupWorker {
    pub(super) fn new(
        registry: Remover,
        inbox: mpsc::UnboundedReceiver<Cleanup>,
        stop: CancellationToken,
    ) -> Self {
        Self { registry, inbox, stop, retired: HashMap::new() }
    }

    /// Initialize the client on the host runtime, report readiness, then serve
    /// cleanup requests. Dropping the readiness receiver stops both workers.
    pub async fn run<C, Fut>(
        self,
        initialize: Fut,
        limit: NonZeroUsize,
        ready: oneshot::Sender<Result<(), StartError>>,
    ) where
        C: DatabaseCleaner,
        Fut: Future<Output = Result<Arc<C>, StartError>>,
    {
        let _stop_on_exit = self.stop.clone().drop_guard();
        let client = tokio::select! {
            biased;
            _ = self.stop.cancelled() => return,
            client = initialize => client,
        };
        match client {
            Ok(client) => {
                if ready.send(Ok(())).is_ok() {
                    self.run_initialized(client, limit).await;
                }
            }
            Err(error) => {
                let _ = ready.send(Err(error));
            }
        }
    }

    pub(crate) async fn run_initialized<C: DatabaseCleaner>(
        mut self,
        client: Arc<C>,
        limit: NonZeroUsize,
    ) {
        let mut queued = VecDeque::new();
        let mut active = FuturesUnordered::new();
        let mut timers = FuturesUnordered::new();
        loop {
            if self.stop.is_cancelled() {
                return;
            }
            while active.len() < limit.get() {
                let Some((id, resource)) = queued.pop_front() else {
                    break;
                };
                let client = client.clone();
                active.push(async move { (id, client.delete_database(resource).await) });
            }
            tokio::select! {
                _ = self.stop.cancelled() => return,
                result = active.next(), if !active.is_empty() => {
                    let (id, result) = result.unwrap();
                    self.completed(id, result);
                },
                expired = timers.next(), if !timers.is_empty() => {
                    if let Some(entry) = expired.unwrap() { self.close(entry, &mut queued); }
                },
                command = self.inbox.recv() => {
                    match command {
                        Some(Cleanup::Release { entry, reply }) => {
                            self.close(entry, &mut queued);
                            if let Some(reply) = reply { let _ = reply.send(()); }
                        }
                        Some(Cleanup::Expire { entry, at }) => {
                            let cancellation = entry.cancellation();
                            timers.push(async move {
                                tokio::select! {
                                    biased;
                                    _ = cancellation.cancelled() => None,
                                    _ = tokio::time::sleep_until(at) => Some(entry),
                                }
                            });
                        }
                        None => return,
                    }
                },
            }
        }
    }

    #[hotpath::measure]
    fn completed(&mut self, id: DatabaseId, result: Result<(), BackendError>) {
        match result {
            Ok(()) => {
                self.retired.remove(&id);
            }
            Err(error) => {
                tracing::error!(?id, resource = ?self.retired.get(&id), %error, "database cleanup failed; retaining retirement record")
            }
        }
    }

    #[hotpath::measure]
    fn close(&mut self, entry: Arc<Entry>, queued: &mut VecDeque<(DatabaseId, ResourceId)>) {
        if let Some(database) = self.registry.close(&entry) {
            let id = database.database_id;
            let resource = database.resource.resource_id;
            self.retired.insert(id, resource.clone());
            queued.push_back((id, resource));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_retirement_keeps_identity_when_other_deletions_succeed() {
        let (_, _, registry) = crate::lifecycle::registry::split();
        let (_, inbox) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
        let mut cleanup = CleanupWorker::new(registry, inbox, CancellationToken::new());
        cleanup.retired.insert(DatabaseId(1), ResourceId("failed".into()));
        cleanup.retired.insert(DatabaseId(2), ResourceId("removed".into()));
        cleanup.completed(DatabaseId(1), Err(BackendError::OperationFailed("failure".into())));
        cleanup.completed(DatabaseId(2), Ok(()));
        cleanup.completed(DatabaseId(999), Ok(()));
        assert_eq!(cleanup.retired.len(), 1);
        assert_eq!(cleanup.retired.get(&DatabaseId(1)), Some(&ResourceId("failed".into())));
    }
}
