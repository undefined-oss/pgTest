//! Cleanup worker receive loop and operation scheduling.
use std::sync::Arc;

use futures_util::{StreamExt, stream::FuturesUnordered};
use pgtest_engine_backend::{
    DatabaseCleaner,
    jobs::{CleanupDatabase, DatabaseWorkerMessages},
    workers::{CleanupAction, CleanupState},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(super) struct CleanupActor<DatabaseClient, ManagerMessage> {
    pub(super) client: Arc<DatabaseClient>,
    pub(super) state: CleanupState,
    pub(super) receiver: mpsc::UnboundedReceiver<CleanupDatabase>,
    pub(super) manager: mpsc::UnboundedSender<ManagerMessage>,
    pub(super) shutdown: CancellationToken,
}
impl<DatabaseClient, ManagerMessage> CleanupActor<DatabaseClient, ManagerMessage>
where
    DatabaseClient: DatabaseCleaner,
    ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static,
{
    pub(super) async fn run(self) {
        let Self { client: backend, mut state, receiver: mut inbox, manager, shutdown } = self;
        let mut active = FuturesUnordered::new();
        let mut open = true;
        loop {
            let actions = tokio::select! { biased;
                _ = shutdown.cancelled() => break,
                completion = active.next(), if !active.is_empty() => {
                    let (id, result) = completion.expect("active future"); state.complete(id, result)
                },
                request = inbox.recv(), if open => {
                    match request { Some(request) => state.enqueue(request), None => { open = false; Vec::new() } }
                },
                else => break,
            };
            for action in actions {
                match action {
                    CleanupAction::Begin(request) => {
                        let backend = backend.clone();
                        active.push(async move {
                            (
                                request.database_id,
                                backend.delete_database(request.resource_id).await,
                            )
                        });
                    }
                    CleanupAction::Report(event) => {
                        if manager.send(event.into()).is_err() {
                            tracing::warn!(
                                "unable to deliver cleanup result: manager inbox closed"
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
