use std::sync::Arc;

use hotpath::wrap::tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use pgtest_database_operations::manager::PostgresManager;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    worker_engine::{
        database_jobs::{CreateDatabases, DatabaseWorkerMessages},
        messages::EngineMessage,
        traits::PostgresClient,
    },
    worker_manager::ConsumerWorker,
};

pub(super) struct DatabaseCreationWorker<P = PostgresManager> {
    inbox_rx: UnboundedReceiver<CreateDatabases>,
    engine_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
    postgres_manager: Arc<P>,
}

impl<P: PostgresClient + Send + Sync + 'static> DatabaseCreationWorker<P> {
    pub(super) fn new(
        engine_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
        tracker: TaskTracker,
        cancel_token: CancellationToken,
        postgres_manager: Arc<P>,
        inbox_rx: UnboundedReceiver<CreateDatabases>,
    ) -> Self {
        Self { engine_tx, tracker, shutdown: cancel_token, postgres_manager, inbox_rx }
    }

    pub(super) async fn run(mut self) {
        loop {
            let request = tokio::select! {
                biased;

                _ = self.shutdown.cancelled() => break,

                request = self.inbox_rx.recv() => {
                    let Some(message) = request else {
                        break;
                    };
                    message
                }
            };

            let postgres_manager = self.postgres_manager.clone();
            let engine_tx = self.engine_tx.clone();
            let shutdown = self.shutdown.clone();

            self.tracker.spawn(async move {
                let on_finished =
                    |index, result| {
                        if shutdown.is_cancelled() {
                            return;
                        }
                        let database_id = request.database_id(index);
                        let message = EngineMessage::DatabaseWorker(
                            DatabaseWorkerMessages::CreationFinished { database_id, result },
                        );
                        if engine_tx.send(message).is_err() {
                            tracing::warn!(
                                ?database_id,
                                "unable to deliver creation result: engine inbox closed"
                            );
                        }
                    };
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {},
                    () = postgres_manager.create_databases(request.amount.get(), on_finished) => {},
                }
            });
        }
    }
}
