use std::{num::NonZeroUsize, sync::Arc};

use futures_util::{StreamExt, stream::FuturesUnordered};
use pgtest_engine_backend::AsyncDatabaseBackend;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::RuntimeError;
use crate::worker_engine::{
    database_jobs::{CleanupDatabase, CreateDatabases},
    messages::EngineMessage,
    workers::{CleanupAction, CleanupState, CreationAction, CreationState},
};

pub(super) async fn run_creation<B: AsyncDatabaseBackend>(
    backend: Arc<B>,
    limit: NonZeroUsize,
    mut inbox: mpsc::UnboundedReceiver<CreateDatabases>,
    events: mpsc::UnboundedSender<EngineMessage>,
    shutdown: CancellationToken,
) -> Result<(), RuntimeError> {
    let mut state = CreationState::new(limit);
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
                CreationAction::Begin(id) => {
                    let backend = backend.clone();
                    active.push(async move { (id, backend.create_database().await) });
                }
                CreationAction::Report(event) => events
                    .send(EngineMessage::DatabaseWorker(event))
                    .map_err(|_| RuntimeError("manager completion mailbox closed".into()))?,
            }
        }
        if !open && state.is_idle() {
            break;
        }
    }
    Ok(())
}

pub(super) async fn run_cleanup<B: AsyncDatabaseBackend>(
    backend: Arc<B>,
    limit: NonZeroUsize,
    mut inbox: mpsc::UnboundedReceiver<CleanupDatabase>,
    events: mpsc::UnboundedSender<EngineMessage>,
    shutdown: CancellationToken,
) -> Result<(), RuntimeError> {
    let mut state = CleanupState::new(limit);
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
                        (request.database_id, backend.delete_database(request.resource_id).await)
                    });
                }
                CleanupAction::Report(event) => {
                    events
                        .send(EngineMessage::DatabaseWorker(event))
                        .map_err(|_| RuntimeError("manager completion mailbox closed".into()))?
                }
            }
        }
        if !open && state.is_idle() {
            break;
        }
    }
    Ok(())
}
