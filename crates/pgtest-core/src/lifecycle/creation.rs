//! The only registry publisher and the owner of the shared ready inventory.
use std::{collections::VecDeque, future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use futures_util::{
    StreamExt,
    stream::{self, FuturesUnordered},
};
use hotpath::wrap::tokio::sync::mpsc;
use pgtest_engine_backend::{
    DatabaseCreator,
    jobs::{CreateDatabases, CreationResult},
};
use rustc_hash::FxHashMap;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    Cleanup, CreationRequest, StartError,
    registry::{Entry, Publisher},
};
use crate::{
    LeaseId, LeaseSession, config::PoolConfig, errors::AttachError, inventory::DatabaseInventory,
};

/// Owns database creation and lease publication on the runtime polling `run`.
pub struct CreationWorker {
    registry: Publisher,
    inbox: mpsc::UnboundedReceiver<CreationRequest>,
    cleanup: mpsc::UnboundedSender<Cleanup>,
    config: PoolConfig,
    lease_ttl: Duration,
    stop: CancellationToken,
    inventory: DatabaseInventory,
    pending: FxHashMap<LeaseId, Vec<oneshot::Sender<Result<LeaseSession, AttachError>>>>,
    waiting: VecDeque<LeaseId>,
}

impl CreationWorker {
    pub(super) fn new(
        registry: Publisher,
        inbox: mpsc::UnboundedReceiver<CreationRequest>,
        cleanup: mpsc::UnboundedSender<Cleanup>,
        config: PoolConfig,
        inventory: DatabaseInventory,
        lease_ttl: Duration,
        stop: CancellationToken,
    ) -> Self {
        Self {
            registry,
            inbox,
            cleanup,
            config,
            lease_ttl,
            stop,
            inventory,
            pending: Default::default(),
            waiting: Default::default(),
        }
    }

    /// Initialize the client on the host runtime, then serve creation requests.
    /// Bootstrap has already supplied the initial inventory. Dropping the
    /// readiness receiver during startup stops both workers.
    pub async fn run<C, Fut>(
        self,
        initialize: Fut,
        limit: NonZeroUsize,
        ready: oneshot::Sender<Result<(), StartError>>,
    ) where
        C: DatabaseCreator,
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

    pub(crate) async fn run_initialized<C: DatabaseCreator>(
        mut self,
        client: Arc<C>,
        limit: NonZeroUsize,
    ) {
        // Initial size is exact: growth begins with the first request.
        let mut queued = VecDeque::new();
        let mut active = FuturesUnordered::new();
        loop {
            if self.stop.is_cancelled() {
                return;
            }
            // Run one complete batch at a time. Each batch limits its own
            // concurrent database operations.
            if active.is_empty() {
                if let Some(batch) = queued.pop_front() {
                    active.push(Self::create_batch(client.as_ref(), batch, limit));
                }
            }
            let replenish = tokio::select! {
                _ = self.stop.cancelled() => return,
                request = self.inbox.recv() => {
                    let Some(request) = request else { return; };
                    self.handle_request(request)
                },
                result = active.next(), if !active.is_empty() => {
                    self.record_results(result.unwrap());
                    true
                },
            };
            self.dispatch();
            if replenish {
                if let Some(batch) = self.grow() {
                    queued.push_back(batch);
                }
            }
        }
    }

    #[hotpath::measure(future = true)]
    async fn create_batch<C: DatabaseCreator>(
        client: &C,
        batch: CreateDatabases,
        limit: NonZeroUsize,
    ) -> Vec<CreationResult> {
        stream::iter(0..batch.amount.get())
            .map(|index| async move {
                CreationResult {
                    database_id: batch.database_id(index),
                    result: client.create_database().await,
                }
            })
            .buffer_unordered(limit.get())
            .collect()
            .await
    }

    #[hotpath::measure]
    fn record_results(&mut self, results: Vec<CreationResult>) {
        for CreationResult { database_id, result } in results {
            if let Some(Err(error)) = self.inventory.complete_creation(database_id, result) {
                tracing::error!(?database_id, %error, "database creation failed");
            }
        }
    }

    #[hotpath::measure]
    fn handle_request(&mut self, request: CreationRequest) -> bool {
        match request {
            CreationRequest::Attach { lease, reply } => {
                if reply.is_closed() {
                    return false;
                }
                // The assignment may have been published since the caller's
                // lookup. Do not allocate another database for the same lease.
                if let Some(entry) = self.registry.get(&lease) {
                    let _ = reply.send(entry.ready());
                    return false;
                }
                let waiters = self.pending.entry(lease.clone()).or_insert_with(|| {
                    self.waiting.push_back(lease);
                    Vec::new()
                });
                waiters.push(reply);
                true
            }
            CreationRequest::Cancel { lease } => {
                if let Some(waiters) = self.pending.get_mut(&lease) {
                    waiters.retain(|reply| !reply.is_closed());
                }
                false
            }
            CreationRequest::Release { lease, reply } => {
                // A release that observed no assignment can arrive after
                // publication. Cleanup remains the only registry remover.
                if let Some(entry) = self.registry.get(&lease) {
                    let _ = self.cleanup.send(Cleanup::Release { entry, reply: Some(reply) });
                } else {
                    if let Some(waiters) = self.pending.remove(&lease) {
                        self.waiting.retain(|waiting| waiting != &lease);
                        for waiter in waiters {
                            let _ = waiter.send(Err(AttachError::LeaseClosed));
                        }
                    }
                    let _ = reply.send(());
                }
                false
            }
        }
    }

    #[hotpath::measure]
    fn dispatch(&mut self) {
        for _ in 0..self.waiting.len() {
            let lease = self.waiting.pop_front().unwrap();
            let waiters = self.pending.get_mut(&lease).expect("queued lease has pending waiters");
            if waiters.is_empty() {
                self.pending.remove(&lease);
                continue;
            }
            let Some(database) = self.inventory.take_ready() else {
                self.waiting.push_back(lease);
                continue;
            };
            let entry = Entry::new(lease.clone(), database, &self.stop);
            let mut notified = false;
            for reply in waiters.drain(..) {
                notified |= reply.send(entry.ready()).is_ok();
            }
            // A successful send commits the assignment. Requests that miss
            // publication queue behind this synchronous dispatch and recheck.
            if notified {
                let entry = self.registry.publish(entry);
                if !self.lease_ttl.is_zero() {
                    let _ = self.cleanup.send(Cleanup::Expire {
                        entry,
                        at: tokio::time::Instant::now() + self.lease_ttl,
                    });
                }
            } else {
                self.inventory.return_ready(entry.into_database());
            }
            self.pending.remove(&lease);
        }
    }

    #[hotpath::measure]
    fn grow(&mut self) -> Option<CreateDatabases> {
        let batch = usize::from(*self.config.grow_batch_size);
        if batch == 0 {
            return None;
        }
        // Each pending lease needs one database, regardless of its waiter
        // count.
        let required = self.pending.len() + usize::from(*self.config.starvation_threshold) + 1;
        let deficit = required.saturating_sub(self.inventory.supply_len());
        self.inventory.reserve_creations(deficit.div_ceil(batch) * batch)
    }
}

#[cfg(test)]
mod tests {
    use pgtest_engine_backend::{
        PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId, jobs::DatabaseId,
    };

    use super::*;
    use crate::lifecycle::registry;

    #[tokio::test]
    async fn cancelled_receivers_only_consume_inventory_when_another_send_succeeds() {
        for live_waiters in [0, 2] {
            let (reader, publisher, _remover) = registry::split();
            let (_requests, inbox) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
            let (cleanup, mut cleanup_requests) =
                hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
            let inventory = DatabaseInventory::from_initial(vec![ProvisionedDatabase {
                resource_id: ResourceId("only".into()),
                target: PgTarget {
                    database: "only".into(),
                    endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
                },
            }]);
            let mut worker = CreationWorker::new(
                publisher,
                inbox,
                cleanup,
                PoolConfig::default(),
                inventory,
                Duration::from_secs(30),
                CancellationToken::new(),
            );
            let lease = LeaseId::new("cancelled-delivery").unwrap();
            let (reply, cancelled) = oneshot::channel();
            worker.handle_request(CreationRequest::Attach { lease: lease.clone(), reply });
            let mut receivers = Vec::new();
            for _ in 0..live_waiters {
                let (reply, receiver) = oneshot::channel();
                worker.handle_request(CreationRequest::Attach { lease: lease.clone(), reply });
                receivers.push(receiver);
            }
            // Cancel after admission, without processing a cancellation
            // message. dispatch must handle send failure, not rely
            // on an earlier check.
            drop(cancelled);
            worker.dispatch();
            assert!(worker.pending.is_empty());
            assert!(worker.waiting.is_empty());
            if live_waiters == 0 {
                assert!(reader.get(&lease).is_none());
                assert!(cleanup_requests.try_recv().is_err());
                assert_eq!(worker.inventory.supply_len(), 1);
                let (reply, receiver) = oneshot::channel();
                worker.handle_request(CreationRequest::Attach { lease: lease.clone(), reply });
                worker.dispatch();
                receivers.push(receiver);
            }
            let published = reader.get(&lease).expect("a successful send publishes the assignment");
            let session = published.ready().unwrap();
            assert_eq!(session.database_id, DatabaseId(1));
            for receiver in receivers {
                let received = receiver.await.unwrap().unwrap();
                assert_eq!(received.database_id, session.database_id);
                assert!(Arc::ptr_eq(&received.target, &session.target));
            }
            assert_eq!(worker.inventory.supply_len(), 0);
            assert!(worker.pending.is_empty());
            assert!(worker.waiting.is_empty());
            match cleanup_requests.try_recv().unwrap() {
                Cleanup::Expire { entry, .. } => assert!(Arc::ptr_eq(&entry, &published)),
                _ => panic!("expected one expiry for the accepted assignment"),
            }
            assert!(cleanup_requests.try_recv().is_err());
        }
    }
}
