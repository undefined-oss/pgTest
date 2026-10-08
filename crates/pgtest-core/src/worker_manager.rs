use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use hotpath::wrap::tokio::sync::mpsc::UnboundedSender;
use pgtest_database_operations::manager::{PostgresManager, config::PostgresConfig};
use tokio::time::timeout_at;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    worker_engine::{
        core::{LeaseId, WorkerEngine, WorkerEngineConfig},
        errors::{AttachError, ReleaseError},
        messages::{ConsumerReply, EngineMessage},
    },
    worker_manager::worker_io::{
        ConsumerWorker, LeaseSession, ManagerReply, WorkerEngineIO, WorkerEngineInbox,
    },
};

mod database_cleanup_worker;
mod database_creation_worker;
mod startup;
pub mod worker_io;

pub use startup::StartError;

#[cfg(test)]
use self::{
    database_cleanup_worker::DatabaseCleanupWorker,
    database_creation_worker::DatabaseCreationWorker, worker_io::DatabaseWorkerSenders,
};

#[cfg(test)]
mod cleanup_tests;

#[cfg(test)]
mod reply_tests;

pub struct WorkerEngineManager {
    worker_inbox_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
    pub pg_client: Arc<PostgresManager>,
    lease_claim_timeout: u64,
    engine_handle: tokio::task::JoinHandle<WorkerEngineType>,
    tracker: TaskTracker,
    shutdown_token: CancellationToken,
}

type WorkerEngineType =
    WorkerEngine<ConsumerWorker, WorkerEngineIO, WorkerEngineInbox, PostgresManager>;

impl WorkerEngineManager {
    #[hotpath::measure]
    pub async fn start(
        postgres_config: PostgresConfig,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<Self, StartError> {
        let postgres_client = startup::prepare_postgres(postgres_config).await?;
        startup::start_workers(postgres_client, worker_engine_config).await
    }

    #[hotpath::measure]
    pub async fn attach(
        &self,
        database_name: &str,
        lease: LeaseId,
    ) -> Result<LeaseSession, AttachError> {
        let template_name = self.pg_client.template_database_name.template_name();
        if template_name.ne(database_name) {
            tracing::warn!(
                %lease,
                requested_database = database_name,
                expected_database = template_name,
                "Lease request rejected: database does not match the configured template"
            );
            return Err(AttachError::TemplateMismatch);
        }

        let now = tokio::time::Instant::now();
        let waiting_response_until = now + Duration::from_millis(self.lease_claim_timeout);
        let (reply_tx, reply_rx) =
            hotpath::channel!(tokio::sync::oneshot::channel(), proxy = true, label = "lease-reply");
        let consumer_worker = ConsumerWorker {
            oneshot_channel: reply_tx,
            attachment: Some((lease.clone(), self.worker_inbox_tx.clone())),
        };

        let request_database_msg = EngineMessage::AttachOrJoin {
            lease: lease.clone(),
            reply: consumer_worker,
            message_time: Instant::from(now),
        };
        let ask_worker_tx = self.worker_inbox_tx.clone();

        match ask_worker_tx.send(request_database_msg) {
            Ok(()) => {
                tracing::debug!(%lease, database_name, "Sent lease request to worker engine");
            }
            Err(_error) => {
                tracing::error!("Unable to ask for a lease");
                return Err(AttachError::EngineUnavailable);
            }
        }

        let reply = if self.lease_claim_timeout == 0 {
            reply_rx.await
        } else {
            timeout_at(waiting_response_until, reply_rx).await.map_err(|_| AttachError::TimedOut)?
        }
        .map_err(|_| AttachError::EngineUnavailable)?;
        match reply {
            ManagerReply::Attached(session) => {
                if session.cancellation.is_cancelled() {
                    return Err(AttachError::LeaseClosed);
                }
                Ok(session)
            }
            ManagerReply::Engine(ConsumerReply::AttachRejected(error)) => Err(error),
            _ => Err(AttachError::Failed),
        }
    }

    /// Success acknowledges logical closure; physical deletion runs in the
    /// background.
    #[hotpath::measure]
    pub async fn release(&self, lease: LeaseId) -> Result<(), ReleaseError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.worker_inbox_tx
            .send(EngineMessage::ReleaseLease { lease, reply: ConsumerWorker::new(reply_tx) })
            .map_err(|_| ReleaseError::EngineUnavailable)?;
        let reply = tokio::time::timeout(Duration::from_secs(5), reply_rx)
            .await
            .map_err(|_| ReleaseError::ReplyTimedOut)?
            .map_err(|_| ReleaseError::EngineUnavailable)?;
        match reply {
            ManagerReply::Engine(ConsumerReply::ReleaseResult(result)) => result,
            _ => Err(ReleaseError::UnexpectedReply),
        }
    }

    #[hotpath::measure]
    pub async fn shutdown(self) {
        self.shutdown_token.cancel();
        self.tracker.close();
        self.tracker.wait().await;

        let _ = self.worker_inbox_tx.send(EngineMessage::Shutdown);
        let _ = self.engine_handle.await;
    }

    #[cfg(test)]
    async fn sync_inbox(&self) {
        let (reply, received) = tokio::sync::oneshot::channel();
        assert!(
            self.worker_inbox_tx.send(EngineMessage::Barrier { reply }).is_ok(),
            "engine inbox must remain open"
        );
        received.await.expect("engine must acknowledge the inbox barrier");
    }

    #[cfg(test)]
    async fn drain_and_snapshot(self) -> WorkerEngineType {
        self.sync_inbox().await;
        // Receiver loops now live in the tracker too. Cancel them before
        // waiting.
        self.shutdown_token.cancel();
        self.tracker.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.tracker.wait().await;
                self.sync_inbox().await;
                if self.tracker.is_empty() {
                    break;
                }
            }
        })
        .await
        .expect("background workers must stop");

        let _ = self.worker_inbox_tx.send(EngineMessage::Shutdown);
        self.engine_handle.await.expect("engine task panicked")
    }
}

#[cfg(test)]
mod worker_engine_manager_test {
    use std::{future::poll_fn, pin::Pin, task::Poll, time::Duration};

    use hotpath::wrap::tokio::sync::mpsc::UnboundedReceiver;
    use pgtest_database_operations::{
        manager::config::PostgresConfig, testcontainer::pg_container_config,
    };
    use pgtest_utils::read_string::ReadString;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use crate::{
        worker_engine::{
            core::{LeaseId, WorkerEngineConfig},
            database_jobs::{CleanupDatabase, CreateDatabases},
            messages::EngineMessage,
            traits::{EngineIO, PostgresClient},
        },
        worker_manager::{WorkerEngineIO, WorkerEngineManager},
    };

    fn no_growth_config(slots: u16) -> WorkerEngineConfig {
        WorkerEngineConfig {
            initial_slots: slots,
            grow_batch_size: 0,
            lease_claim_timeout_ms: 30_000,
            ..WorkerEngineConfig::default()
        }
    }

    #[test]
    fn startup_future_fits_stack_budget() {
        // Nested profiling wrappers used to inflate this to over 600 KiB,
        // overflowing the main thread's stack when startup was first polled.
        let startup =
            WorkerEngineManager::start(PostgresConfig::default(), WorkerEngineConfig::default());
        let size = std::mem::size_of_val(&startup);
        assert!(size < 64 * 1024, "startup future uses {size} bytes");
    }

    async fn start_manager(config: WorkerEngineConfig) -> WorkerEngineManager {
        WorkerEngineManager::start(pg_container_config().await, config)
            .await
            .expect("engine must start")
    }

    // Keep the real manager/engine and PostgreSQL, but explicitly deliver DDL
    // results so timeout and cancellation tests do not depend on database
    // creation speed.
    struct DeferredWorkers {
        creation_rx: UnboundedReceiver<CreateDatabases>,
        _cleanup_rx: UnboundedReceiver<CleanupDatabase>,
        pending: std::collections::VecDeque<crate::worker_engine::database_jobs::DatabaseId>,
    }

    impl DeferredWorkers {
        async fn complete_creation(&mut self, manager: &WorkerEngineManager) -> ReadString {
            if self.pending.is_empty() {
                let request = tokio::time::timeout(Duration::from_secs(5), self.creation_rx.recv())
                    .await
                    .expect("creation must be queued")
                    .expect("creation channel must be open");
                self.pending
                    .extend((0..request.amount.get()).map(|index| request.database_id(index)));
            }
            let database_id = self.pending.pop_front().expect("creation batch must be nonempty");
            let database_name =
                manager.pg_client.create_database().await.expect("test database must be created");
            manager
                .worker_inbox_tx
                .send(EngineMessage::DatabaseWorker(
                    crate::worker_engine::database_jobs::DatabaseWorkerMessages::CreationFinished {
                        database_id,
                        result: Ok(database_name.clone()),
                    },
                ))
                .unwrap();
            manager.sync_inbox().await;
            database_name
        }
    }

    async fn start_with_deferred_workers() -> (WorkerEngineManager, DeferredWorkers) {
        let config = WorkerEngineConfig {
            initial_slots: 1,
            starvation_threshold: 0,
            grow_batch_size: 1,
            ..WorkerEngineConfig::default()
        };
        let pg_client = std::sync::Arc::new(
            pgtest_database_operations::manager::PostgresManager::start(
                pg_container_config().await,
            )
            .await
            .expect("PostgreSQL must start"),
        );
        let (engine_tx, engine_rx) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
        let tracker = tokio_util::task::TaskTracker::new();
        let shutdown_token = CancellationToken::new();
        let (senders, creation_rx, cleanup_rx) =
            super::DatabaseWorkerSenders::init_database_worker_channels();
        let io = WorkerEngineIO::new(
            engine_tx.clone(),
            tracker.clone(),
            shutdown_token.clone(),
            senders,
        );
        let mut engine = super::WorkerEngineType::new(
            config,
            pg_client.clone(),
            io,
            super::WorkerEngineInbox::new(engine_rx),
        );
        engine.try_init().await.unwrap();
        let engine_handle = tokio::spawn(async move {
            engine.run().await;
            engine
        });
        let manager = WorkerEngineManager {
            worker_inbox_tx: engine_tx,
            pg_client,
            lease_claim_timeout: config.lease_claim_timeout_ms,
            engine_handle,
            tracker,
            shutdown_token,
        };
        (
            manager,
            DeferredWorkers { creation_rx, _cleanup_rx: cleanup_rx, pending: Default::default() },
        )
    }

    async fn enqueue_attach<F: Future>(manager: &WorkerEngineManager, mut attach: Pin<&mut F>) {
        poll_fn(|cx| {
            assert!(attach.as_mut().poll(cx).is_pending(), "attach must wait for a slot");
            Poll::Ready(())
        })
        .await;
        manager.sync_inbox().await;
    }

    #[tokio::test]
    async fn immediate_available_templates() {
        start_manager(WorkerEngineConfig::default()).await.shutdown().await;
    }

    #[tokio::test]
    async fn startup_prefills_a_large_batch_on_one_connection() {
        let manager = WorkerEngineManager::start(
            PostgresConfig {
                pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::MIN,
                ..pg_container_config().await
            },
            WorkerEngineConfig { initial_slots: 33, ..WorkerEngineConfig::default() },
        )
        .await
        .unwrap();
        let engine = manager.drain_and_snapshot().await;
        assert_eq!(engine.inventory.ready().len(), 33);
        assert!(engine.inventory.creating().is_empty());
        assert_eq!(
            engine
                .inventory
                .ready()
                .iter()
                .map(|db| db.database_id)
                .collect::<rustc_hash::FxHashSet<_>>()
                .len(),
            33
        );
    }

    #[tokio::test]
    async fn startup_returns_initial_creation_failure_without_panicking() {
        let client = std::sync::Arc::new(
            pgtest_database_operations::manager::PostgresManager::start(
                pg_container_config().await,
            )
            .await
            .unwrap(),
        );
        client.drop_ddl_database(client.template_database_name.template_name()).await.unwrap();
        let result = super::startup::start_workers(client, no_growth_config(1)).await;
        assert!(matches!(result, Err(super::StartError::InitialDatabaseCreation(_))));
    }

    #[tokio::test]
    async fn startup_cleanup_is_isolated_between_test_managers() {
        use pgtest_database_operations::manager::PostgresManager;
        use tokio_postgres::{Config, NoTls};

        let first_config = pg_container_config().await;
        let mut options = Config::new();
        options
            .host(&first_config.pgtest_pg_host)
            .port(first_config.pgtest_pg_port.get())
            .user(&first_config.pgtest_pg_user)
            .password("postgres")
            .dbname("postgres");
        let first = WorkerEngineManager::start(first_config, no_growth_config(1)).await.unwrap();
        let session = first
            .attach(
                first.pg_client.template_database_name.template_name(),
                LeaseId::new("active").unwrap(),
            )
            .await
            .unwrap();

        let second_config = pg_container_config().await;
        let stale_database = {
            let client = PostgresManager::start(PostgresConfig {
                pgtest_pg_database: second_config.pgtest_pg_database.clone(),
                pgtest_pg_host: second_config.pgtest_pg_host.clone(),
                pgtest_pg_user: second_config.pgtest_pg_user.clone(),
                ..second_config
            })
            .await
            .unwrap();
            client.create_ddl_database().await.unwrap()
        };

        // Run startup cleanup after the first manager has assigned a database,
        // making the destructive interleaving deterministic.
        let second = WorkerEngineManager::start(second_config, no_growth_config(0)).await.unwrap();
        let (client, connection) = options.connect(NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        let databases: Vec<String> = client
            .query("SELECT datname FROM pg_database", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        drop(client);
        connection.await.unwrap().unwrap();
        first.shutdown().await;
        second.shutdown().await;

        assert!(
            databases.iter().any(|name| name == session.database_name.as_ref()),
            "startup cleanup must preserve another test manager's active database"
        );
        assert!(
            !databases.iter().any(|name| name == stale_database.as_ref()),
            "startup cleanup must still remove its own stale databases"
        );
    }

    #[tokio::test]
    async fn when_the_last_lease_session_is_dropped_then_the_database_remains_assigned() {
        let worker_engine_config =
            WorkerEngineConfig { lease_claim_timeout_ms: 5_000, ..WorkerEngineConfig::default() };
        let manager = start_manager(worker_engine_config).await;

        let lease = LeaseId::new("connection1").unwrap();
        let session = manager
            .attach(manager.pg_client.template_database_name.template_name(), lease.clone())
            .await
            .expect("attach against the template database succeeds");
        let assigned_database = session.database_name.to_string();

        drop(session);

        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();

        assert_eq!(snapshot.leases.get(&lease).expect("lease must remain assigned").conns, 0);
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(
            snapshot.inventory.ready().len(),
            usize::from(WorkerEngineConfig::default().initial_slots - 1)
        );

        let mut assigned_config = pg_container_config().await;
        assigned_config.pgtest_pg_database = assigned_database;
        assert!(
            pgtest_database_operations::manager::PostgresManager::start(assigned_config)
                .await
                .is_ok(),
            "the database must still exist after its last session closes"
        );
    }

    #[tokio::test]
    async fn given_a_lease_with_multiples_connection_when_one_session_is_dropped_then_lease_connection_is_not_tracked_anymore()
     {
        let worker_engine_config =
            WorkerEngineConfig { lease_claim_timeout_ms: 5_000, ..WorkerEngineConfig::default() };
        let manager = start_manager(worker_engine_config).await;

        let lease = LeaseId::new("connection1").unwrap();
        let _no_dropped_session = manager
            .attach(manager.pg_client.template_database_name.template_name(), lease.clone())
            .await
            .expect("attach against the template database succeeds");

        let session = manager
            .attach(manager.pg_client.template_database_name.template_name(), lease.clone())
            .await
            .expect("attach against the template database succeeds");

        drop(session);

        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();

        assert!(snapshot.leases.contains_key(&lease));
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(snapshot.leases[&lease].conns, 1);
        assert_eq!(
            snapshot.inventory.ready().len(),
            usize::from(WorkerEngineConfig::default().initial_slots - 1)
        );
    }

    #[tokio::test]
    async fn attach_unknown_template_rejected() {
        let manager = start_manager(WorkerEngineConfig::default()).await;

        let result = manager.attach("not_the_template", LeaseId::new("connection1").unwrap()).await;
        assert!(result.is_err());

        let engine = manager.drain_and_snapshot().await;
        assert!(engine.snapshot().leases.is_empty());
    }

    #[tokio::test]
    async fn when_attach_times_out_without_free_slots_then_the_consumer_stops_waiting_for_a_response()
     {
        let worker_engine_config = WorkerEngineConfig {
            initial_slots: 0,
            grow_batch_size: 0,
            lease_claim_timeout_ms: 100,
            ..WorkerEngineConfig::default()
        };

        let manager = start_manager(worker_engine_config).await;

        tokio::time::pause();
        let lease = LeaseId::new("connection1").unwrap();
        let started_at = tokio::time::Instant::now();
        let result = manager
            .attach(manager.pg_client.template_database_name.template_name(), lease.clone())
            .await;
        assert!(result.is_err(), "attach must time out when no slot can ever free up");
        assert!(
            started_at.elapsed()
                >= std::time::Duration::from_millis(worker_engine_config.lease_claim_timeout_ms),
            "attach must wait until the claim deadline"
        );

        tokio::time::resume();
        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert_eq!(
            snapshot.waiters.len(),
            1,
            "the timed-out claim remains queued until supply arrives"
        );
        assert_eq!(snapshot.waiters.front().unwrap(), &lease);
    }

    #[tokio::test]
    async fn creation_after_attach_timeout_leaves_the_new_database_ready() {
        let (mut manager, mut workers) = start_with_deferred_workers().await;
        let session = manager
            .attach(
                manager.pg_client.template_database_name.template_name(),
                LeaseId::new("holder").unwrap(),
            )
            .await
            .expect("first lease must occupy the only slot");
        let original_database = session.database_name.clone();

        // The engine uses its claim setting for lease lifetime too. Shorten
        // only the caller's deadline, then explicitly deliver lifetime expiry.
        manager.lease_claim_timeout = 100;
        let started_at = Instant::now();
        let result = manager
            .attach(
                manager.pg_client.template_database_name.template_name(),
                LeaseId::new("timed_out").unwrap(),
            )
            .await;
        assert!(result.is_err(), "the occupied slot cannot satisfy the request");
        assert!(started_at.elapsed() >= Duration::from_millis(100));

        drop(session);
        manager
            .worker_inbox_tx
            .send(EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("holder").unwrap(),
                generation: 1,
            })
            .unwrap();
        let created = workers.complete_creation(&manager).await;
        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert!(snapshot.waiters.is_empty(), "creation must skip the closed reply channel");
        assert_eq!(snapshot.inventory.ready().len(), 1);
        assert_eq!(snapshot.inventory.ready()[0].database_name, created);
        assert_ne!(created, original_database);
        assert_eq!(snapshot.inventory.retiring().len(), 1, "cleanup has not completed");
    }

    #[tokio::test]
    async fn queued_attach_receives_creation_while_cleanup_is_pending() {
        let (manager, mut workers) = start_with_deferred_workers().await;
        let holder = LeaseId::new("holder").unwrap();
        let waiting = LeaseId::new("waiting").unwrap();
        let session = manager
            .attach(manager.pg_client.template_database_name.template_name(), holder.clone())
            .await
            .expect("first lease must occupy the only slot");

        let mut attach = Box::pin(
            manager
                .attach(manager.pg_client.template_database_name.template_name(), waiting.clone()),
        );
        enqueue_attach(&manager, attach.as_mut()).await;
        drop(session);
        manager
            .worker_inbox_tx
            .send(EngineMessage::LeaseMaxTimeReached { lease: holder.clone(), generation: 1 })
            .unwrap();
        let created = workers.complete_creation(&manager).await;
        let received_session = attach.await.expect("queued attach must receive the fresh database");
        assert_eq!(received_session.database_name, created);

        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();
        assert!(snapshot.waiters.is_empty());
        assert!(!snapshot.leases.contains_key(&holder));
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(snapshot.leases.get(&waiting).expect("new lease must exist").conns, 1);
        assert_eq!(
            snapshot.leases[&waiting].database.database_name,
            received_session.database_name
        );
        assert_eq!(snapshot.inventory.retiring().len(), 1, "attachment did not wait for cleanup");
        assert_eq!(snapshot.inventory.ready().len(), 0);
        drop(received_session);
    }

    #[tokio::test]
    async fn creation_skips_a_cancelled_attach_and_keeps_the_database_ready() {
        let (manager, mut workers) = start_with_deferred_workers().await;
        let session = manager
            .attach(
                manager.pg_client.template_database_name.template_name(),
                LeaseId::new("holder").unwrap(),
            )
            .await
            .expect("first lease must occupy the only slot");
        let original_database = session.database_name.clone();

        let started_at = Instant::now();
        let mut attach = Box::pin(manager.attach(
            manager.pg_client.template_database_name.template_name(),
            LeaseId::new("cancelled").unwrap(),
        ));
        enqueue_attach(&manager, attach.as_mut()).await;
        drop(attach);
        drop(session);
        manager
            .worker_inbox_tx
            .send(EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("holder").unwrap(),
                generation: 1,
            })
            .unwrap();

        let created = workers.complete_creation(&manager).await;
        let engine = manager.drain_and_snapshot().await;
        assert!(
            started_at.elapsed() < Duration::from_secs(30),
            "the reply must be skipped because its receiver closed, before waiter expiry"
        );
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert!(snapshot.waiters.is_empty());
        assert_eq!(snapshot.inventory.ready()[0].database_name, created);
        assert_ne!(created, original_database);
        assert_eq!(snapshot.inventory.ready().len(), 1);
    }

    #[tokio::test]
    async fn given_a_closed_engine_inbox_when_attaching_then_the_caller_fails_without_waiting() {
        let mut manager = start_manager(no_growth_config(0)).await;
        assert!(manager.worker_inbox_tx.send(EngineMessage::Shutdown).is_ok());
        let engine = (&mut manager.engine_handle).await.expect("engine must not panic");
        drop(engine);
        assert!(manager.worker_inbox_tx.is_closed());

        let result = manager
            .attach(
                manager.pg_client.template_database_name.template_name(),
                LeaseId::new("after_shutdown").unwrap(),
            )
            .await;

        manager.shutdown_token.cancel();
        manager.tracker.close();
        manager.tracker.wait().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_a_pending_delayed_message_when_shutting_down_then_the_task_is_cancelled() {
        let manager = start_manager(no_growth_config(0)).await;
        let tracker = manager.tracker.clone();
        let inbox = manager.worker_inbox_tx.clone();
        let initial_tasks = tracker.len();
        let (senders, _creation_rx, _cleanup_rx) =
            super::DatabaseWorkerSenders::init_database_worker_channels();
        let io = WorkerEngineIO::new(
            inbox.clone(),
            tracker.clone(),
            manager.shutdown_token.clone(),
            senders,
        );
        let (reply, mut received) = tokio::sync::oneshot::channel();
        io.send_delayed_message(EngineMessage::Barrier { reply }, 60_000, CancellationToken::new())
            .expect("delayed message task must be scheduled");

        assert_eq!(tracker.len(), initial_tasks + 1);
        assert!(matches!(
            received.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        manager.shutdown().await;
        assert!(tracker.is_empty());
        assert!(inbox.is_closed());
        assert!(received.await.is_err(), "shutdown must discard the delayed message");
    }
}
