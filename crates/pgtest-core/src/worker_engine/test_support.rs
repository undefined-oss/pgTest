#![cfg(test)]
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use pgtest_database_operations::manager::config::PostgresConfig;
use pgtest_utils::read_string::ReadString;
use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::worker_engine::{
    core::{EngineCounters, LeaseEntry, LeaseId, WorkerEngine, WorkerEngineConfig},
    database_inventory::DatabaseInventory,
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseWorkerMessages},
    errors::{IOError, PostgresDDLClientError},
    messages::{ConsumerReply, EngineMessage},
    traits::{ConsumerIO, EngineIO, EngineInbox, PostgresClient},
};

#[derive(Clone, Debug)]
pub struct ConsumerWorker {
    messages_replied: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>>,
    fail_send: bool,
}

impl ConsumerWorker {
    pub fn new(buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>>) -> Self {
        Self { messages_replied: buffer, fail_send: false }
    }

    pub fn failing(buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>>) -> Self {
        Self { messages_replied: buffer, fail_send: true }
    }

    pub fn messages(&self) -> VecDeque<ConsumerReply> {
        self.messages_replied.lock().unwrap().clone()
    }
}

impl ConsumerIO for ConsumerWorker {
    fn reply(self, msg: ConsumerReply) -> Result<(), ConsumerReply> {
        if self.fail_send {
            return Err(msg);
        }
        self.messages_replied.lock().unwrap().push_back(msg);
        Ok(())
    }
}

pub struct WorkerEngineIO<'a> {
    inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
    database_progression_name: &'a SequentialDatabaseNames,
    fail: bool,
    messages_pushed: AtomicUsize,
    operations_before_fail: usize,
}

impl<'a> WorkerEngineIO<'a> {
    fn new(
        inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
        database_progression_name: &'a SequentialDatabaseNames,
    ) -> Self {
        Self {
            inbox,
            database_progression_name,
            fail: false,
            operations_before_fail: 0,
            messages_pushed: AtomicUsize::new(0),
        }
    }

    fn fail_once(
        inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
        database_progression_name: &'a SequentialDatabaseNames,
        operations_before_fail: usize,
    ) -> Self {
        Self {
            inbox,
            database_progression_name,
            fail: true,
            operations_before_fail,
            messages_pushed: AtomicUsize::new(0),
        }
    }
}

impl<'a> EngineIO<ConsumerWorker> for WorkerEngineIO<'a> {
    fn request_creation(&self, request: CreateDatabases) -> Result<(), IOError> {
        for index in 0..request.amount.get() {
            let request_number = self.messages_pushed.fetch_add(1, Ordering::SeqCst) + 1;

            let result = if self.fail && request_number == self.operations_before_fail {
                Err(PostgresDDLClientError::OperationFailed("injected creation failure".into()))
            } else {
                Ok(ReadString::from(self.database_progression_name.generate_database_name()))
            };

            self.send_message(EngineMessage::DatabaseWorker(
                DatabaseWorkerMessages::CreationFinished {
                    database_id: request.database_id(index),
                    result,
                },
            ))?;
        }
        Ok(())
    }

    fn request_cleanup(&self, request: CleanupDatabase) -> Result<(), IOError> {
        self.send_message(EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CleanupFinished {
            database_id: request.database_id,
            result: Ok(()),
        }))
    }

    fn send_delayed_message(
        &self,
        _msg: EngineMessage<ConsumerWorker>,
        _wait_duration: u32,
        _cancel_token: CancellationToken,
    ) -> Result<(), IOError> {
        Ok(())
    }

    fn send_message(&self, msg: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        self.inbox.lock().unwrap().push_back(msg);

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct WorkerInboxImpl {
    inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
}

impl WorkerInboxImpl {
    pub fn new(buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>) -> Self {
        WorkerInboxImpl { inbox: buffer }
    }

    fn push_message(&self, msg: EngineMessage<ConsumerWorker>) {
        self.inbox.lock().unwrap().push_back(msg);
    }
}

impl EngineInbox<ConsumerWorker> for WorkerInboxImpl {
    fn wait_for_message(
        &mut self,
    ) -> impl Future<Output = Option<EngineMessage<ConsumerWorker>>> + Send {
        std::future::ready(self.inbox.lock().unwrap().pop_front())
    }
}

/// Each simulation owns its sequence; no global state or production naming
/// policy.
struct SequentialDatabaseNames {
    template: String,
    sequence: AtomicUsize,
}

impl SequentialDatabaseNames {
    fn new(template: String) -> Self {
        Self { template, sequence: AtomicUsize::new(0) }
    }

    fn generate_database_name(&self) -> String {
        let id = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}_{id}", self.template)
    }
}

pub struct PostgresConnection {
    template_database_name: SequentialDatabaseNames,
}

impl PostgresConnection {
    pub(super) fn start(postgres_config: PostgresConfig) -> Self {
        Self {
            template_database_name: SequentialDatabaseNames::new(
                postgres_config.pgtest_pg_database,
            ),
        }
    }
}

impl PostgresClient for PostgresConnection {
    async fn create_databases(
        &self,
        amount: usize,
        mut on_finished: impl FnMut(usize, Result<ReadString, PostgresDDLClientError>) + Send,
    ) {
        for index in 0..amount {
            on_finished(index, self.create_database().await);
        }
    }

    async fn create_database(&self) -> Result<ReadString, PostgresDDLClientError> {
        let database_name = self.template_database_name.generate_database_name();
        Ok(ReadString::from(database_name))
    }

    async fn drop_database(&self, _database_name: &str) -> Result<(), PostgresDDLClientError> {
        Ok(())
    }

    async fn drop_databases(
        &self,
        database_names: &[ReadString],
        mut on_finished: impl FnMut(usize, Result<(), PostgresDDLClientError>) + Send,
    ) {
        for index in 0..database_names.len() {
            on_finished(index, Ok(()));
        }
    }
}

#[derive(Debug)]
pub struct EngineOutcome {
    pub counters: EngineCounters,
    pub leases: FxHashMap<LeaseId, LeaseEntry>,
    pub inventory: DatabaseInventory,
    pub waiters: Vec<LeaseId>,
}

pub struct EngineSimulator;

impl EngineSimulator {
    pub async fn run(msgs: Vec<EngineMessage<ConsumerWorker>>) -> Result<EngineOutcome, IOError> {
        Self::run_with_custom_config(msgs, WorkerEngineConfig::default()).await
    }

    pub async fn run_with_failing_pg<'a>(
        msgs: Vec<EngineMessage<ConsumerWorker>>,
        operations_before_fail: usize,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<EngineOutcome, IOError> {
        assert!(
            operations_before_fail > 0,
            "the failing creation request number must be greater than zero"
        );
        let config = PostgresConfig::default();
        let manager = Arc::from(PostgresConnection::start(config));

        let inbox_buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
            Arc::from(Mutex::new(VecDeque::new()));

        let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
        let manager_to_bg = manager.clone();
        let engine_io = WorkerEngineIO::fail_once(
            inbox_buffer.clone(),
            &manager_to_bg.template_database_name,
            operations_before_fail,
        );

        for msg in msgs {
            inbox.push_message(msg);
        }

        let mut engine = WorkerEngine::new(worker_engine_config, manager, engine_io, inbox.clone());

        engine.try_init().await.unwrap();

        engine.process_messages().await;

        Ok(EngineOutcome {
            counters: engine.counters.clone(),
            leases: engine.leases.clone(),
            inventory: engine.inventory.clone(),
            waiters: engine.waiters.iter().map(|lease| lease.clone()).collect(),
        })
    }

    pub async fn run_with_custom_config<'a>(
        msgs: Vec<EngineMessage<ConsumerWorker>>,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<EngineOutcome, IOError> {
        let config = PostgresConfig::default();
        let manager = Arc::from(PostgresConnection::start(config));

        let inbox_buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
            Arc::from(Mutex::new(VecDeque::new()));

        let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
        let manager_to_bg = manager.clone();
        let engine_io =
            WorkerEngineIO::new(inbox_buffer.clone(), &manager_to_bg.template_database_name);

        for msg in msgs {
            inbox.push_message(msg);
        }

        let mut engine = WorkerEngine::new(worker_engine_config, manager, engine_io, inbox.clone());

        engine.try_init().await.unwrap();

        engine.process_messages().await;

        Ok(EngineOutcome {
            counters: engine.counters.clone(),
            leases: engine.leases.clone(),
            inventory: engine.inventory.clone(),
            waiters: engine.waiters.iter().map(|lease| lease.clone()).collect(),
        })
    }
}

pub fn past_instant() -> std::time::Instant {
    std::time::Instant::now() - std::time::Duration::from_millis(30_001)
}

#[derive(Clone)]
pub struct ScriptedWorkerIO {
    script: Arc<Mutex<VecDeque<Result<(), IOError>>>>,
    inbox: Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
}

impl ScriptedWorkerIO {
    fn new(
        returns: Vec<Result<(), IOError>>,
        inbox: Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
    ) -> Self {
        Self { script: Arc::new(Mutex::new(VecDeque::from(returns))), inbox }
    }

    pub(super) fn remaining(&self) -> usize {
        self.script.lock().unwrap().len()
    }
}

impl EngineIO<ConsumerWorker> for ScriptedWorkerIO {
    fn request_creation(&self, request: CreateDatabases) -> Result<(), IOError> {
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .expect("request_creation called more times than scripted")?;

        for index in 0..request.amount.get() {
            let database_name = ReadString::from(format!("grow_{}", request.database_id(index).0));

            self.send_message(EngineMessage::DatabaseWorker(
                DatabaseWorkerMessages::CreationFinished {
                    database_id: request.database_id(index),
                    result: Ok(database_name),
                },
            ))?;
        }
        Ok(())
    }

    fn request_cleanup(&self, request: CleanupDatabase) -> Result<(), IOError> {
        self.send_message(EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CleanupFinished {
            database_id: request.database_id,
            result: Ok(()),
        }))
    }

    fn send_delayed_message(
        &self,
        _msg: EngineMessage<ConsumerWorker>,
        _wait_duration: u32,
        _cancel_token: CancellationToken,
    ) -> Result<(), IOError> {
        Ok(())
    }

    fn send_message(&self, msg: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        self.inbox.lock().unwrap().push_back(msg);
        Ok(())
    }
}

pub type GrowWorker =
    WorkerEngine<ConsumerWorker, ScriptedWorkerIO, WorkerInboxImpl, PostgresConnection>;

pub fn grow_config() -> WorkerEngineConfig {
    WorkerEngineConfig {
        initial_slots: 1,
        starvation_threshold: 1,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    }
}

pub async fn run_grow(script: Vec<Result<(), IOError>>) -> (GrowWorker, ScriptedWorkerIO) {
    run_grow_with(grow_config(), script).await
}

pub async fn run_grow_with(
    config: WorkerEngineConfig,
    script: Vec<Result<(), IOError>>,
) -> (GrowWorker, ScriptedWorkerIO) {
    let manager = Arc::new(PostgresConnection::start(PostgresConfig::default()));

    let inbox_buffer: Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
    let engine_io = ScriptedWorkerIO::new(script, inbox_buffer);

    let mut worker = GrowWorker::new(config, manager, engine_io.clone(), inbox);

    worker.try_init().await.unwrap();
    worker.grow();
    worker.process_messages().await;

    (worker, engine_io)
}
