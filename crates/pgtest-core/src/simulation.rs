//! Deterministic runtime: no futures, executor, wall clock, threads, or Tokio.
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    num::NonZeroUsize,
    rc::Rc,
    time::Duration,
};

use pgtest_engine_backend::{
    BackendError, ProvisionedDatabase,
    jobs::{CleanupDatabase, CreateDatabases, DatabaseId, DatabaseWorkerMessages},
    workers::{CleanupAction, CleanupState, CreationAction, CreationState},
};

use crate::{
    config::ManagerConfig,
    manager_handle::{
        database_inventory::DatabaseInventory,
        errors::{AttachError, IOError, ReleaseError},
        lease::{LeaseId, LeaseKey},
        manager_worker::{ManagerIO, ManagerWorker},
        messages::{ConsumerReply, ElapsedTime, ManagerMessage},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorId {
    Manager,
    Creation,
    Cleanup,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    Progress,
    Idle,
    Stopped,
}
#[derive(Clone, Debug)]
pub enum ReplySlot {
    Pending,
    Delivered(ConsumerReply),
    Cancelled,
}

struct Timer {
    key: LeaseKey,
    deadline: ElapsedTime,
    sequence: u64,
    message: ManagerMessage<RequestId>,
}
enum CreationInput {
    Command(CreateDatabases),
    Complete(DatabaseId, Result<ProvisionedDatabase, BackendError>),
}
enum CleanupInput {
    Command(CleanupDatabase),
    Complete(DatabaseId, Result<(), BackendError>),
}
#[derive(Default)]
struct Shared {
    manager: VecDeque<ManagerMessage<RequestId>>,
    creation: VecDeque<CreationInput>,
    cleanup: VecDeque<CleanupInput>,
    replies: BTreeMap<RequestId, ReplySlot>,
    timers: Vec<Timer>,
    cancelled: Vec<LeaseKey>,
    closed: [bool; 3],
    fail_next: [bool; 3],
    trace: Vec<String>,
    sequence: u64,
}
#[derive(Clone)]
pub struct SimPorts(Rc<RefCell<Shared>>);
impl ManagerIO for SimPorts {
    type ReplyHandle = RequestId;

    fn request_creation(&mut self, request: CreateDatabases) -> Result<(), IOError> {
        let mut s = self.0.borrow_mut();
        if s.closed[1] || std::mem::take(&mut s.fail_next[1]) {
            return Err(IOError::FailedToSendTheMessage);
        }
        s.trace.push(format!("create {request:?}"));
        s.creation.push_back(CreationInput::Command(request));
        Ok(())
    }

    fn request_cleanup(&mut self, request: CleanupDatabase) -> Result<(), IOError> {
        let mut s = self.0.borrow_mut();
        if s.closed[2] || std::mem::take(&mut s.fail_next[2]) {
            return Err(IOError::FailedToSendTheMessage);
        }
        s.trace.push(format!("delete {request:?}"));
        s.cleanup.push_back(CleanupInput::Command(request));
        Ok(())
    }

    fn reply(&mut self, id: RequestId, reply: ConsumerReply) -> Result<(), ConsumerReply> {
        let mut s = self.0.borrow_mut();
        if !matches!(s.replies.get(&id), Some(ReplySlot::Pending)) {
            return Err(reply);
        }
        s.trace.push(format!("reply {id:?} {reply:?}"));
        s.replies.insert(id, ReplySlot::Delivered(reply));
        Ok(())
    }

    fn schedule(
        &mut self,
        key: LeaseKey,
        deadline: ElapsedTime,
        message: ManagerMessage<RequestId>,
    ) {
        let mut s = self.0.borrow_mut();
        s.sequence += 1;
        let sequence = s.sequence;
        s.timers.push(Timer { key, deadline, sequence, message });
    }

    fn cancel_sessions(&mut self, key: &LeaseKey) {
        let mut s = self.0.borrow_mut();
        s.timers.retain(|timer| &timer.key != key);
        s.cancelled.push(key.clone());
    }
}

pub struct SimRuntime {
    manager: ManagerWorker<SimPorts>,
    creation: CreationState,
    cleanup: CleanupState,
    shared: Rc<RefCell<Shared>>,
    now: ElapsedTime,
    next_request: u64,
    claim_timeout: Duration,
    deadlines: BTreeMap<RequestId, (ElapsedTime, bool)>,
    active_creation: BTreeMap<u64, DatabaseId>,
    active_cleanup: BTreeMap<u64, CleanupDatabase>,
    round_robin: usize,
    startup_result: Option<Result<(), BackendError>>,
}
impl SimRuntime {
    pub fn new(config: ManagerConfig) -> Self {
        Self::with_options(config, NonZeroUsize::new(10).unwrap(), NonZeroUsize::new(5).unwrap())
    }

    pub fn with_options(
        config: ManagerConfig,
        creation_limit: NonZeroUsize,
        cleanup_limit: NonZeroUsize,
    ) -> Self {
        let shared = Rc::new(RefCell::new(Shared::default()));
        let mut inventory = DatabaseInventory::default();
        let mut ports = SimPorts(shared.clone());
        if let Some(request) = inventory.reserve_creations(usize::from(*config.initial_slots)) {
            ports.request_creation(request).expect("new simulation has open worker inboxes");
        }
        let startup_result = inventory.creating().is_empty().then_some(Ok(()));
        let manager = ManagerWorker::new(config, inventory, ports);
        Self {
            manager,
            shared,
            creation: CreationState::new(creation_limit),
            cleanup: CleanupState::new(cleanup_limit),
            now: ElapsedTime::default(),
            next_request: 0,
            claim_timeout: Duration::from_nanos_u128(
                config
                    .lease_claim_timeout_ms
                    .checked_mul(1_000_000)
                    .expect("lease timeout is too large"),
            ),
            deadlines: BTreeMap::new(),
            active_creation: BTreeMap::new(),
            active_cleanup: BTreeMap::new(),
            round_robin: 0,
            startup_result,
        }
    }

    fn request(&mut self) -> RequestId {
        self.next_request += 1;
        let id = RequestId(self.next_request);
        let slot =
            if self.shared.borrow().closed[0] { ReplySlot::Cancelled } else { ReplySlot::Pending };
        self.shared.borrow_mut().replies.insert(id, slot);
        id
    }

    pub fn attach(&mut self, lease: &str) -> RequestId {
        let reply = self.request();
        if self.startup_result.is_none() {
            let _ = SimPorts(self.shared.clone())
                .reply(reply, ConsumerReply::AttachRejected(AttachError::EngineUnavailable));
            return reply;
        }
        if !self.claim_timeout.is_zero() {
            self.deadlines.insert(reply, (ElapsedTime(self.now.0 + self.claim_timeout), true));
        }
        self.inject(ManagerMessage::AttachOrJoin {
            lease: LeaseId::new(lease).unwrap(),
            reply,
            message_time: self.now,
        });
        reply
    }

    pub fn release(&mut self, lease: &str) -> RequestId {
        let reply = self.request();
        if self.startup_result.is_none() {
            let _ = SimPorts(self.shared.clone())
                .reply(reply, ConsumerReply::ReleaseResult(Err(ReleaseError::EngineUnavailable)));
            return reply;
        }
        self.deadlines.insert(reply, (ElapsedTime(self.now.0 + Duration::from_secs(5)), false));
        self.inject(ManagerMessage::ReleaseLease { lease: LeaseId::new(lease).unwrap(), reply });
        reply
    }

    /// Inject duplicate/stale messages without pretending they are active
    /// operations.
    pub fn inject(&mut self, message: ManagerMessage<RequestId>) {
        if !self.shared.borrow().closed[0] {
            self.shared.borrow_mut().manager.push_back(message);
        }
    }

    pub fn reply(&self, id: RequestId) -> ReplySlot {
        self.shared.borrow().replies[&id].clone()
    }

    pub fn cancel_request(&mut self, id: RequestId) {
        self.drop_session(id);
    }

    /// Dropping a reply or session does not release an assigned lease.
    pub fn drop_session(&mut self, id: RequestId) {
        self.shared.borrow_mut().replies.insert(id, ReplySlot::Cancelled);
    }

    pub fn session_cancelled(&self, key: &LeaseKey) -> bool {
        self.shared.borrow().cancelled.contains(key)
    }

    pub fn active_creations(&self) -> Vec<DatabaseId> {
        self.active_creation.values().copied().collect()
    }

    pub fn active_cleanups(&self) -> Vec<CleanupDatabase> {
        self.active_cleanup.values().cloned().collect()
    }

    pub fn complete_creation(
        &mut self,
        id: DatabaseId,
        result: Result<ProvisionedDatabase, BackendError>,
    ) {
        assert!(self.active_creation.remove(&id.0).is_some(), "creation is not active");
        self.shared.borrow_mut().creation.push_back(CreationInput::Complete(id, result));
    }

    pub fn complete_cleanup(&mut self, id: DatabaseId, result: Result<(), BackendError>) {
        assert!(self.active_cleanup.remove(&id.0).is_some(), "cleanup is not active");
        self.shared.borrow_mut().cleanup.push_back(CleanupInput::Complete(id, result));
    }

    pub fn now(&self) -> ElapsedTime {
        self.now
    }

    pub fn advance_by(&mut self, duration: Duration) {
        self.now.0 += duration;
        let mut s = self.shared.borrow_mut();
        // These are caller deadlines, separate from actor-owned lease timers.
        self.deadlines.retain(|id, (deadline, attach)| {
            if !matches!(s.replies.get(id), Some(ReplySlot::Pending)) {
                return false;
            }
            if *deadline > self.now {
                return true;
            }
            let reply = if *attach {
                ConsumerReply::AttachRejected(crate::manager_handle::errors::AttachError::TimedOut)
            } else {
                ConsumerReply::ReleaseResult(Err(
                    crate::manager_handle::errors::ReleaseError::ReplyTimedOut,
                ))
            };
            s.replies.insert(*id, ReplySlot::Delivered(reply));
            false
        });
        s.timers.sort_by_key(|t| (t.deadline, t.sequence));
        let timers = std::mem::take(&mut s.timers);
        for timer in timers {
            if timer.deadline <= self.now {
                s.manager.push_back(timer.message);
            } else {
                s.timers.push(timer);
            }
        }
    }

    pub fn startup_result(&self) -> Option<&Result<(), BackendError>> {
        self.startup_result.as_ref()
    }

    pub fn snapshot(&self) -> &ManagerWorker<SimPorts> {
        &self.manager
    }

    pub fn trace(&self) -> Vec<String> {
        self.shared.borrow().trace.clone()
    }

    pub fn step(&mut self, actor: ActorId) -> StepResult {
        if self.shared.borrow().closed.iter().all(|closed| *closed) {
            return StepResult::Stopped;
        }
        match actor {
            ActorId::Manager => {
                let Some(message) = self.shared.borrow_mut().manager.pop_front() else {
                    return StepResult::Idle;
                };
                self.shared.borrow_mut().trace.push(format!("manager {message:?}"));
                if matches!(message, ManagerMessage::Shutdown) {
                    self.stop();
                    return StepResult::Stopped;
                }
                if self.startup_result.is_none() {
                    // Consume initial results in the runtime before dispatching
                    // to Manager.
                    let ManagerMessage::DatabaseWorker(DatabaseWorkerMessages::CreationFinished {
                        database_id,
                        result,
                    }) = message
                    else {
                        panic!("only creation results can arrive during startup");
                    };
                    if let Some(Err(error)) =
                        self.manager.inventory.complete_creation(database_id, result)
                    {
                        self.startup_result = Some(Err(error));
                        self.stop();
                        return StepResult::Stopped;
                    }
                    if self.manager.inventory.creating().is_empty() {
                        self.startup_result = Some(Ok(()));
                    }
                } else {
                    self.manager.handle(message, self.now);
                }
            }
            ActorId::Creation => {
                let Some(input) = self.shared.borrow_mut().creation.pop_front() else {
                    return StepResult::Idle;
                };
                let actions = match input {
                    CreationInput::Command(r) => self.creation.enqueue(r),
                    CreationInput::Complete(id, result) => self.creation.complete(id, result),
                };
                for action in actions {
                    match action {
                        CreationAction::Begin(id) => {
                            self.active_creation.insert(id.0, id);
                            self.shared.borrow_mut().trace.push(format!("begin create {id:?}"));
                        }
                        CreationAction::Report(event) => {
                            self.inject(ManagerMessage::DatabaseWorker(event))
                        }
                    }
                }
            }
            ActorId::Cleanup => {
                let Some(input) = self.shared.borrow_mut().cleanup.pop_front() else {
                    return StepResult::Idle;
                };
                let actions = match input {
                    CleanupInput::Command(r) => self.cleanup.enqueue(r),
                    CleanupInput::Complete(id, result) => self.cleanup.complete(id, result),
                };
                for action in actions {
                    match action {
                        CleanupAction::Begin(r) => {
                            self.shared
                                .borrow_mut()
                                .trace
                                .push(format!("begin delete {:?}", r.database_id));
                            self.active_cleanup.insert(r.database_id.0, r);
                        }
                        CleanupAction::Report(event) => {
                            self.inject(ManagerMessage::DatabaseWorker(event))
                        }
                    }
                }
            }
        }
        StepResult::Progress
    }

    pub fn run_until_idle(&mut self, max_steps: usize) -> Result<(), String> {
        let mut idle = 0;
        for _ in 0..max_steps {
            let actor = [ActorId::Manager, ActorId::Creation, ActorId::Cleanup][self.round_robin];
            self.round_robin = (self.round_robin + 1) % 3;
            match self.step(actor) {
                StepResult::Progress => idle = 0,
                StepResult::Stopped => return Ok(()),
                StepResult::Idle => idle += 1,
            }
            if idle == 3 {
                return Ok(());
            }
        }
        Err(format!("simulation step limit reached: {:?}", self.trace()))
    }

    /// Inject one failed worker submission without executing the requested
    /// work.
    pub fn fail_next_submission(&mut self, actor: ActorId) {
        assert!(actor != ActorId::Manager, "only worker submissions can fail");
        self.shared.borrow_mut().fail_next[actor as usize] = true;
    }

    pub fn close_mailbox(&mut self, actor: ActorId) {
        self.shared.borrow_mut().closed[actor as usize] = true;
        // Closing a mailbox explicitly stops this simulation.
        self.stop();
    }

    pub fn shutdown(&mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if self.shared.borrow().closed.iter().all(|closed| *closed) {
            return;
        }
        self.manager.shutdown();
        self.creation.stop();
        self.cleanup.stop();
        self.active_creation.clear();
        self.active_cleanup.clear();
        self.deadlines.clear();
        let mut s = self.shared.borrow_mut();
        s.closed = [true; 3];
        s.manager.clear();
        s.creation.clear();
        s.cleanup.clear();
        s.timers.clear();
        for reply in s.replies.values_mut() {
            if matches!(reply, ReplySlot::Pending) {
                *reply = ReplySlot::Cancelled;
            }
        }
    }
}
