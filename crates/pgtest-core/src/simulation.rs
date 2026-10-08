//! Deterministic runtime: no futures, executor, wall clock, threads, or Tokio.
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    num::NonZeroUsize,
    rc::Rc,
    time::Duration,
};

use pgtest_engine_backend::{BackendError, ProvisionedDatabase, ResourceId};

use crate::worker_engine::{
    core::{LeaseId, WorkerEngine, WorkerEngineConfig},
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId},
    errors::IOError,
    messages::{ConsumerReply, EngineMessage, LeaseKey, RequestId, Tick},
    traits::EngineIO,
    workers::{CleanupAction, CleanupState, CreationAction, CreationState},
};

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
    deadline: Tick,
    sequence: u64,
    message: EngineMessage,
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
    manager: VecDeque<EngineMessage>,
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
impl EngineIO for SimPorts {
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

    fn schedule(&mut self, key: LeaseKey, deadline: Tick, message: EngineMessage) {
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
    manager: WorkerEngine<SimPorts>,
    creation: CreationState,
    cleanup: CleanupState,
    shared: Rc<RefCell<Shared>>,
    now: Tick,
    next_request: u64,
    claim_timeout: Duration,
    deadlines: BTreeMap<RequestId, (Tick, bool)>,
    active_creation: BTreeMap<u64, DatabaseId>,
    active_cleanup: BTreeMap<u64, CleanupDatabase>,
    round_robin: usize,
}
impl SimRuntime {
    pub fn new(config: WorkerEngineConfig) -> Self {
        Self::with_options(
            config,
            "template".into(),
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(5).unwrap(),
            vec![],
        )
    }

    pub fn with_options(
        config: WorkerEngineConfig,
        template: String,
        creation_limit: NonZeroUsize,
        cleanup_limit: NonZeroUsize,
        stale: Vec<ResourceId>,
    ) -> Self {
        let shared = Rc::new(RefCell::new(Shared::default()));
        let mut manager = WorkerEngine::new(config, template, SimPorts(shared.clone()));
        manager.initialize(stale);
        Self {
            manager,
            shared,
            creation: CreationState::new(creation_limit),
            cleanup: CleanupState::new(cleanup_limit),
            now: Tick::default(),
            next_request: 0,
            claim_timeout: Duration::from_millis(config.lease_claim_timeout_ms),
            deadlines: BTreeMap::new(),
            active_creation: BTreeMap::new(),
            active_cleanup: BTreeMap::new(),
            round_robin: 0,
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

    pub fn attach(&mut self, template: &str, lease: &str) -> RequestId {
        let reply = self.request();
        if !self.claim_timeout.is_zero() {
            self.deadlines.insert(reply, (Tick(self.now.0 + self.claim_timeout), true));
        }
        self.inject(EngineMessage::AttachOrJoin {
            template: template.into(),
            lease: LeaseId::new(lease).unwrap(),
            reply,
            message_time: self.now,
        });
        reply
    }

    pub fn release(&mut self, lease: &str) -> RequestId {
        let reply = self.request();
        self.deadlines.insert(reply, (Tick(self.now.0 + Duration::from_secs(5)), false));
        self.inject(EngineMessage::ReleaseLease { lease: LeaseId::new(lease).unwrap(), reply });
        reply
    }

    /// Inject duplicate/stale messages without pretending they are active
    /// operations.
    pub fn inject(&mut self, message: EngineMessage) {
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

    /// A delivered reply owns a session even before a caller reads it.
    pub fn drop_session(&mut self, id: RequestId) {
        let old = self.shared.borrow_mut().replies.insert(id, ReplySlot::Cancelled);
        if let Some(ReplySlot::Delivered(ConsumerReply::Attached { key, .. })) = old {
            self.inject(EngineMessage::Detach { lease: key.lease, generation: key.generation });
        }
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

    pub fn now(&self) -> Tick {
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
                ConsumerReply::AttachRejected(crate::worker_engine::errors::AttachError::TimedOut)
            } else {
                ConsumerReply::ReleaseResult(Err(
                    crate::worker_engine::errors::ReleaseError::ReplyTimedOut,
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

    pub fn snapshot(&self) -> &WorkerEngine<SimPorts> {
        &self.manager
    }

    pub fn trace(&self) -> Vec<String> {
        self.shared.borrow().trace.clone()
    }

    pub fn step(&mut self, actor: ActorId) -> StepResult {
        if self.manager.is_stopped() {
            return StepResult::Stopped;
        }
        match actor {
            ActorId::Manager => {
                let Some(message) = self.shared.borrow_mut().manager.pop_front() else {
                    return StepResult::Idle;
                };
                self.shared.borrow_mut().trace.push(format!("manager {message:?}"));
                self.manager.handle(message, self.now);
                if self.manager.is_stopped() {
                    self.stop();
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
                            self.inject(EngineMessage::DatabaseWorker(event))
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
                            self.inject(EngineMessage::DatabaseWorker(event))
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
        // Runtime supervision treats loss of any worker as fatal.
        self.stop();
    }

    pub fn shutdown(&mut self) {
        self.stop();
    }

    fn stop(&mut self) {
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
