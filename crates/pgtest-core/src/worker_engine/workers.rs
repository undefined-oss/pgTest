//! Executor-independent worker scheduling. Drivers execute returned operations.
use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
};

use pgtest_engine_backend::{BackendError, ProvisionedDatabase};

use super::database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId, DatabaseWorkerMessages};

pub enum CreationAction {
    Begin(DatabaseId),
    Report(DatabaseWorkerMessages),
}
pub struct CreationState {
    queued: VecDeque<CreateDatabases>,
    current: Option<(CreateDatabases, usize)>,
    active: HashSet<DatabaseId>,
    limit: usize,
}
impl CreationState {
    pub fn new(limit: NonZeroUsize) -> Self {
        Self { queued: VecDeque::new(), current: None, active: HashSet::new(), limit: limit.get() }
    }

    pub fn enqueue(&mut self, request: CreateDatabases) -> Vec<CreationAction> {
        self.queued.push_back(request);
        self.fill()
    }

    pub fn complete(
        &mut self,
        id: DatabaseId,
        result: Result<ProvisionedDatabase, BackendError>,
    ) -> Vec<CreationAction> {
        if !self.active.remove(&id) {
            return Vec::new();
        }
        let mut actions = vec![CreationAction::Report(DatabaseWorkerMessages::CreationFinished {
            database_id: id,
            result,
        })];
        actions.extend(self.fill());
        actions
    }

    fn fill(&mut self) -> Vec<CreationAction> {
        let mut actions = Vec::new();
        loop {
            if self.current.as_ref().is_some_and(|(batch, next)| *next == batch.amount.get()) {
                if !self.active.is_empty() {
                    break;
                }
                self.current = None;
            }
            if self.current.is_none() {
                self.current = self.queued.pop_front().map(|batch| (batch, 0));
            }
            let Some((batch, next)) = &mut self.current else {
                break;
            };
            if self.active.len() == self.limit {
                break;
            }
            let id = batch.database_id(*next);
            *next += 1;
            self.active.insert(id);
            actions.push(CreationAction::Begin(id));
        }
        actions
    }

    pub fn is_idle(&self) -> bool {
        self.current.is_none() && self.queued.is_empty() && self.active.is_empty()
    }

    pub fn stop(&mut self) {
        self.queued.clear();
        self.current = None;
        self.active.clear();
    }
}

pub enum CleanupAction {
    Begin(CleanupDatabase),
    Report(DatabaseWorkerMessages),
}
pub struct CleanupState {
    queued: VecDeque<CleanupDatabase>,
    active: HashSet<DatabaseId>,
    limit: usize,
}
impl CleanupState {
    pub fn new(limit: NonZeroUsize) -> Self {
        Self { queued: VecDeque::new(), active: HashSet::new(), limit: limit.get() }
    }

    pub fn enqueue(&mut self, request: CleanupDatabase) -> Vec<CleanupAction> {
        self.queued.push_back(request);
        self.fill()
    }

    pub fn complete(
        &mut self,
        id: DatabaseId,
        result: Result<(), BackendError>,
    ) -> Vec<CleanupAction> {
        if !self.active.remove(&id) {
            return Vec::new();
        }
        let mut actions = vec![CleanupAction::Report(DatabaseWorkerMessages::CleanupFinished {
            database_id: id,
            result,
        })];
        actions.extend(self.fill());
        actions
    }

    fn fill(&mut self) -> Vec<CleanupAction> {
        let mut actions = Vec::new();
        while self.active.len() < self.limit {
            let Some(request) = self.queued.pop_front() else {
                break;
            };
            self.active.insert(request.database_id);
            actions.push(CleanupAction::Begin(request));
        }
        actions
    }

    pub fn is_idle(&self) -> bool {
        self.queued.is_empty() && self.active.is_empty()
    }

    pub fn stop(&mut self) {
        self.queued.clear();
        self.active.clear();
    }
}
