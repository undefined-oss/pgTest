//! Executor-independent worker scheduling. Drivers execute returned operations.
use std::{
    collections::{HashSet, VecDeque},
    num::NonZeroUsize,
};

use crate::{
    BackendError, ProvisionedDatabase,
    jobs::{CleanupDatabase, CreateDatabases, CreationResult, DatabaseId, DatabaseWorkerMessages},
};

pub enum CreationAction {
    Begin(DatabaseId),
    Report(Vec<CreationResult>),
}
pub struct CreationState {
    queued: VecDeque<CreateDatabases>,
    current: Option<(CreateDatabases, usize)>,
    active: HashSet<DatabaseId>,
    completed: Vec<CreationResult>,
    limit: usize,
}
impl CreationState {
    pub fn new(limit: NonZeroUsize) -> Self {
        Self {
            queued: VecDeque::new(),
            current: None,
            active: HashSet::new(),
            completed: Vec::new(),
            limit: limit.get(),
        }
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
        self.completed.push(CreationResult { database_id: id, result });
        self.fill()
    }

    fn fill(&mut self) -> Vec<CreationAction> {
        let mut actions = Vec::new();
        loop {
            if self.current.as_ref().is_some_and(|(batch, next)| *next == batch.amount.get()) {
                if !self.active.is_empty() {
                    break;
                }
                // Publish exactly once for the complete CreateDatabases
                // request, including failures, before starting
                // the next queued batch.
                actions.push(CreationAction::Report(std::mem::take(&mut self.completed)));
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
        self.completed.clear();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request(first: u64, amount: usize) -> CreateDatabases {
        CreateDatabases {
            first_database_id: DatabaseId(first),
            amount: NonZeroUsize::new(amount).unwrap(),
        }
    }

    fn failed() -> Result<ProvisionedDatabase, BackendError> {
        Err(BackendError::OperationFailed("injected failure".into()))
    }

    #[test]
    fn creation_reports_once_per_request_after_all_results_and_before_next_batch() {
        let mut state = CreationState::new(NonZeroUsize::new(2).unwrap());
        assert!(matches!(
            state.enqueue(request(1, 3)).as_slice(),
            [CreationAction::Begin(DatabaseId(1)), CreationAction::Begin(DatabaseId(2)),]
        ));
        assert!(state.enqueue(request(4, 2)).is_empty());
        assert!(matches!(
            state.complete(DatabaseId(2), failed()).as_slice(),
            [CreationAction::Begin(DatabaseId(3)),]
        ));
        assert!(state.complete(DatabaseId(2), failed()).is_empty());
        assert!(state.complete(DatabaseId(999), failed()).is_empty());
        assert!(state.complete(DatabaseId(3), failed()).is_empty());
        let actions = state.complete(DatabaseId(1), failed());
        let [
            CreationAction::Report(results),
            CreationAction::Begin(DatabaseId(4)),
            CreationAction::Begin(DatabaseId(5)),
        ] = actions.as_slice()
        else {
            panic!("must report the completed batch before starting the next one")
        };
        assert_eq!(
            results.iter().map(|result| result.database_id).collect::<Vec<_>>(),
            [DatabaseId(2), DatabaseId(3), DatabaseId(1)]
        );
        assert!(results.iter().all(|result| result.result.is_err()));
        assert!(state.complete(DatabaseId(4), failed()).is_empty());
        let actions = state.complete(DatabaseId(5), failed());
        let [CreationAction::Report(results)] = actions.as_slice() else {
            panic!("must report exactly the second batch")
        };
        assert_eq!(
            results.iter().map(|result| result.database_id).collect::<Vec<_>>(),
            [DatabaseId(4), DatabaseId(5)]
        );
        assert!(state.is_idle());
    }

    #[test]
    fn stopping_discards_partial_batch_results() {
        let mut state = CreationState::new(NonZeroUsize::new(2).unwrap());
        state.enqueue(request(1, 2));
        assert!(state.complete(DatabaseId(1), failed()).is_empty());
        state.stop();
        assert!(state.is_idle());
        assert!(state.complete(DatabaseId(2), failed()).is_empty());
        state.enqueue(request(3, 1));
        let actions = state.complete(DatabaseId(3), failed());
        let [CreationAction::Report(results)] = actions.as_slice() else {
            panic!("must report only the new batch")
        };
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].database_id, DatabaseId(3));
    }
}
