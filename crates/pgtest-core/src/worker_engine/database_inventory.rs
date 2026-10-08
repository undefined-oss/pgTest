use std::collections::VecDeque;

use pgtest_engine_backend::{ProvisionedDatabase, ResourceId};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::worker_engine::{
    database_jobs::{CleanupDatabase, CreateDatabases, DatabaseId},
    errors::BackendError,
};

#[derive(Clone, Debug)]
pub struct Database {
    pub database_id: DatabaseId,
    pub resource: ProvisionedDatabase,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_ranges_without_reusing_cancelled_ids() {
        let mut inventory = DatabaseInventory::default();
        assert!(inventory.reserve_creations(0).is_none());
        let batch = inventory.reserve_creations(3).unwrap();
        assert_eq!(batch.first_database_id, DatabaseId(1));
        assert_eq!(inventory.creating.len(), 3);
        for index in 0..batch.amount.get() {
            assert!(inventory.cancel_creation(batch.database_id(index)));
        }
        let next = inventory.reserve_creations(2).unwrap();
        assert_eq!(next.first_database_id, DatabaseId(4));
        assert_eq!(next.database_id(1), DatabaseId(5));
    }

    #[test]
    fn overflowing_range_does_not_partially_reserve_ids() {
        let mut inventory =
            DatabaseInventory { next_database_id: u64::MAX - 1, ..DatabaseInventory::default() };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inventory.reserve_creations(2);
        }));
        assert!(result.is_err());
        assert!(inventory.creating.is_empty());
        assert_eq!(inventory.next_database_id, u64::MAX - 1);
        assert!(inventory.reserve_creations(0).is_none());
        assert_eq!(inventory.reserve_creations(1).unwrap().first_database_id, DatabaseId(u64::MAX));
    }
}

#[derive(Default, Clone, Debug)]
pub struct DatabaseInventory {
    next_database_id: u64,
    creating: FxHashSet<DatabaseId>,
    ready: VecDeque<Database>,
    retiring: FxHashMap<DatabaseId, ResourceId>,
}

impl DatabaseInventory {
    pub fn retire_resource(&mut self, resource_id: ResourceId) -> CleanupDatabase {
        let request = self.reserve_creations(1).expect("one identity");
        let database_id = request.first_database_id;
        self.creating.remove(&database_id);
        self.retiring.insert(database_id, resource_id.clone());
        CleanupDatabase { database_id, resource_id }
    }

    pub fn reserve_creations(&mut self, amount: usize) -> Option<CreateDatabases> {
        let amount = std::num::NonZeroUsize::new(amount)?;
        let last = self
            .next_database_id
            .checked_add(u64::try_from(amount.get()).expect("database identity exhausted"))
            .expect("database identity exhausted");
        let request =
            CreateDatabases { first_database_id: DatabaseId(self.next_database_id + 1), amount };
        self.creating.extend((0..amount.get()).map(|index| request.database_id(index)));
        self.next_database_id = last;
        Some(request)
    }

    /// Release a reservation when its creation request cannot be submitted.
    pub fn cancel_creation(&mut self, database_id: DatabaseId) -> bool {
        self.creating.remove(&database_id)
    }

    /// Settle a reserved creation. Unknown or repeated completions are ignored.
    /// A failed creation frees its reservation without contributing ready
    /// supply.
    pub fn complete_creation(
        &mut self,
        database_id: DatabaseId,
        result: Result<ProvisionedDatabase, BackendError>,
    ) -> Option<Result<(), BackendError>> {
        if !self.creating.remove(&database_id) {
            return None;
        }
        Some(result.map(|resource| {
            self.ready.push_back(Database { database_id, resource });
        }))
    }

    pub fn take_ready(&mut self) -> Option<Database> {
        self.ready.pop_front()
    }

    /// Restore a checked-out database to its original place ahead of unused
    /// supply.
    pub fn return_ready(&mut self, database: Database) {
        debug_assert!(!self.contains(database.database_id));
        self.ready.push_front(database);
    }

    /// Record retirement before the caller submits cleanup, so a submission
    /// failure cannot lose the database's identity.
    pub fn retire(&mut self, database: Database) -> CleanupDatabase {
        debug_assert!(!self.contains(database.database_id));
        let Database { database_id, resource } = database;
        self.retiring.insert(database_id, resource.resource_id.clone());
        CleanupDatabase { database_id, resource_id: resource.resource_id }
    }

    /// Forget a retired database only after successful cleanup. Unknown or
    /// repeated completions are ignored, and failures retain the record.
    pub fn complete_cleanup(
        &mut self,
        database_id: DatabaseId,
        result: Result<(), BackendError>,
    ) -> Option<Result<(), BackendError>> {
        if !self.retiring.contains_key(&database_id) {
            return None;
        }
        Some(result.map(|()| {
            self.retiring.remove(&database_id);
        }))
    }

    pub fn creating(&self) -> &FxHashSet<DatabaseId> {
        &self.creating
    }

    pub fn ready(&self) -> &VecDeque<Database> {
        &self.ready
    }

    pub fn retiring(&self) -> &FxHashMap<DatabaseId, ResourceId> {
        &self.retiring
    }

    pub fn supply_len(&self) -> usize {
        self.ready.len() + self.creating.len()
    }

    fn contains(&self, database_id: DatabaseId) -> bool {
        self.creating.contains(&database_id)
            || self.ready.iter().any(|database| database.database_id == database_id)
            || self.retiring.contains_key(&database_id)
    }
}
