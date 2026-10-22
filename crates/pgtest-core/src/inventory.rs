use std::collections::VecDeque;

use pgtest_engine_backend::{
    BackendError, ProvisionedDatabase,
    jobs::{CreateDatabases, DatabaseId},
};
use rustc_hash::FxHashSet;

#[derive(Clone, Debug)]
pub struct Database {
    pub database_id: DatabaseId,
    pub resource: ProvisionedDatabase,
}

#[derive(Default, Clone, Debug)]
pub struct DatabaseInventory {
    next_database_id: u64,
    creating: FxHashSet<DatabaseId>,
    ready: VecDeque<Database>,
}

impl DatabaseInventory {
    /// Take ownership of databases created during bootstrap. Runtime growth
    /// continues allocating identities after this initial inventory.
    pub fn from_initial(databases: Vec<ProvisionedDatabase>) -> Self {
        let mut inventory = Self::default();
        if let Some(batch) = inventory.reserve_creations(databases.len()) {
            for (index, database) in databases.into_iter().enumerate() {
                inventory.complete_creation(batch.database_id(index), Ok(database));
            }
        }
        inventory
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

    pub(crate) fn return_ready(&mut self, database: Database) {
        self.ready.push_front(database);
    }

    pub fn supply_len(&self) -> usize {
        self.ready.len() + self.creating.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_inventory_is_ready_and_growth_uses_fresh_identities() {
        let databases = (0..2)
            .map(|index| ProvisionedDatabase {
                resource_id: pgtest_engine_backend::ResourceId(format!("bootstrap-{index}")),
                target: pgtest_engine_backend::PgTarget {
                    database: format!("bootstrap-{index}"),
                    endpoint: pgtest_engine_backend::PgEndpoint::Tcp {
                        host: "localhost".into(),
                        port: 5432,
                    },
                },
            })
            .collect();
        let mut inventory = DatabaseInventory::from_initial(databases);
        assert!(inventory.creating.is_empty());
        assert_eq!(inventory.supply_len(), 2);
        for id in 1..=2 {
            let database = inventory.take_ready().unwrap();
            assert_eq!(database.database_id, DatabaseId(id));
            assert_eq!(database.resource.target.database, format!("bootstrap-{}", id - 1));
        }
        assert!(inventory.take_ready().is_none());
        assert_eq!(inventory.reserve_creations(1).unwrap().first_database_id, DatabaseId(3));
    }

    #[test]
    fn reserves_ranges_without_reusing_failed_ids() {
        let mut inventory = DatabaseInventory::default();
        assert!(inventory.reserve_creations(0).is_none());
        let batch = inventory.reserve_creations(3).unwrap();
        assert_eq!(batch.first_database_id, DatabaseId(1));
        assert_eq!(inventory.creating.len(), 3);
        for index in 0..batch.amount.get() {
            assert!(
                inventory
                    .complete_creation(
                        batch.database_id(index),
                        Err(BackendError::OperationFailed("failed".into()))
                    )
                    .unwrap()
                    .is_err()
            );
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
