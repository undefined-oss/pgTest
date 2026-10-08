use std::num::NonZeroUsize;

use crate::{BackendError, ProvisionedDatabase, ResourceId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseId(pub u64);

#[derive(Debug, Clone, Copy)]
pub struct CreateDatabases {
    pub first_database_id: DatabaseId,
    pub amount: NonZeroUsize,
}

impl CreateDatabases {
    pub fn database_id(&self, index: usize) -> DatabaseId {
        assert!(index < self.amount.get(), "creation index outside reserved batch");
        DatabaseId(
            self.first_database_id
                .0
                .checked_add(u64::try_from(index).expect("database identity exhausted"))
                .expect("database identity exhausted"),
        )
    }
}

#[derive(Clone, Debug)]
pub struct CleanupDatabase {
    pub database_id: DatabaseId,
    pub resource_id: ResourceId,
}

#[derive(Clone, Debug)]
pub enum DatabaseWorkerMessages {
    CreationFinished { database_id: DatabaseId, result: Result<ProvisionedDatabase, BackendError> },
    CleanupFinished { database_id: DatabaseId, result: Result<(), BackendError> },
}
