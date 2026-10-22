use std::io;

use pgtest_database_operations::{backend::BootstrapError, errors::PostgresClientError};

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    #[error(transparent)]
    Postgres(#[from] PostgresClientError),
    #[error("lifecycle worker stopped during startup")]
    RuntimeStopped,
    #[error(transparent)]
    Thread(#[from] io::Error),
}
