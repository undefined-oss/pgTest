use deadpool_postgres::PoolError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PostgresClientError {
    #[error("unable to connect to postgres at {0}")]
    UnableToConnectToPostgres(String),
    #[error("unable to fetch the postgres server version")]
    UnableToFetchPostgresVersion,
    #[error("unable to fetch the list of available databases")]
    UnableToFetchDatabaseList,
    #[error("database {0} does not exist")]
    DatabaseDoesNotExist(String),
    #[error("unsupported postgres version {0}; expected version 13 or higher")]
    UnsupportedVersion(u8),
    #[error("unexpected server version format: got {0}, expected an unsigned integer like 130_000")]
    UnexpectedServerVersionFormatFetched(i64),
}

#[derive(Error, Debug)]
pub enum PostgresOperationsError {
    #[error("unable to create database {database_name}: {source}")]
    UnableToCreateDatabase {
        database_name: String,
        #[source]
        source: PoolError,
    },
    #[error("unable to drop database {database_name}: {source}")]
    UnableToDropDatabase {
        database_name: String,
        #[source]
        source: PoolError,
    },
    #[error("unable to list databases: {0}")]
    UnableToListDatabases(#[source] PoolError),
}
