//! Bridge tokio-postgres completions to Hotpath's SQL collector.
//!
//! The collector re-exported as `sql_tracing_layer` consumes completion events.
//! Keep its required target and fields here; no database driver adapter is
//! needed in the applications.

pub(crate) const LIST_DATABASES: &str = "SELECT datname FROM pg_database WHERE datname LIKE $1";
pub(crate) const SERVER_VERSION: &str = "SELECT current_setting('server_version_num')::int8";
pub(crate) const CREATE_DATABASE: &str = "CREATE DATABASE \"<database>\" TEMPLATE \"<template>\"";
pub(crate) const CREATE_DATABASE_FILE_COPY: &str =
    "CREATE DATABASE \"<database>\" TEMPLATE \"<template>\" STRATEGY=FILE_COPY";
pub(crate) const DROP_DATABASE: &str = "DROP DATABASE IF EXISTS \"<database>\" WITH (FORCE)";

/// Measure only the driver future, excluding pool acquisition.
/// Labels omit generated identifiers so repeated DDL shares one SQL bucket.
pub(crate) fn query<T>(
    statement: &str,
    query: impl Future<Output = Result<T, tokio_postgres::Error>>,
) -> impl Future<Output = Result<T, tokio_postgres::Error>> {
    #[cfg(feature = "hotpath")]
    {
        // Box before constructing the wrapper future: otherwise its inline
        // driver state multiplies through Hotpath's nested measured futures
        // and can overflow the startup stack in profiling builds.
        let query = Box::pin(query);
        async move {
            let started = std::time::Instant::now();
            let result = query.await;
            tracing::debug!(
                target: "sqlx::query",
                {
                    db.statement = statement,
                    db.system = "postgresql",
                    db.driver = "tokio-postgres",
                    elapsed_secs = started.elapsed().as_secs_f64(),
                    success = result.is_ok(),
                    sqlstate = result.as_ref().err().and_then(tokio_postgres::Error::code)
                        .map_or("", tokio_postgres::error::SqlState::code),
                },
                "PostgreSQL query completed"
            );
            result
        }
    }
    #[cfg(not(feature = "hotpath"))]
    {
        let _ = statement;
        query
    }
}
