use std::{num::NonZeroUsize, time::Duration};

use deadpool_postgres::{Config, Pool, PoolConfig, Runtime, Timeouts};
use tokio_postgres::NoTls;

use super::{
    config::PostgresConfig,
    errors::{PostgresClientError, PostgresOperationsError},
    sql_profile,
};
pub(super) const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
impl From<&PostgresConfig> for Config {
    fn from(value: &PostgresConfig) -> Self {
        let mut config = Config::new();
        config.host = Some(value.pgtest_pg_host.clone().into());
        config.port = Some(*value.pgtest_pg_port);
        config.user = Some(value.pgtest_pg_user.clone().into());
        config.password = Some("postgres".into());
        config.dbname = Some("postgres".into());
        config
    }
}
pub(crate) async fn connect_pool(
    config: &PostgresConfig,
    max_connections: NonZeroUsize,
    purpose: &str,
) -> Result<Pool, PostgresClientError> {
    let connection_error = || {
        PostgresClientError::UnableToConnectToPostgres(format!(
            "postgres://{}@{}:{}/postgres",
            config.pgtest_pg_user, config.pgtest_pg_host, config.pgtest_pg_port,
        ))
    };
    let mut options = Config::from(config);
    options.application_name = Some(format!("pgtest-{purpose}"));
    let mut pool_config = PoolConfig::new(max_connections.get());
    pool_config.timeouts = Timeouts {
        wait: Some(CONNECTION_TIMEOUT),
        create: Some(CONNECTION_TIMEOUT),
        recycle: Some(CONNECTION_TIMEOUT),
    };
    options.pool = Some(pool_config);
    let pool = options.create_pool(Some(Runtime::Tokio1), NoTls).map_err(|error| {
        tracing::error!(%error, purpose, "unable to configure PostgreSQL pool");
        connection_error()
    })?;
    // Verify connectivity eagerly; Deadpool builds pools lazily.
    if let Err(error) = pool.get().await {
        tracing::error!(%error, purpose, "unable to connect PostgreSQL pool");
        pool.close();
        return Err(connection_error());
    }
    Ok(pool)
}
pub(crate) async fn execute_ddl(
    client: &tokio_postgres::Client,
    query: &str,
    statement: &str,
) -> Result<(), tokio_postgres::Error> {
    sql_profile::query(statement, client.execute_typed(query, &[])).await?;
    Ok(())
}
pub(crate) async fn discover_stale(
    client: &tokio_postgres::Client,
    template: &str,
) -> Result<Vec<String>, PostgresOperationsError> {
    let rows = sql_profile::query(
        sql_profile::LIST_DATABASES,
        client.query(sql_profile::LIST_DATABASES, &[&format!("{}_%", template)]),
    )
    .await
    .map_err(|error| PostgresOperationsError::UnableToListDatabases(error.into()))?;
    let names: Vec<String> = rows
        .iter()
        .map(|row| row.try_get(0))
        .collect::<Result<_, _>>()
        .map_err(|error| PostgresOperationsError::UnableToListDatabases(error.into()))?;

    Ok(names)
}
