//! PostgreSQL bootstrap and prepared configuration for runtime initialization.
use std::num::NonZeroUsize;

use deadpool_postgres::Client;
use pgtest_engine_backend::PgEndpoint;

use crate::{
    cleanup_worker_handle::CleanupClient,
    config::PostgresConfig,
    connection::{connect_pool, discover_stale},
    errors::PostgresClientError,
    sql_profile,
};

#[derive(Clone, Debug)]
pub struct PostgresMetadata {
    pub version: u8,
    pub template: String,
    pub endpoint: PgEndpoint,
}
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Postgres(#[from] PostgresClientError),
    #[error("bootstrap connection failed: {0}")]
    Connection(String),
}

struct BootstrapClient {
    client: Client,
}
#[hotpath::measure_all]
impl BootstrapClient {
    async fn connect(config: &PostgresConfig) -> Result<Self, BootstrapError> {
        let pool = connect_pool(config, NonZeroUsize::MIN, "bootstrap").await?;
        let client =
            pool.get().await.map_err(|error| BootstrapError::Connection(error.to_string()))?;
        // Keep only the checked-out connection; dropping it closes it instead
        // of returning it to an idle pool.
        pool.close();
        Ok(Self { client })
    }

    async fn validate(&self, template: &str) -> Result<u8, PostgresClientError> {
        let row = sql_profile::query(
            sql_profile::SERVER_VERSION,
            self.client.query_one(sql_profile::SERVER_VERSION, &[]),
        )
        .await
        .map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let number: i64 =
            row.try_get(0).map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let version = u8::try_from(number / 10000)
            .map_err(|_| PostgresClientError::UnexpectedServerVersionFormatFetched(number))?;
        if version < 13 {
            return Err(PostgresClientError::UnsupportedVersion(version));
        }
        let rows = sql_profile::query(
            sql_profile::LIST_DATABASES,
            self.client.query(sql_profile::LIST_DATABASES, &[&template]),
        )
        .await
        .map_err(|_| PostgresClientError::UnableToFetchDatabaseList)?;
        if !rows.iter().any(|row| row.get::<_, String>(0) == template) {
            return Err(PostgresClientError::DatabaseDoesNotExist(template.into()));
        }
        Ok(version)
    }

    async fn clean_stale(&self, template: &str) {
        let names = match discover_stale(&self.client, template).await {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(%error, "startup discovery failed; continuing startup");
                return;
            }
        };
        for name in names {
            if let Err(error) = CleanupClient::drop_on_connection(&self.client, &name).await {
                tracing::warn!(%error, database = %name, "startup cleanup failed; continuing startup");
                if self.client.is_closed() {
                    break;
                }
            }
        }
    }
}

/// Closes its only connection before returning metadata to worker constructors.
/// Cleanup is best effort; connection/template/version validation is mandatory.
pub async fn bootstrap(config: &PostgresConfig) -> Result<PostgresMetadata, BootstrapError> {
    let connection = BootstrapClient::connect(config).await?;
    let template = config.pgtest_pg_database.to_string();
    let version = connection.validate(&template).await?;
    connection.clean_stale(&template).await;
    drop(connection);
    let endpoint = if config.pgtest_pg_host.starts_with('/') {
        PgEndpoint::Unix {
            directory: config.pgtest_pg_host.to_string().into(),
            port: *config.pgtest_pg_port,
        }
    } else {
        PgEndpoint::Tcp { host: config.pgtest_pg_host.to_string(), port: *config.pgtest_pg_port }
    };
    Ok(PostgresMetadata { version, template, endpoint })
}

pub struct PreparedPostgres {
    pub config: PostgresConfig,
    pub metadata: PostgresMetadata,
}

impl PreparedPostgres {
    pub async fn prepare(config: PostgresConfig) -> Result<Self, BootstrapError> {
        let metadata = bootstrap(&config).await?;
        Ok(Self { config, metadata })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{database_name::PostgresDatabaseName, testcontainer::pg_container_config};

    async fn admin(config: &PostgresConfig) -> BootstrapClient {
        BootstrapClient::connect(config).await.unwrap()
    }
    fn quote(name: &str) -> String {
        PostgresDatabaseName::quote_ident(name)
    }

    #[tokio::test]
    async fn validation_and_cleanup_share_one_connection_and_close_it_before_pools() {
        let config = pg_container_config().await;
        let connection = admin(&config).await;
        let pid = connection
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get::<_, i32>(0);
        let template = config.pgtest_pg_database.to_string();
        let stale = format!("{template}_old");
        connection
            .client
            .execute_typed(&format!("CREATE DATABASE {}", quote(&stale)), &[])
            .await
            .unwrap();
        assert_eq!(connection.validate(&template).await.unwrap(), 18);
        connection.clean_stale(&template).await;
        let row = connection
            .client
            .query_one(
                "SELECT pg_backend_pid(), EXISTS(SELECT FROM pg_database WHERE datname = $1)",
                &[&stale],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), pid);
        assert!(!row.get::<_, bool>(1));
        drop(connection);
        let observer = admin(&config).await;
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let row = observer
                    .client
                    .query_one(
                        "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid = $1)",
                        &[&pid],
                    )
                    .await
                    .unwrap();
                if !row.get::<_, bool>(0) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(observer);
    }

    #[tokio::test]
    async fn bootstrap_cleanup_failure_warns_and_does_not_skip_remaining_databases() {
        let config = pg_container_config().await;
        let setup = admin(&config).await;
        let protected = format!("{}_protected", config.pgtest_pg_database);
        let ordinary = format!("{}_ordinary", config.pgtest_pg_database);
        for name in [&protected, &ordinary] {
            setup
                .client
                .execute_typed(&format!("CREATE DATABASE {}", quote(name)), &[])
                .await
                .unwrap();
        }
        setup
            .client
            .execute_typed(&format!("ALTER DATABASE {} IS_TEMPLATE true", quote(&protected)), &[])
            .await
            .unwrap();
        let metadata = bootstrap(&config).await.unwrap();
        assert_eq!(metadata.version, 18);
        let names: Vec<String> = setup
            .client
            .query("SELECT datname FROM pg_database", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect();
        assert!(names.contains(&protected));
        assert!(!names.contains(&ordinary));
        setup
            .client
            .execute_typed(&format!("ALTER DATABASE {} IS_TEMPLATE false", quote(&protected)), &[])
            .await
            .unwrap();
        setup
            .client
            .execute_typed(&format!("DROP DATABASE {}", quote(&protected)), &[])
            .await
            .unwrap();
        drop(setup);
    }

    #[tokio::test]
    async fn missing_template_fails_bootstrap() {
        let mut config = pg_container_config().await;
        config.pgtest_pg_database =
            format!("{}_missing", config.pgtest_pg_database).parse().unwrap();
        assert!(matches!(
            bootstrap(&config).await,
            Err(BootstrapError::Postgres(PostgresClientError::DatabaseDoesNotExist(_)))
        ));
    }

    #[tokio::test]
    async fn startup_connects_eagerly_and_reports_unreachable_postgres() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let config = PostgresConfig {
            pgtest_pg_host: "127.0.0.1".parse().unwrap(),
            pgtest_pg_port: port.try_into().unwrap(),
            ..PostgresConfig::default()
        };
        assert!(matches!(
            PreparedPostgres::prepare(config).await,
            Err(BootstrapError::Postgres(PostgresClientError::UnableToConnectToPostgres(_)))
        ));
    }
}
