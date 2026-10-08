use std::{num::NonZeroUsize, time::Duration};

use deadpool_postgres::{Client, Config, Pool, PoolConfig, PoolError, Runtime, Timeouts};
use futures_util::{StreamExt, future::join_all, stream::FuturesUnordered};
use pgtest_utils::read_string::ReadString;
use tokio_postgres::NoTls;

use crate::manager::{
    config::{CreationPoolSize, PostgresConfig, PostgresUpstreamPort},
    database_name::PostgresDatabaseName,
    errors::{PostgresClientError, PostgresOperationsError},
};

pub mod config;
mod creation;
pub mod database_name;
pub mod errors;
mod sql_profile;

pub struct PostgresManager {
    pub version: u8,
    pub host: String,
    pub port: PostgresUpstreamPort,
    pub template_database_name: PostgresDatabaseName,
    create_pool: Pool,
    creation_concurrency: CreationPoolSize,
    cleanup_pool: Pool,
}

const MIN_SERVER_VERSION_NUM: u8 = 13;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const CLEANUP_PIPELINE_DEPTH: usize = 32;

impl From<&PostgresConfig> for Config {
    fn from(value: &PostgresConfig) -> Self {
        let mut config = Config::new();
        config.host = Some(value.pgtest_pg_host.clone().into());
        config.port = Some(*value.pgtest_pg_port);
        config.user = Some(value.pgtest_pg_user.clone().into());
        config.password = Some(String::from("postgres"));
        config.dbname = Some(String::from("postgres"));
        config
    }
}

#[hotpath::measure_all]
impl PostgresManager {
    pub async fn start(config: PostgresConfig) -> Result<Self, PostgresClientError> {
        let create_pool = Self::connect_pool(
            &config,
            config.pgtest_pg_creation_pool_connection.into(),
            "creation",
        )
        .await?;
        let validation = async {
            Self::database_exists(&create_pool, &config.pgtest_pg_database).await?;
            Self::is_valid_version(&create_pool).await
        }
        .await;
        let version = match validation {
            Ok(version) => version,
            Err(error) => {
                create_pool.close();
                return Err(error);
            }
        };
        let cleanup_pool = match Self::connect_pool(
            &config,
            config.pgtest_pg_cleanup_pool_connection.into(),
            "cleanup",
        )
        .await
        {
            Ok(pool) => pool,
            Err(error) => {
                create_pool.close();
                return Err(error);
            }
        };

        Ok(Self {
            version,
            create_pool,
            creation_concurrency: config.pgtest_pg_creation_pool_connection,
            cleanup_pool,
            template_database_name: PostgresDatabaseName::new(config.pgtest_pg_database.into()),
            host: config.pgtest_pg_host.into(),
            port: config.pgtest_pg_port,
        })
    }

    async fn connect_pool(
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
        // Deadpool builds lazily. Verify both pools during startup, not on
        // first DDL.
        if let Err(error) = pool.get().await {
            tracing::error!(%error, purpose, "unable to connect PostgreSQL pool");
            pool.close();
            return Err(connection_error());
        }
        Ok(pool)
    }

    async fn database_exists(pool: &Pool, database_name: &str) -> Result<(), PostgresClientError> {
        let client =
            pool.get().await.map_err(|_| PostgresClientError::UnableToFetchDatabaseList)?;
        let rows = sql_profile::query(
            sql_profile::LIST_DATABASES,
            client.query(sql_profile::LIST_DATABASES, &[&database_name]),
        )
        .await
        .map_err(|_| PostgresClientError::UnableToFetchDatabaseList)?;
        if rows.is_empty() {
            return Err(PostgresClientError::DatabaseDoesNotExist(database_name.to_owned()));
        }
        Ok(())
    }

    async fn is_valid_version(pool: &Pool) -> Result<u8, PostgresClientError> {
        let client =
            pool.get().await.map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let row = sql_profile::query(
            sql_profile::SERVER_VERSION,
            client.query_one(sql_profile::SERVER_VERSION, &[]),
        )
        .await
        .map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let number: i64 =
            row.try_get(0).map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let version = u8::try_from(number / 10000)
            .map_err(|_| PostgresClientError::UnexpectedServerVersionFormatFetched(number))?;
        if version < MIN_SERVER_VERSION_NUM {
            return Err(PostgresClientError::UnsupportedVersion(version));
        }
        Ok(version)
    }

    pub async fn drop_ddl_database(
        &self,
        database_name: &str,
    ) -> Result<(), PostgresOperationsError> {
        let client = self
            .acquire_drop_connection()
            .await
            .map_err(|error| Self::drop_error(database_name, error))?;
        Self::drop_on_connection(&client, database_name).await
    }

    /// Pipeline one attempt for each name through a single cleanup connection.
    ///
    /// Reports each result immediately, using its index in `database_names`.
    /// An outer error means acquiring the connection failed and no drops were
    /// submitted. Individual failures do not cancel the other drops.
    pub async fn drop_ddl_databases(
        &self,
        database_names: &[&str],
        mut on_result: impl FnMut(usize, Result<(), PostgresOperationsError>) + Send,
    ) -> Result<(), PostgresOperationsError> {
        if database_names.is_empty() {
            return Ok(());
        }
        let client = self
            .acquire_drop_connection()
            .await
            .map_err(|error| Self::drop_error("cleanup batch", error))?;
        let mut names = database_names.iter().enumerate();
        let mut drops = FuturesUnordered::new();
        for (index, name) in names.by_ref().take(CLEANUP_PIPELINE_DEPTH) {
            drops.push(Self::drop_indexed(&client, index, name));
        }
        while let Some((index, result)) = drops.next().await {
            on_result(index, result);
            if let Some((index, name)) = names.next() {
                drops.push(Self::drop_indexed(&client, index, name));
            }
        }
        Ok(())
    }

    async fn drop_indexed(
        client: &tokio_postgres::Client,
        index: usize,
        database_name: &str,
    ) -> (usize, Result<(), PostgresOperationsError>) {
        (index, Self::drop_on_connection(client, database_name).await)
    }

    fn drop_error(database_name: &str, error: PoolError) -> PostgresOperationsError {
        tracing::warn!(database_name, %error, "PostgreSQL DROP DATABASE failed");
        PostgresOperationsError::UnableToDropDatabase {
            database_name: database_name.to_owned(),
            source: error,
        }
    }

    async fn drop_on_connection(
        client: &tokio_postgres::Client,
        database_name: &str,
    ) -> Result<(), PostgresOperationsError> {
        let quoted = PostgresDatabaseName::quote_ident(database_name);
        let query = format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)");
        Self::execute_ddl(client, &query, sql_profile::DROP_DATABASE)
            .await
            .map_err(|error| Self::drop_error(database_name, error.into()))?;
        Ok(())
    }

    pub async fn drop_ddl_templates_like(&self) -> Result<(), PostgresOperationsError> {
        let client = self
            .acquire_drop_connection()
            .await
            .map_err(|error| PostgresOperationsError::UnableToListDatabases(error))?;
        let rows = sql_profile::query(
            sql_profile::LIST_DATABASES,
            client.query(
                sql_profile::LIST_DATABASES,
                &[&format!("{}_%", self.template_database_name.template_name())],
            ),
        )
        .await
        .map_err(|error| PostgresOperationsError::UnableToListDatabases(error.into()))?;
        let names: Vec<String> = rows
            .iter()
            .map(|row| row.try_get(0))
            .collect::<Result<_, _>>()
            .map_err(|error| PostgresOperationsError::UnableToListDatabases(error.into()))?;

        for batch in names.chunks(CLEANUP_PIPELINE_DEPTH) {
            let results =
                join_all(batch.iter().map(|name| Self::drop_on_connection(&client, name))).await;
            for result in results {
                result?;
            }
        }
        Ok(())
    }

    pub async fn create_ddl_database(&self) -> Result<ReadString, PostgresOperationsError> {
        let database_name = self.template_database_name.generate_database_name();
        let client = self
            .acquire_create_connection()
            .await
            .map_err(|error| Self::create_error(database_name.clone(), error))?;
        self.create_on_connection(&client, database_name).await
    }

    /// Run one creation attempt per index with pool-bounded concurrency.
    /// Acquisition and execution failures are reported through `on_result`.
    pub async fn create_ddl_databases(
        &self,
        amount: usize,
        mut on_result: impl FnMut(usize, Result<ReadString, PostgresOperationsError>) + Send,
    ) {
        if amount == 0 {
            return;
        }
        creation::run_bounded(
            amount,
            self.creation_concurrency,
            |_| self.create_ddl_database(),
            &mut on_result,
        )
        .await;
    }

    fn create_error(database_name: String, error: PoolError) -> PostgresOperationsError {
        PostgresOperationsError::UnableToCreateDatabase { database_name, source: error }
    }

    async fn create_on_connection(
        &self,
        client: &tokio_postgres::Client,
        database_name: String,
    ) -> Result<ReadString, PostgresOperationsError> {
        let quoted_database = PostgresDatabaseName::quote_ident(&database_name);
        let quoted_template =
            PostgresDatabaseName::quote_ident(self.template_database_name.template_name());
        let mut query = format!("CREATE DATABASE {quoted_database} TEMPLATE {quoted_template}");
        let statement = if self.version >= 15 {
            query.push_str(" STRATEGY=FILE_COPY");
            sql_profile::CREATE_DATABASE_FILE_COPY
        } else {
            sql_profile::CREATE_DATABASE
        };
        Self::execute_ddl(client, &query, statement)
            .await
            .map_err(|error| Self::create_error(database_name.clone(), error.into()))?;
        Ok(ReadString::from(database_name))
    }

    async fn execute_ddl(
        client: &tokio_postgres::Client,
        query: &str,
        statement: &str,
    ) -> Result<(), tokio_postgres::Error> {
        sql_profile::query(statement, client.execute_typed(query, &[])).await?;
        Ok(())
    }

    async fn acquire_create_connection(&self) -> Result<Client, PoolError> {
        self.create_pool.get().await
    }

    async fn acquire_drop_connection(&self) -> Result<Client, PoolError> {
        self.cleanup_pool.get().await
    }
}

#[cfg(test)]
mod postgres_manager_test {
    use tokio::time::Instant;

    use super::{PostgresClientError, PostgresConfig, PostgresManager};
    use crate::testcontainer::pg_container_config;

    #[tokio::test]
    async fn cleanup_and_creation_have_independent_connection_capacity() {
        let config = PostgresConfig {
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::MIN.into(),
            pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::MIN.into(),
            ..pg_container_config().await
        };
        let manager = PostgresManager::start(config).await.unwrap();

        let cleanup_connection = manager.acquire_drop_connection().await.unwrap();
        let creation_connection = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            manager.acquire_create_connection(),
        )
        .await
        .expect("a full cleanup pool must not block creation")
        .unwrap();

        drop(cleanup_connection);
        let cleanup_connection = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            manager.acquire_drop_connection(),
        )
        .await
        .expect("a full creation pool must not block cleanup")
        .unwrap();

        drop(creation_connection);
        drop(cleanup_connection);
        manager.create_pool.close();
        manager.cleanup_pool.close();
    }

    #[tokio::test]
    async fn start_ok() {
        let manager = PostgresManager::start(pg_container_config().await).await.unwrap();

        assert_eq!(manager.version, 18);

        let now_create = Instant::now();
        let database_name = manager.create_ddl_database().await.unwrap();
        println!("Creating time {:.2?}", now_create.elapsed());

        let now_drop = Instant::now();
        manager.drop_ddl_database(&database_name).await.unwrap();
        println!("Drop time {:.2?}", now_drop.elapsed());

        // Dropping an already absent database remains idempotent.
        manager.drop_ddl_database(&database_name).await.unwrap();
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
            PostgresManager::start(config).await,
            Err(PostgresClientError::UnableToConnectToPostgres(_))
        ));
    }

    #[tokio::test]
    async fn pipelined_cleanup_works_with_one_connection_and_quoted_names() {
        let config = PostgresConfig {
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::MIN.into(),
            pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::MIN.into(),
            ..pg_container_config().await
        };
        let manager = PostgresManager::start(config).await.unwrap();
        let client = manager.acquire_create_connection().await.unwrap();
        let prefix = manager.template_database_name.template_name();
        // Cross the batch boundary and include names that require SQL quoting.
        let names: Vec<_> = (0..=super::CLEANUP_PIPELINE_DEPTH)
            .map(|index| format!("{prefix}_Mixed \"{index}"))
            .collect();
        let template = super::PostgresDatabaseName::quote_ident(prefix);
        let queries: Vec<_> = names
            .iter()
            .map(|name| {
                format!(
                    "CREATE DATABASE {} TEMPLATE {template}",
                    super::PostgresDatabaseName::quote_ident(name),
                )
            })
            .collect();
        for result in futures_util::future::join_all(
            queries.iter().map(|sql| PostgresManager::execute_ddl(&client, sql, sql)),
        )
        .await
        {
            result.expect("pipelined CREATE must run outside a transaction block");
        }
        drop(client);

        tokio::time::timeout(std::time::Duration::from_secs(30), manager.drop_ddl_templates_like())
            .await
            .expect("cleanup must not wait for a second pool connection")
            .expect("all pipelined drops must execute");
        let client = manager.acquire_drop_connection().await.unwrap();
        let row = client
            .query_one("SELECT count(*) FROM pg_database WHERE datname::text = ANY($1)", &[&names])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        let template_exists = client
            .query_one("SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)", &[&prefix])
            .await
            .unwrap();
        assert!(template_exists.get::<_, bool>(0));
    }

    #[tokio::test]
    async fn pipeline_failure_does_not_skip_later_commands_or_poison_connection() {
        let manager = PostgresManager::start(pg_container_config().await).await.unwrap();
        let database = manager.create_ddl_database().await.unwrap();
        let client = manager.acquire_drop_connection().await.unwrap();
        let (failure, success) = tokio::join!(
            PostgresManager::execute_ddl(&client, "SELECT 1 / 0", "SELECT 1 / 0"),
            PostgresManager::drop_on_connection(&client, &database),
        );
        assert_eq!(failure.unwrap_err().code().unwrap().code(), "22012");
        success.expect("a separate Sync must allow the next DROP to succeed");
        client.simple_query("SELECT 1").await.expect("connection must remain usable");
    }

    #[tokio::test]
    async fn cleanup_batch_reports_every_result_and_drains_failures_on_one_connection() {
        let manager = PostgresManager::start(PostgresConfig {
            pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::MIN.into(),
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let client = manager.acquire_create_connection().await.unwrap();
        let names: Vec<_> = (0..=super::CLEANUP_PIPELINE_DEPTH)
            .map(|index| {
                format!("{}_Batch \"{index}", manager.template_database_name.template_name())
            })
            .collect();
        for name in &names {
            PostgresManager::execute_ddl(
                &client,
                &format!("CREATE DATABASE {}", super::PostgresDatabaseName::quote_ident(name)),
                "CREATE DATABASE \"<database>\"",
            )
            .await
            .unwrap();
        }
        // Both errors are safe: PostgreSQL refuses to drop a template database
        // or the database to which this cleanup connection is connected.
        let mut batch = vec!["template0", "postgres"];
        batch.extend(names.iter().map(String::as_str));
        let mut results = std::collections::BTreeMap::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            manager.drop_ddl_databases(&batch, |index, result| {
                assert!(results.insert(index, result).is_none(), "duplicate result");
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(results.len(), batch.len());
        assert!(matches!(
            results.remove(&0).unwrap(),
            Err(super::errors::PostgresOperationsError::UnableToDropDatabase { .. })
        ));
        assert!(matches!(
            results.remove(&1).unwrap(),
            Err(super::errors::PostgresOperationsError::UnableToDropDatabase { .. })
        ));
        assert!(results.into_values().all(|result| result.is_ok()));
        let cleanup = manager.acquire_drop_connection().await.unwrap();
        let row = cleanup
            .query_one("SELECT count(*) FROM pg_database WHERE datname::text = ANY($1)", &[&names])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        drop(cleanup);

        manager.cleanup_pool.close();
        manager
            .drop_ddl_databases(&[], |_, _| panic!("empty batch must not report results"))
            .await
            .unwrap();
        assert!(matches!(
            manager.drop_ddl_databases(&["unused"], |_, _| panic!("no drops were submitted")).await,
            Err(super::errors::PostgresOperationsError::UnableToDropDatabase { .. })
        ));
    }

    #[tokio::test]
    async fn closed_pools_report_operation_errors() {
        use super::errors::PostgresOperationsError;

        let manager = PostgresManager::start(pg_container_config().await).await.unwrap();
        manager.create_pool.close();
        manager.cleanup_pool.close();
        assert!(matches!(
            manager.create_ddl_database().await,
            Err(PostgresOperationsError::UnableToCreateDatabase {
                source: deadpool_postgres::PoolError::Closed,
                ..
            })
        ));
        assert!(matches!(
            manager.drop_ddl_database("db").await,
            Err(PostgresOperationsError::UnableToDropDatabase {
                source: deadpool_postgres::PoolError::Closed,
                ..
            })
        ));
        assert!(manager.drop_ddl_templates_like().await.is_err());
    }

    #[tokio::test]
    async fn bounded_creation_with_one_connection_and_quoted_names_recovers_from_errors() {
        let mut manager = PostgresManager::start(PostgresConfig {
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::MIN.into(),
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let template = format!("{}_\"Template", manager.template_database_name.template_name());
        let client = manager.acquire_create_connection().await.unwrap();
        client
            .execute_typed(
                &format!("CREATE DATABASE {}", super::PostgresDatabaseName::quote_ident(&template)),
                &[],
            )
            .await
            .unwrap();
        drop(client);
        manager.template_database_name = super::PostgresDatabaseName::new(template.clone());
        let mut created = std::collections::BTreeMap::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            manager.create_ddl_databases(33, |index, result| {
                assert!(created.insert(index, result.unwrap()).is_none());
                assert_eq!(manager.create_pool.status().available, 1, "release before callback");
            }),
        )
        .await
        .unwrap();
        assert_eq!(created.keys().copied().collect::<Vec<_>>(), (0..33).collect::<Vec<_>>());
        let names: Vec<String> = created.values().map(ToString::to_string).collect();
        assert_eq!(names.iter().collect::<std::collections::HashSet<_>>().len(), names.len());
        let client = manager.acquire_create_connection().await.unwrap();
        let count = client
            .query_one("SELECT count(*) FROM pg_database WHERE datname::text = ANY($1)", &[&names])
            .await
            .unwrap();
        assert_eq!(count.get::<_, i64>(0), names.len() as i64);
        let extra = manager.template_database_name.generate_database_name();
        let (failure, success) = tokio::join!(
            manager.create_on_connection(&client, names[0].clone()),
            manager.create_on_connection(&client, extra),
        );
        assert!(matches!(
            failure,
            Err(super::errors::PostgresOperationsError::UnableToCreateDatabase { .. })
        ));
        success.expect("failed CREATE must not skip the next request's Sync boundary");
        drop(client);
        manager.drop_ddl_templates_like().await.unwrap();
        manager.drop_ddl_database(&template).await.unwrap();

        manager.create_pool.close();
        manager.create_ddl_databases(0, |_, _| panic!("zero amount must not execute")).await;
        let mut failed = Vec::new();
        manager
            .create_ddl_databases(3, |index, result| {
                assert!(matches!(
                    result,
                    Err(super::errors::PostgresOperationsError::UnableToCreateDatabase { .. })
                ));
                failed.push(index);
            })
            .await;
        failed.sort_unstable();
        assert_eq!(failed, [0, 1, 2]);
    }

    #[tokio::test]
    async fn overlapping_creation_batches_and_single_create_share_pool_capacity() {
        let manager = PostgresManager::start(PostgresConfig {
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::new(2).unwrap().into(),
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let mut first = std::collections::BTreeMap::new();
        let mut second = std::collections::BTreeMap::new();
        let ((), (), single) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            tokio::join!(
                manager.create_ddl_databases(7, |index, result| {
                    assert!(first.insert(index, result.unwrap()).is_none());
                    assert!(manager.create_pool.status().size <= 2);
                }),
                manager.create_ddl_databases(8, |index, result| {
                    assert!(second.insert(index, result.unwrap()).is_none());
                    assert!(manager.create_pool.status().size <= 2);
                }),
                manager.create_ddl_database(),
            )
        })
        .await
        .expect("overlapping batches must drain without holding connections while waiting");
        let single = single.unwrap();
        assert_eq!(first.keys().copied().collect::<Vec<_>>(), (0..7).collect::<Vec<_>>());
        assert_eq!(second.keys().copied().collect::<Vec<_>>(), (0..8).collect::<Vec<_>>());
        let names: std::collections::HashSet<_> =
            first.values().chain(second.values()).chain([&single]).collect();
        assert_eq!(names.len(), 16);
        assert_eq!(manager.create_pool.status().size, 2);
        manager.drop_ddl_templates_like().await.unwrap();
    }
}
