// 99% of this files is the same as-is in https://github.com/testcontainers/testcontainers-rs-modules-community/blob/main/src/postgres/mod.rs.
// I have only adapted for my needs, the config deactivate all PG safeguards for
// data safety, non essential work, and persist our data in memory.
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};

use testcontainers::{
    Container, CopyDataSource, CopyToContainer, Image, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::SyncRunner,
};
use tokio_postgres::{Config, NoTls};

use crate::manager::config::PostgresUpstreamPort;
#[cfg(any(test, feature = "test-support"))]
use crate::manager::{config::PostgresConfig, database_name::PostgresDatabaseName};

const NAME: &str = "postgres";
const TAG: &str = "18-alpine";

pub static POSTGRES_CONTAINER: LazyLock<Container<Postgres>> = LazyLock::new(|| {
    let postgres = Postgres::default()
        .with_host_config_modifier(|config| {
            config.tmpfs.get_or_insert_default().insert(
                Postgres::mount_data_dir_post_pg_18(),
                "rw,noexec,nosuid,size=3g".to_owned(),
            );
        })
        .start()
        .unwrap();

    postgres
});

/// PostgreSQL test container based on the official [`Postgres docker image`].
///
/// The default database is `pgtest`; the user and password are `postgres`.
///
/// # Example
/// ```
/// use pgtest_database_operations::testcontainer::Postgres;
/// use testcontainers::runners::SyncRunner;
///
/// let postgres_instance = Postgres::default().start().unwrap();
///
/// let connection_string = format!(
///     "postgres://postgres:postgres@{}:{}/pgtest",
///     postgres_instance.get_host().unwrap(),
///     postgres_instance.get_host_port_ipv4(5432).unwrap()
/// );
/// ```
///
/// [`Postgres`]: https://www.postgresql.org/
/// [`Postgres docker image`]: https://hub.docker.com/_/postgres
#[derive(Debug, Clone)]
pub struct Postgres {
    env_vars: HashMap<String, String>,
    copy_to_sources: Vec<CopyToContainer>,
}

impl Postgres {
    pub fn with_db_name(mut self, db_name: &str) -> Self {
        self.env_vars.insert("POSTGRES_DB".to_owned(), db_name.to_owned());
        self
    }

    pub fn with_user(mut self, user: &str) -> Self {
        self.env_vars.insert("POSTGRES_USER".to_owned(), user.to_owned());
        self
    }

    pub fn with_password(mut self, password: &str) -> Self {
        self.env_vars.insert("POSTGRES_PASSWORD".to_owned(), password.to_owned());
        self
    }

    pub fn mount_data_dir_post_pg_18() -> String {
        "/var/lib/postgresql".to_owned()
    }

    pub fn mount_data_dir_pre_pg_18() -> String {
        "/var/lib/postgresql/data".to_owned()
    }

    /// Registers sql to be executed automatically when the container starts.
    /// Can be called multiple times to add (not override) scripts.
    ///
    /// # Example
    ///
    /// ```
    /// # use pgtest_database_operations::testcontainer::Postgres;
    /// let postgres_image = Postgres::default()
    ///     .with_init_sql("CREATE EXTENSION IF NOT EXISTS hstore;".to_string().into_bytes());
    /// ```
    ///
    /// ```rust,ignore
    /// # use pgtest_database_operations::testcontainer::Postgres;
    /// let postgres_image = Postgres::default()
    ///                                .with_init_sql(include_str!("path_to_init.sql").to_string().into_bytes());
    /// ```
    pub fn with_init_sql(mut self, init_sql: impl Into<CopyDataSource>) -> Self {
        let target =
            format!("/docker-entrypoint-initdb.d/init_{i}.sql", i = self.copy_to_sources.len());
        self.copy_to_sources.push(CopyToContainer::new(init_sql.into(), target));
        self
    }
}

impl Default for Postgres {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("POSTGRES_DB".to_owned(), "pgtest".to_owned());
        env_vars.insert("POSTGRES_USER".to_owned(), "postgres".to_owned());
        env_vars.insert("POSTGRES_PASSWORD".to_owned(), "postgres".to_owned());
        env_vars.insert("POSTGRES_HOST_AUTH_METHOD".to_owned(), "trust".to_owned());

        Self { env_vars, copy_to_sources: Vec::new() }
    }
}

impl Image for Postgres {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![
            WaitFor::message_on_stderr("database system is ready to accept connections"),
            WaitFor::message_on_stdout("database system is ready to accept connections"),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        &self.copy_to_sources
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec![
            "postgres",
            "-c",
            "file_copy_method=clone",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
            "-c",
            "wal_level=minimal",
            "-c",
            "max_wal_senders=0",
            "-c",
            "archive_mode=off",
            "-c",
            "summarize_wal=off",
            "-c",
            "autovacuum=off",
            "-c",
            "random_page_cost=1.1",
        ]
    }

    fn expose_ports(&self) -> &[testcontainers::core::ContainerPort] {
        &[testcontainers::core::ContainerPort::Tcp(5432)]
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            pgtest_pg_database: Default::default(),
            pgtest_pg_port: PostgresUpstreamPort::default(),
            pgtest_pg_user: Default::default(),
            pgtest_pg_host: "localhost".parse().unwrap(),
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::new(5).unwrap().into(),
            pgtest_pg_cleanup_pool_connection: std::num::NonZeroUsize::new(2).unwrap().into(),
        }
    }
}

impl<'a> From<&'a Container<Postgres>> for PostgresConfig {
    fn from(value: &'a Container<Postgres>) -> Self {
        Self {
            pgtest_pg_host: value.get_host().unwrap().to_string().parse().unwrap(),
            pgtest_pg_port: value
                .get_host_port_ipv4(ContainerPort::Tcp(5432))
                .unwrap()
                .try_into()
                .unwrap(),
            ..PostgresConfig::default()
        }
    }
}

/// Provision a separate template for each test manager in the shared container.
/// Templates and any remaining clones live until the container is removed.
pub async fn pg_container_config() -> PostgresConfig {
    static NEXT_TEMPLATE_ID: AtomicU64 = AtomicU64::new(0);

    // The synchronous container runner must not run inside a Tokio runtime.
    // https://github.com/tokio-rs/tokio/discussions/3857
    let mut config =
        tokio::task::spawn_blocking(|| PostgresConfig::from(&*POSTGRES_CONTAINER)).await.unwrap();
    let id = NEXT_TEMPLATE_ID.fetch_add(1, Ordering::Relaxed);

    config.pgtest_pg_database = format!("pgt{id:016x}").parse().unwrap();
    let (client, connection) = Config::new()
        .host(config.pgtest_pg_host.as_str())
        .port(config.pgtest_pg_port.port())
        .user(config.pgtest_pg_user.as_str())
        .password("postgres")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .expect("connect to the shared test container");
    let connection_task = tokio::spawn(connection);
    let template = PostgresDatabaseName::quote_ident(&config.pgtest_pg_database);
    client
        .execute_typed(
            &format!("CREATE DATABASE {template} TEMPLATE pgtest STRATEGY=FILE_COPY"),
            &[],
        )
        .await
        .expect("create an isolated test template");

    drop(client);
    connection_task
        .await
        .expect("join the template connection driver")
        .expect("close the template creation connection");
    config
}
