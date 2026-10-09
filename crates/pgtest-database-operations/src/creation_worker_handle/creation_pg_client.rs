use deadpool_postgres::{Client, Pool, PoolError};
#[cfg(any(test, feature = "test-support"))]
use futures_util::{StreamExt, stream};
use pgtest_engine_backend::{
    BackendError, DatabaseCreator, PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId,
};
use pgtest_utils::read_string::ReadString;

#[cfg(any(test, feature = "test-support"))]
use crate::config::CreationPoolSize;
use crate::{
    backend::PostgresMetadata,
    config::PostgresConfig,
    connection::{connect_pool, execute_ddl},
    database_name::PostgresDatabaseName,
    errors::{PostgresClientError, PostgresOperationsError},
    sql_profile,
};

pub struct CreationClient {
    pub(crate) pool: Pool,
    pub(crate) version: u8,
    pub(crate) template_database_name: PostgresDatabaseName,
    endpoint: PgEndpoint,
    #[cfg(any(test, feature = "test-support"))]
    creation_concurrency: CreationPoolSize,
}
#[hotpath::measure_all]
impl CreationClient {
    pub async fn connect(
        config: &PostgresConfig,
        metadata: PostgresMetadata,
    ) -> Result<Self, PostgresClientError> {
        let pool =
            connect_pool(config, config.pgtest_pg_creation_pool_connection.into(), "creation")
                .await?;
        let PostgresMetadata { version, template, endpoint } = metadata;
        Ok(Self {
            pool,
            version,
            template_database_name: PostgresDatabaseName::new(template),
            endpoint,
            #[cfg(any(test, feature = "test-support"))]
            creation_concurrency: config.pgtest_pg_creation_pool_connection,
        })
    }

    pub async fn create_ddl_database(&self) -> Result<ReadString, PostgresOperationsError> {
        let database_name = self.template_database_name.generate_database_name();
        let client = self
            .acquire_create_connection()
            .await
            .map_err(|error| Self::create_error(database_name.clone(), error))?;
        self.create_on_connection(&client, database_name).await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn create_ddl_databases(
        &self,
        amount: usize,
        mut on_result: impl FnMut(usize, Result<ReadString, PostgresOperationsError>) + Send,
    ) {
        if amount == 0 {
            return;
        }
        run_bounded(
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

    pub(crate) async fn create_on_connection(
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
        execute_ddl(client, &query, statement)
            .await
            .map_err(|error| Self::create_error(database_name.clone(), error.into()))?;
        Ok(ReadString::from(database_name))
    }

    pub(crate) async fn acquire_create_connection(&self) -> Result<Client, PoolError> {
        self.pool.get().await
    }
}
impl Drop for CreationClient {
    fn drop(&mut self) {
        self.pool.close();
    }
}
impl DatabaseCreator for CreationClient {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        let name = self
            .create_ddl_database()
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;
        Ok(ProvisionedDatabase {
            resource_id: ResourceId(name.to_string()),
            target: PgTarget { endpoint: self.endpoint.clone(), database: name.to_string() },
        })
    }
}

// The window bounds pending futures per batch. Each operation checks out its
// own connection, so the shared pool also bounds execution across batches.
#[cfg(any(test, feature = "test-support"))]
fn run_bounded<CreationFuture, CreationResult>(
    amount: usize,
    limit: CreationPoolSize,
    mut create: impl FnMut(usize) -> CreationFuture + Send,
    mut on_result: impl FnMut(usize, CreationResult) + Send,
) -> impl Future<Output = ()> + Send
where
    CreationFuture: Future<Output = CreationResult> + Send,
    CreationResult: Send,
{
    async move {
        let mut creates = stream::iter(0..amount)
            .map(|index| {
                let future = create(index);
                async move { (index, future.await) }
            })
            .buffer_unordered(limit.get());
        while let Some((index, result)) = creates.next().await {
            on_result(index, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, task::Poll};

    use futures_util::poll;
    use tokio::sync::oneshot;

    use super::run_bounded;
    use crate::{
        config::PostgresConfig,
        database_name::PostgresDatabaseName,
        errors::PostgresOperationsError,
        testcontainer::{TestClients, pg_container_config},
    };

    #[tokio::test]
    async fn window_refills_after_out_of_order_success_and_failure() {
        let started = Mutex::new(Vec::new());
        let finished = Mutex::new(Vec::new());
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..5).map(|_| oneshot::channel::<Result<(), &'static str>>()).unzip();
        let mut senders: Vec<_> = senders.into_iter().map(Some).collect();
        let mut receivers = receivers.into_iter();
        let batch = run_bounded(
            5,
            std::num::NonZeroUsize::new(2).unwrap().into(),
            |index| {
                started.lock().unwrap().push(index);
                let receiver = receivers.next().unwrap();
                async move { receiver.await.unwrap() }
            },
            |index, result| finished.lock().unwrap().push((index, result)),
        );
        tokio::pin!(batch);
        assert!(poll!(&mut batch).is_pending());
        assert_eq!(*started.lock().unwrap(), [0, 1]);

        for (index, result, expected_started) in
            [(1, Ok(()), 3), (2, Err("failed"), 4), (3, Ok(()), 5), (0, Ok(()), 5)]
        {
            senders[index].take().unwrap().send(result).unwrap();
            assert!(poll!(&mut batch).is_pending());
            assert_eq!(started.lock().unwrap().len(), expected_started);
            assert_eq!(finished.lock().unwrap().last(), Some(&(index, result)));
        }
        senders[4].take().unwrap().send(Ok(())).unwrap();
        assert_eq!(poll!(&mut batch), Poll::Ready(()));
        assert_eq!(
            finished.lock().unwrap().iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            [1, 2, 3, 0, 4]
        );
    }

    #[tokio::test]
    async fn cancellation_drops_active_futures_without_starting_remaining_work() {
        let mut senders = Vec::new();
        let batch = run_bounded(
            5,
            std::num::NonZeroUsize::new(2).unwrap().into(),
            |_| {
                let (sender, receiver) = oneshot::channel::<()>();
                senders.push(sender);
                async move { receiver.await }
            },
            |_, _| panic!("cancelled work must not report completion"),
        );
        let mut batch = Box::pin(batch);
        assert!(poll!(&mut batch).is_pending());
        drop(batch);
        assert_eq!(senders.len(), 2);
        assert!(senders.into_iter().all(|sender| sender.send(()).is_err()));
    }

    #[tokio::test]
    async fn bounded_creation_with_one_connection_and_quoted_names_recovers_from_errors() {
        let mut clients = TestClients::start(PostgresConfig {
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::MIN.into(),
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let template =
            format!("{}_\"Template", clients.creation.template_database_name.template_name());
        let client = clients.creation.acquire_create_connection().await.unwrap();
        client
            .execute_typed(
                &format!("CREATE DATABASE {}", PostgresDatabaseName::quote_ident(&template)),
                &[],
            )
            .await
            .unwrap();
        drop(client);
        clients.creation.template_database_name = PostgresDatabaseName::new(template.clone());
        let mut created = std::collections::BTreeMap::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            clients.creation.create_ddl_databases(33, |index, result| {
                assert!(created.insert(index, result.unwrap()).is_none());
                assert_eq!(clients.creation.pool.status().available, 1, "release before callback");
            }),
        )
        .await
        .unwrap();
        assert_eq!(created.keys().copied().collect::<Vec<_>>(), (0..33).collect::<Vec<_>>());
        let names: Vec<String> = created.values().map(ToString::to_string).collect();
        assert_eq!(names.iter().collect::<std::collections::HashSet<_>>().len(), names.len());
        let client = clients.creation.acquire_create_connection().await.unwrap();
        let count = client
            .query_one("SELECT count(*) FROM pg_database WHERE datname::text = ANY($1)", &[&names])
            .await
            .unwrap();
        assert_eq!(count.get::<_, i64>(0), names.len() as i64);
        let extra = clients.creation.template_database_name.generate_database_name();
        let (failure, success) = tokio::join!(
            clients.creation.create_on_connection(&client, names[0].clone()),
            clients.creation.create_on_connection(&client, extra),
        );
        assert!(matches!(
            failure,
            Err(crate::errors::PostgresOperationsError::UnableToCreateDatabase { .. })
        ));
        success.expect("failed CREATE must not skip the next request's Sync boundary");
        drop(client);
        clients.drop_ddl_templates_like().await.unwrap();
        clients.cleanup.drop_ddl_database(&template).await.unwrap();

        clients.creation.pool.close();
        clients
            .creation
            .create_ddl_databases(0, |_, _| panic!("zero amount must not execute"))
            .await;
        let mut failed = Vec::new();
        clients
            .creation
            .create_ddl_databases(3, |index, result| {
                assert!(matches!(
                    result,
                    Err(crate::errors::PostgresOperationsError::UnableToCreateDatabase { .. })
                ));
                failed.push(index);
            })
            .await;
        failed.sort_unstable();
        assert_eq!(failed, [0, 1, 2]);
    }

    #[tokio::test]
    async fn overlapping_creation_batches_and_single_create_share_pool_capacity() {
        let clients = TestClients::start(PostgresConfig {
            pgtest_pg_creation_pool_connection: std::num::NonZeroUsize::new(2).unwrap().into(),
            ..pg_container_config().await
        })
        .await
        .unwrap();
        let mut first = std::collections::BTreeMap::new();
        let mut second = std::collections::BTreeMap::new();
        let ((), (), single) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            tokio::join!(
                clients.creation.create_ddl_databases(7, |index, result| {
                    assert!(first.insert(index, result.unwrap()).is_none());
                    assert!(clients.creation.pool.status().size <= 2);
                }),
                clients.creation.create_ddl_databases(8, |index, result| {
                    assert!(second.insert(index, result.unwrap()).is_none());
                    assert!(clients.creation.pool.status().size <= 2);
                }),
                clients.creation.create_ddl_database(),
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
        assert_eq!(clients.creation.pool.status().size, 2);
        clients.drop_ddl_templates_like().await.unwrap();
    }

    #[tokio::test]
    async fn closed_pool_reports_creation_error() {
        let clients = TestClients::start(pg_container_config().await).await.unwrap();
        clients.creation.pool.close();
        assert!(matches!(
            clients.creation.create_ddl_database().await,
            Err(PostgresOperationsError::UnableToCreateDatabase {
                source: deadpool_postgres::PoolError::Closed,
                ..
            })
        ));
    }
}
