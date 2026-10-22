use tokio::sync::{mpsc, oneshot};

use super::*;

type Reply = oneshot::Sender<Result<ProvisionedDatabase, BackendError>>;
struct Creator(mpsc::UnboundedSender<Reply>);
impl DatabaseCreator for Creator {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        let (reply, result) = oneshot::channel();
        self.0.send(reply).unwrap();
        result.await.unwrap()
    }
}

fn database(name: &str) -> ProvisionedDatabase {
    ProvisionedDatabase {
        resource_id: pgtest_engine_backend::ResourceId(name.into()),
        target: pgtest_engine_backend::PgTarget {
            database: name.into(),
            endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
        },
    }
}

#[tokio::test]
async fn bootstrap_waits_for_the_full_batch_and_limits_concurrency() {
    for fail in [false, true] {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let (requests, mut creates) = mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                create_initial_databases(&Creator(requests), 3, NonZeroUsize::new(2).unwrap()).await
            });
            let slow = creates.recv().await.unwrap();
            let first = creates.recv().await.unwrap();
            assert!(creates.try_recv().is_err());
            first
                .send(if fail {
                    Err(BackendError::OperationFailed("injected".into()))
                } else {
                    Ok(database("first"))
                })
                .unwrap();
            creates.recv().await.unwrap().send(Ok(database("third"))).unwrap();
            assert!(!task.is_finished(), "even failure must wait for the complete batch");
            slow.send(Ok(database("slow"))).unwrap();
            let result = task.await.unwrap();
            if fail {
                assert_eq!(result.unwrap_err(), BackendError::OperationFailed("injected".into()));
            } else {
                let mut names: Vec<_> =
                    result.unwrap().into_iter().map(|db| db.target.database).collect();
                names.sort();
                assert_eq!(names, ["first", "slow", "third"]);
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn cancelling_bootstrap_drops_pending_operations() {
    let (requests, mut creates) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        create_initial_databases(&Creator(requests), 3, NonZeroUsize::MIN).await
    });
    let mut pending = tokio::time::timeout(std::time::Duration::from_secs(3), creates.recv())
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(3), pending.closed()).await.unwrap();
    assert!(creates.recv().await.is_none());
}

#[tokio::test]
async fn zero_initial_inventory_creates_no_databases() {
    let (requests, mut creates) = mpsc::unbounded_channel();
    assert!(
        create_initial_databases(&Creator(requests), 0, NonZeroUsize::MIN)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(creates.recv().await.is_none());
}
