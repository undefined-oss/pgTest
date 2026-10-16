use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use hotpath::{Format, HotpathGuardBuilder, Section};
use pgtest_database_operations::{
    cleanup_worker_handle::CleanupHandle, creation_worker_handle::CreationHandle,
};
use pgtest_engine_backend::{
    BackendError, DatabaseCleaner, DatabaseCreator, ProvisionedDatabase, ResourceId,
    jobs::{CleanupDatabase, CreateDatabases, DatabaseId, DatabaseWorkerMessages},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

struct Backend;

impl DatabaseCreator for Backend {
    async fn create_database(&self) -> Result<ProvisionedDatabase, BackendError> {
        Err(BackendError::OperationFailed("injected failure".into()))
    }
}

impl DatabaseCleaner for Backend {
    async fn delete_database(&self, _: ResourceId) -> Result<(), BackendError> {
        Ok(())
    }
}

// Own the process-wide collector in a separate test binary. No PostgreSQL is
// needed: count real worker requests and completions with an in-memory backend.
#[tokio::test]
async fn worker_queues_report_sent_and_received_message_counts() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("channels.json");
    let guard = HotpathGuardBuilder::new("channel_profiling")
        .format(Format::Json)
        .sections(vec![Section::Channels])
        .output_path(&output)
        .build();
    let (manager, mut inbox) = hotpath::channel!(
        tokio::sync::mpsc::unbounded_channel::<DatabaseWorkerMessages>(),
        label = "manager"
    );
    let backend = Arc::new(Backend);
    let tracker = TaskTracker::new();
    let shutdown = CancellationToken::new();
    let creation = CreationHandle::new_with_client(
        backend.clone(),
        NonZeroUsize::MIN,
        manager.clone(),
        tracker.clone(),
        shutdown.clone(),
    );
    let cleanup = CleanupHandle::new_with_client(
        backend,
        NonZeroUsize::MIN,
        manager.clone(),
        tracker.clone(),
        shutdown,
    );
    creation
        .create(CreateDatabases {
            first_database_id: DatabaseId(1),
            amount: NonZeroUsize::new(2).unwrap(),
        })
        .unwrap();
    creation
        .clone()
        .create(CreateDatabases { first_database_id: DatabaseId(3), amount: NonZeroUsize::MIN })
        .unwrap();
    cleanup
        .delete(CleanupDatabase {
            database_id: DatabaseId(4),
            resource_id: ResourceId("removed".into()),
        })
        .unwrap();
    let mut creations = 0;
    let mut cleanups = 0;
    let mut group_sizes = Vec::new();
    while creations < 3 || cleanups < 1 {
        match tokio::time::timeout(Duration::from_secs(3), inbox.recv()).await.unwrap().unwrap() {
            DatabaseWorkerMessages::CreationsFinished { results } => {
                assert!(results.iter().all(|result| result.result.is_err()));
                group_sizes.push(results.len());
                creations += results.len();
            }
            DatabaseWorkerMessages::CleanupFinished { result, .. } => {
                assert!(result.is_ok());
                cleanups += 1;
            }
        }
    }
    assert_eq!((creations, cleanups), (3, 1));
    assert_eq!(group_sizes, [2, 1]);
    drop((creation, cleanup));
    tracker.close();
    tokio::time::timeout(Duration::from_secs(3), tracker.wait()).await.unwrap();
    inbox.close();
    // Failed sends must not inflate the number of enqueued messages.
    assert!(
        manager
            .send(DatabaseWorkerMessages::CleanupFinished {
                database_id: DatabaseId(5),
                result: Ok(()),
            })
            .is_err()
    );
    drop((manager, inbox));
    drop(guard);

    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    let channels = report["channels"]["data"].as_array().unwrap();
    assert_eq!(channels.len(), 3);
    for (label, expected) in [("manager", 3), ("creation", 2), ("cleanup", 1)] {
        let channel = channels.iter().find(|channel| channel["label"] == label).unwrap();
        assert_eq!(channel["sent_count"], expected, "{label}: sent count");
        assert_eq!(channel["received_count"], expected, "{label}: received count");
        assert_eq!(channel["queue_size"], 0, "{label}: drained queue");
        assert!(channel["max_queue_size"].as_u64().unwrap() > 0);
        assert_eq!(channel["wrap"], true);
    }
}
