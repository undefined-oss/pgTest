pub mod core;
pub mod database_inventory;
pub mod database_jobs;
pub mod errors;
mod lease_id;
pub mod messages;
pub mod traits;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod burst_tests;

#[cfg(test)]
mod worker_engine_test {
    use std::{sync::Arc, time::Instant};

    use super::{
        core::{LeaseId, WorkerEngineConfig},
        database_jobs::DatabaseId,
        messages::{ConsumerReply, EngineMessage},
        test_support::*,
    };

    fn consumer() -> ConsumerWorker {
        ConsumerWorker::new(Arc::default())
    }

    fn attach(lease: &str, reply: &ConsumerWorker) -> EngineMessage<ConsumerWorker> {
        EngineMessage::AttachOrJoin {
            lease: LeaseId::new(lease).unwrap(),
            reply: reply.clone(),
            message_time: Instant::now(),
        }
    }

    fn occupy_initial(reply: &ConsumerWorker) -> Vec<EngineMessage<ConsumerWorker>> {
        (1..=4).map(|n| attach(&format!("connection{n}"), reply)).collect()
    }

    #[tokio::test]
    async fn simulations_restart_the_sequence_and_share_it_with_background_creation() {
        for _ in 0..2 {
            let consumer = consumer();
            let outcome = EngineSimulator::run(occupy_initial(&consumer)).await.unwrap();
            let mut databases: Vec<_> = outcome
                .leases
                .values()
                .map(|entry| &entry.database)
                .chain(outcome.inventory.ready().iter())
                .collect();
            databases.sort_by_key(|database| database.database_id.0);

            // Four initial clones plus one background growth batch share a
            // sequence.
            assert_eq!(databases.len(), 8);
            for (index, database) in databases.iter().enumerate() {
                assert_eq!(database.database_name.as_ref(), format!("pgtest_{}", index + 1));
            }
        }
    }

    #[tokio::test]
    async fn immediate_available_templates() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![attach("first", &consumer)]).await.unwrap();
        let messages = consumer.messages();
        let Some(ConsumerReply::Attached { database_name, cancellation, .. }) = messages.front()
        else {
            panic!("expected an attachment");
        };
        assert_eq!(outcome.leases["first"].database.database_name, *database_name);
        assert_eq!(outcome.leases["first"].database.database_id, DatabaseId(1));
        assert_eq!(outcome.inventory.ready().len(), 3);
        assert!(!cancellation.is_cancelled(), "pumping a fake inbox must not shut down sessions");
    }

    #[tokio::test]
    async fn starvation_replenishes_ready_supply() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![
            attach("first", &consumer),
            attach("second", &consumer),
            attach("third", &consumer),
        ])
        .await
        .unwrap();
        assert_eq!(outcome.leases.len(), 3);
        assert_eq!(outcome.inventory.ready().len(), 5);
        assert!(outcome.inventory.creating().is_empty());
    }

    #[tokio::test]
    async fn joining_a_lease_shares_its_database_and_generation() {
        let consumer = consumer();
        let outcome =
            EngineSimulator::run(vec![attach("shared", &consumer), attach("shared", &consumer)])
                .await
                .unwrap();
        let messages = consumer.messages();
        assert_eq!(messages.len(), 2);
        for message in messages {
            let ConsumerReply::Attached { database_name, generation, .. } = message else {
                panic!("expected an attachment");
            };
            assert_eq!(database_name, outcome.leases["shared"].database.database_name);
            assert_eq!(generation, outcome.leases["shared"].generation);
        }
        assert_eq!(outcome.leases.len(), 1);
        assert_eq!(outcome.leases["shared"].conns, 2);
        assert_eq!(outcome.inventory.ready().len(), 3);
        assert!(outcome.waiters.is_empty());
    }

    #[tokio::test]
    async fn detach_reduces_connections_without_returning_the_database() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![
            attach("shared", &consumer),
            attach("shared", &consumer),
            EngineMessage::Detach { lease: LeaseId::new("shared").unwrap(), generation: 1 },
        ])
        .await
        .unwrap();
        assert_eq!(outcome.leases["shared"].conns, 1);
        assert_eq!(outcome.leases["shared"].database.database_id, DatabaseId(1));
        assert_eq!(outcome.inventory.ready().len(), 3);
        assert!(outcome.inventory.retiring().is_empty());
        assert_eq!(outcome.counters.detach_on_zero, 0);
    }

    #[tokio::test]
    async fn detach_on_zero_connections_is_noop() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![
            attach("first", &consumer),
            EngineMessage::Detach { lease: LeaseId::new("first").unwrap(), generation: 1 },
            EngineMessage::Detach { lease: LeaseId::new("first").unwrap(), generation: 1 },
        ])
        .await
        .unwrap();
        assert_eq!(outcome.leases["first"].conns, 0);
        assert_eq!(outcome.leases["first"].database.database_id, DatabaseId(1));
        assert_eq!(outcome.inventory.ready().len(), 3);
        assert!(outcome.inventory.retiring().is_empty());
        assert_eq!(outcome.counters.detach_on_zero, 1);
    }

    #[tokio::test]
    async fn detach_unknown_lease_is_ignored() {
        let outcome = EngineSimulator::run(vec![EngineMessage::Detach {
            lease: LeaseId::new("ghost").unwrap(),
            generation: 1,
        }])
        .await
        .unwrap();
        assert!(outcome.leases.is_empty());
        assert_eq!(outcome.inventory.ready().len(), 4);
        assert_eq!(outcome.counters.detach_on_zero, 0);
    }

    #[tokio::test]
    async fn single_waiter_is_fulfilled_by_creation_completion() {
        let consumer = consumer();
        let mut messages = occupy_initial(&consumer);
        messages.push(attach("waiting", &consumer));
        let outcome = EngineSimulator::run(messages).await.unwrap();
        assert_eq!(outcome.leases["waiting"].database.database_id, DatabaseId(5));
        assert!(outcome.waiters.is_empty());
        assert_eq!(consumer.messages().len(), 5);
        assert!(matches!(consumer.messages().back(),
            Some(ConsumerReply::Attached { database_name, .. })
                if *database_name == outcome.leases["waiting"].database.database_name));
    }

    #[tokio::test]
    async fn second_waiter_is_served_when_first_times_out() {
        let consumer = consumer();
        let mut messages = occupy_initial(&consumer);
        messages.extend([
            EngineMessage::AttachOrJoin {
                lease: LeaseId::new("expired").unwrap(),
                reply: consumer.clone(),
                message_time: past_instant(),
            },
            attach("live", &consumer),
        ]);
        let outcome = EngineSimulator::run(messages).await.unwrap();
        assert!(!outcome.leases.contains_key("expired"));
        assert_eq!(outcome.leases["live"].database.database_id, DatabaseId(5));
        assert_eq!(outcome.counters.waiter_timeouts, 1);
        assert!(outcome.waiters.is_empty());
        assert_eq!(consumer.messages().len(), 5);
    }

    #[tokio::test]
    async fn all_waiters_timing_out_leaves_created_databases_ready() {
        let consumer = consumer();
        let mut messages = occupy_initial(&consumer);
        messages.extend(["expired_a", "expired_b"].map(|lease| EngineMessage::AttachOrJoin {
            lease: LeaseId::new(lease).unwrap(),
            reply: consumer.clone(),
            message_time: past_instant(),
        }));
        let outcome = EngineSimulator::run(messages).await.unwrap();
        assert_eq!(outcome.leases.len(), 4);
        assert_eq!(outcome.inventory.ready().len(), 4);
        assert!(outcome.inventory.ready().iter().any(|db| db.database_id == DatabaseId(5)));
        assert_eq!(outcome.counters.waiter_timeouts, 2);
        assert!(outcome.waiters.is_empty());
        assert_eq!(consumer.messages().len(), 4);
    }

    #[tokio::test]
    async fn expired_lease_can_attach_to_a_fresh_database() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![
            attach("test", &consumer),
            EngineMessage::LeaseMaxTimeReached {
                lease: LeaseId::new("test").unwrap(),
                generation: 1,
            },
            attach("test", &consumer),
        ])
        .await
        .unwrap();
        let messages = consumer.messages();
        let ConsumerReply::Attached {
            database_name: old_name,
            generation: old_generation,
            cancellation: old_cancel,
        } = &messages[0]
        else {
            panic!("expected original attachment");
        };
        let ConsumerReply::Attached {
            database_name: new_name,
            generation: new_generation,
            cancellation: new_cancel,
        } = &messages[1]
        else {
            panic!("expected fresh attachment");
        };
        assert_ne!(old_name, new_name);
        assert_ne!(old_generation, new_generation);
        assert!(old_cancel.is_cancelled());
        assert!(!new_cancel.is_cancelled());
        assert_eq!(outcome.leases["test"].database.database_name, *new_name);
        assert_eq!(outcome.counters.rejected_attach_max_lifetime, 1);
        assert!(outcome.inventory.retiring().is_empty());
    }

    #[tokio::test]
    async fn shutdown_cancels_sessions_and_discards_later_messages() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![
            attach("first", &consumer),
            EngineMessage::Shutdown,
            attach("second", &consumer),
            EngineMessage::Detach { lease: LeaseId::new("first").unwrap(), generation: 1 },
        ])
        .await
        .unwrap();
        assert_eq!(consumer.messages().len(), 1);
        assert_eq!(outcome.leases.len(), 1);
        assert_eq!(outcome.leases["first"].conns, 1);
        assert!(outcome.leases["first"].cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn transient_creation_failure_recovers_the_waiter() {
        let consumer = consumer();
        let outcome = EngineSimulator::run_with_failing_pg(
            vec![attach("waiting", &consumer)],
            1,
            WorkerEngineConfig {
                initial_slots: 0.into(),
                starvation_threshold: 0.into(),
                grow_batch_size: 1.into(),
                ..WorkerEngineConfig::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.counters.template_create_failures, 1);
        assert_eq!(outcome.leases["waiting"].database.database_id, DatabaseId(2));
        assert!(outcome.waiters.is_empty());
        assert_eq!(consumer.messages().len(), 1);
        assert_eq!(outcome.inventory.ready().len(), 1);
    }

    #[tokio::test]
    async fn duplicate_waiter_joins_and_unused_creations_stay_ready() {
        let consumer = consumer();
        let mut messages = occupy_initial(&consumer);
        messages.extend([attach("shared", &consumer), attach("shared", &consumer)]);
        let outcome = EngineSimulator::run(messages).await.unwrap();
        assert_eq!(outcome.leases["shared"].conns, 2);
        assert_eq!(outcome.leases["shared"].database.database_id, DatabaseId(5));
        assert_eq!(outcome.inventory.ready().len(), 3);
        assert!(outcome.inventory.ready().iter().any(|db| db.database_id == DatabaseId(6)));
        assert!(outcome.waiters.is_empty());
        assert_eq!(consumer.messages().len(), 6);
    }

    #[tokio::test]
    async fn growth_preserves_initial_databases_and_adds_a_full_batch() {
        let consumer = consumer();
        let outcome = EngineSimulator::run(vec![
            attach("first", &consumer),
            attach("second", &consumer),
            attach("third", &consumer),
        ])
        .await
        .unwrap();
        assert_eq!(
            outcome.inventory.ready().iter().map(|db| db.database_id).collect::<Vec<_>>(),
            (4..=8).map(DatabaseId).collect::<Vec<_>>()
        );
        assert!(outcome.inventory.creating().is_empty());
    }

    #[tokio::test]
    async fn failed_join_reply_does_not_count_a_connection() {
        let consumer = consumer();
        let failing = ConsumerWorker::failing(Arc::default());
        for queued in [false, true] {
            let mut messages = if queued { occupy_initial(&consumer) } else { vec![] };
            messages.extend([
                attach("shared", &consumer),
                attach("shared", &failing),
                attach("shared", &consumer),
            ]);
            let outcome = EngineSimulator::run(messages).await.unwrap();
            assert_eq!(outcome.leases["shared"].conns, 2);
            assert!(!outcome.leases["shared"].cancellation.is_cancelled());
            assert!(outcome.waiters.is_empty());
            assert!(outcome.inventory.retiring().is_empty());
        }
    }

    #[tokio::test]
    async fn failed_fresh_reply_returns_the_database_to_ready() {
        for queued in [false, true] {
            let consumer = consumer();
            let failing = ConsumerWorker::failing(Arc::default());
            let mut messages = if queued { occupy_initial(&consumer) } else { vec![] };
            messages.push(attach("unreachable", &failing));
            let outcome = EngineSimulator::run(messages).await.unwrap();
            assert!(!outcome.leases.contains_key("unreachable"));
            assert!(outcome.waiters.is_empty());
            assert!(outcome.inventory.retiring().is_empty());
            let unused_id = DatabaseId(if queued { 5 } else { 1 });
            assert!(outcome.inventory.ready().iter().any(|db| db.database_id == unused_id));
            assert_eq!(consumer.messages().len(), if queued { 4 } else { 0 });
        }
    }
}

#[cfg(test)]
mod grow_test {
    use super::{
        core::WorkerEngineConfig, database_jobs::DatabaseId, errors::IOError, test_support::*,
    };

    #[tokio::test]
    async fn later_growth_recovers_after_submission_failure_with_a_fresh_id() {
        let (mut worker, io) = run_grow(vec![Err(IOError::FailedToSendTheMessage), Ok(())]).await;
        assert!(worker.inventory.creating().is_empty());
        assert_eq!(worker.inventory.ready().len(), 1);
        assert_eq!(worker.counters.unable_to_start_database_slots, 1);
        assert_eq!(io.remaining(), 1, "submission failure must not retry inline");

        worker.grow();
        worker.process_messages().await;
        assert_eq!(
            worker.inventory.ready().iter().map(|db| db.database_id).collect::<Vec<_>>(),
            vec![DatabaseId(1), DatabaseId(3)]
        );
        assert_eq!(io.remaining(), 0);
    }

    #[tokio::test]
    async fn rejected_submission_stops_the_batch_and_clears_its_reservation() {
        let (worker, io) = run_grow(vec![
            Err(IOError::FailedToSendTheMessage),
            Err(IOError::FailedToSendTheMessage),
            Err(IOError::FailedToSendTheMessage),
        ])
        .await;
        assert!(worker.inventory.creating().is_empty());
        assert_eq!(worker.inventory.ready().len(), 1);
        assert_eq!(worker.counters.unable_to_start_database_slots, 1);
        assert_eq!(io.remaining(), 2, "one attempt per growth event");
    }

    #[tokio::test]
    async fn grow_creates_a_full_batch() {
        let (worker, io) = run_grow_with(
            WorkerEngineConfig {
                initial_slots: 1.into(),
                starvation_threshold: 2.into(),
                grow_batch_size: 2.into(),
                ..WorkerEngineConfig::default()
            },
            vec![Ok(())],
        )
        .await;
        assert_eq!(
            worker.inventory.ready().iter().map(|db| db.database_id).collect::<Vec<_>>(),
            vec![DatabaseId(1), DatabaseId(2), DatabaseId(3)]
        );
        assert!(worker.inventory.creating().is_empty());
        assert_eq!(io.remaining(), 0);
    }

    #[tokio::test]
    async fn growth_rounds_a_small_deficit_up_to_a_full_batch() {
        let (worker, io) = run_grow_with(
            WorkerEngineConfig {
                initial_slots: 3.into(),
                starvation_threshold: 4.into(),
                grow_batch_size: 4.into(),
                ..WorkerEngineConfig::default()
            },
            vec![Ok(())],
        )
        .await;
        assert_eq!(worker.inventory.ready().len(), 7);
        assert_eq!(worker.inventory.ready().back().unwrap().database_id, DatabaseId(7));
        assert!(worker.inventory.creating().is_empty());
        assert_eq!(io.remaining(), 0);
    }

    #[tokio::test]
    async fn growth_is_unnecessary_when_ready_supply_exceeds_the_threshold() {
        let (worker, io) = run_grow_with(
            WorkerEngineConfig {
                initial_slots: 4.into(),
                starvation_threshold: 3.into(),
                grow_batch_size: 4.into(),
                ..WorkerEngineConfig::default()
            },
            vec![],
        )
        .await;
        assert_eq!(worker.inventory.ready().len(), 4);
        assert!(worker.inventory.creating().is_empty());
        assert_eq!(io.remaining(), 0);
    }
}
