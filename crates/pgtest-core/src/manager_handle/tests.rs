//! Behavioral tests run without an executor and with explicitly ordered
//! effects.
use std::{num::NonZeroUsize, time::Duration};

use pgtest_engine_backend::{
    BackendError, PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId,
    jobs::{DatabaseId, DatabaseWorkerMessages},
};

use super::{
    errors::{AttachError, ReleaseError},
    messages::{ConsumerReply, ManagerMessage},
};
use crate::{
    config::ManagerConfig,
    simulation::{ActorId, ReplySlot, RequestId, SimRuntime},
};
fn db(id: DatabaseId) -> ProvisionedDatabase {
    ProvisionedDatabase {
        resource_id: ResourceId(format!("provider/{}", id.0)),
        target: PgTarget {
            database: format!("db_{}", id.0),
            endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
        },
    }
}
fn pump(runtime: &mut SimRuntime) {
    runtime.run_until_idle(10_000).unwrap();
}
fn finish_creations(runtime: &mut SimRuntime) {
    for _ in 0..1000 {
        pump(runtime);
        let ids = runtime.active_creations();
        if ids.is_empty() {
            return;
        }
        for id in ids {
            runtime.complete_creation(id, Ok(db(id)));
        }
    }
    panic!("creation did not quiesce");
}
fn ready(config: ManagerConfig) -> SimRuntime {
    let mut runtime = SimRuntime::new(config);
    finish_creations(&mut runtime);
    assert!(matches!(runtime.startup_result(), Some(Ok(()))));
    runtime
}
fn config(initial: u16, threshold: u16, batch: u16) -> ManagerConfig {
    ManagerConfig {
        initial_slots: initial.into(),
        starvation_threshold: threshold.into(),
        grow_batch_size: batch.into(),
        ..ManagerConfig::default()
    }
}
fn attached(runtime: &SimRuntime, request: RequestId) -> (DatabaseId, super::lease::LeaseKey) {
    match runtime.reply(request) {
        ReplySlot::Delivered(ConsumerReply::Attached { database_id, key, .. }) => {
            (database_id, key)
        }
        other => panic!("expected attachment: {other:?}"),
    }
}
fn failure() -> BackendError {
    BackendError::OperationFailed("injected failure".into())
}

#[test]
fn startup_does_not_prefill_beyond_initial_size() {
    let r = ready(config(1, 8, 16));
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
}
#[test]
fn joining_a_lease_shares_database_and_generation() {
    let mut r = ready(config(4, 0, 0));
    let a = r.attach("shared");
    let b = r.attach("shared");
    pump(&mut r);
    assert_eq!(attached(&r, a), attached(&r, b));
    assert_eq!(r.snapshot().leases().len(), 1);
    assert_eq!(r.snapshot().inventory.ready().len(), 3);
}
#[test]
fn last_disconnect_keeps_database_for_reconnect() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("same");
    pump(&mut r);
    let old = attached(&r, a);
    r.drop_session(a);
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["same"].database.database_id, old.0);
    let b = r.attach("same");
    pump(&mut r);
    assert_eq!(attached(&r, b), old);
}
#[test]
fn release_allows_reusing_id_while_cleanup_is_pending() {
    let mut r = ready(config(2, 0, 0));
    let a = r.attach("a");
    pump(&mut r);
    let (old_database, key) = attached(&r, a);
    let release = r.release("a");
    let duplicate_release = r.release("a");
    let b = r.attach("a");
    pump(&mut r);
    assert!(matches!(r.reply(release), ReplySlot::Delivered(ConsumerReply::ReleaseResult(Ok(())))));
    assert!(matches!(
        r.reply(duplicate_release),
        ReplySlot::Delivered(ConsumerReply::ReleaseResult(Ok(())))
    ));
    assert!(r.session_cancelled(&key));
    let (new_database, new_key) = attached(&r, b);
    assert_ne!(old_database, new_database);
    assert_ne!(key.generation, new_key.generation);
    assert!(!r.session_cancelled(&new_key));
    assert_eq!(r.active_cleanups().len(), 1);
    assert_eq!(r.snapshot().inventory.retiring().len(), 1);
}
#[test]
fn releasing_unseen_id_is_a_noop() {
    let mut r = ready(config(1, 0, 0));
    r.release("unseen");
    r.release("unseen");
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
    assert!(r.active_cleanups().is_empty());
    let a = r.attach("unseen");
    pump(&mut r);
    attached(&r, a);
}
#[test]
fn release_without_assignment_leaves_waiters_pending() {
    let mut r = ready(config(0, 0, 1));
    let a = r.attach("a");
    let b = r.attach("a");
    pump(&mut r);
    let release = r.release("a");
    pump(&mut r);
    assert!(matches!(r.reply(release), ReplySlot::Delivered(ConsumerReply::ReleaseResult(Ok(())))));
    for id in [a, b] {
        assert!(matches!(r.reply(id), ReplySlot::Pending));
    }
    finish_creations(&mut r);
    assert_eq!(attached(&r, a), attached(&r, b));
    assert!(r.snapshot().waiters().is_empty());
    assert!(r.active_cleanups().is_empty());
}
#[test]
fn lost_release_reply_does_not_undo_removal_or_cleanup() {
    let mut r = ready(config(1, 0, 0));
    r.attach("a");
    pump(&mut r);
    let reply = r.release("a");
    r.cancel_request(reply);
    pump(&mut r);
    assert!(r.snapshot().leases().is_empty());
    assert_eq!(r.active_cleanups().len(), 1);
}
#[test]
fn virtual_expiry_and_old_generation_events_preserve_new_assignment() {
    let mut r = ready(config(2, 0, 0));
    let a = r.attach("a");
    pump(&mut r);
    let (_, old) = attached(&r, a);
    r.advance_by(Duration::from_secs(30));
    pump(&mut r);
    assert!(r.session_cancelled(&old));
    let b = r.attach("a");
    pump(&mut r);
    let (_, new) = attached(&r, b);
    assert_ne!(old, new);
    r.inject(ManagerMessage::LeaseMaxTimeReached { lease: old.lease, generation: old.generation });
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["a"].generation, new.generation);
    assert!(!r.session_cancelled(&new));
    assert_eq!(r.active_cleanups().len(), 1);
}
#[test]
fn duplicate_expiry_schedules_cleanup_only_once() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("a");
    pump(&mut r);
    let (_, key) = attached(&r, a);
    for _ in 0..2 {
        r.inject(ManagerMessage::LeaseMaxTimeReached {
            lease: key.lease.clone(),
            generation: key.generation,
        });
    }
    pump(&mut r);
    assert_eq!(r.active_cleanups().len(), 1);
}
#[test]
fn zero_claim_timeout_disables_lifetime_expiry() {
    let mut cfg = config(1, 0, 0);
    cfg.lease_claim_timeout_ms = 0;
    let mut r = ready(cfg);
    r.attach("a");
    pump(&mut r);
    r.advance_by(Duration::from_secs(1_000_000));
    pump(&mut r);
    assert!(r.snapshot().leases().contains_key("a"));
}
#[test]
fn covered_burst_does_not_schedule_redundant_batches() {
    let mut r = ready(config(0, 0, 4));
    for id in ["a", "b", "c"] {
        r.attach(id);
    }
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.creating().len(), 4);
    finish_creations(&mut r);
    assert_eq!(r.snapshot().leases().len(), 3);
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
}
#[test]
fn excess_demand_grows_in_full_batches_before_completion() {
    let mut r = ready(config(0, 0, 4));
    for id in 0..8 {
        r.attach(&id.to_string());
    }
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.creating().len(), 12);
}
#[test]
fn connections_for_one_lease_share_demand_and_completion() {
    let mut r = ready(config(0, 0, 1));
    let requests: Vec<_> = (0..10).map(|_| r.attach("a")).collect();
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.creating().len(), 2);
    let id = r.active_creations()[0];
    r.complete_creation(id, Ok(db(id)));
    pump(&mut r);
    for request in requests {
        assert_eq!(attached(&r, request).0, id);
    }
}
#[test]
fn one_creation_serves_only_one_distinct_waiting_lease() {
    let mut r = ready(config(0, 0, 1));
    let a = r.attach("a");
    let b = r.attach("b");
    pump(&mut r);
    let id = r.active_creations()[0];
    r.complete_creation(id, Ok(db(id)));
    pump(&mut r);
    attached(&r, a);
    assert!(matches!(r.reply(b), ReplySlot::Pending));
}
#[test]
fn failed_creation_replenishes_without_reusing_identity() {
    let mut r = ready(config(0, 0, 1));
    let a = r.attach("a");
    pump(&mut r);
    let id = r.active_creations()[0];
    r.complete_creation(id, Err(failure()));
    pump(&mut r);
    finish_creations(&mut r);
    assert_ne!(attached(&r, a).0, id);
}
#[test]
fn failed_and_cancelled_replies_restore_fresh_supply() {
    for cancel_after_delivery in [false, true] {
        let mut r = ready(config(1, 0, 0));
        let a = r.attach("a");
        if cancel_after_delivery {
            pump(&mut r);
        }
        r.cancel_request(a);
        pump(&mut r);
        if cancel_after_delivery {
            assert!(r.snapshot().leases().contains_key("a"));
            assert!(r.snapshot().inventory.ready().is_empty());
        } else {
            assert!(r.snapshot().leases().is_empty());
            assert_eq!(r.snapshot().inventory.ready().len(), 1);
        }
    }
}
#[test]
fn cancelled_join_preserves_existing_assignment() {
    let mut r = ready(config(1, 0, 0));
    let first = r.attach("a");
    pump(&mut r);
    let (database_id, key) = attached(&r, first);
    let b = r.attach("a");
    r.cancel_request(b);
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["a"].database.database_id, database_id);
    assert_eq!(r.snapshot().leases()["a"].generation, key.generation);
    assert!(!r.session_cancelled(&key));
    assert!(r.snapshot().inventory.ready().is_empty());
}
#[test]
fn cancelled_shared_waiter_preserves_assignment_for_all_live_replies() {
    for cancelled_index in 0..3 {
        let mut r = ready(config(0, 0, 1));
        let requests = [r.attach("shared"), r.attach("shared"), r.attach("shared")];
        pump(&mut r);
        r.cancel_request(requests[cancelled_index]);
        let database_id = r.active_creations()[0];
        r.complete_creation(database_id, Ok(db(database_id)));
        pump(&mut r);

        let live: Vec<_> = requests
            .into_iter()
            .enumerate()
            .filter(|(index, _)| *index != cancelled_index)
            .map(|(_, request)| attached(&r, request))
            .collect();
        assert_eq!(live[0], live[1]);
        assert_eq!(live[0].0, database_id);
        assert_eq!(r.snapshot().leases()["shared"].database.database_id, database_id);
        assert!(!r.session_cancelled(&live[0].1));
        assert!(r.snapshot().inventory.ready().iter().all(|db| db.database_id != database_id));
    }
}

#[test]
fn cancelled_waiter_does_not_block_live_waiter() {
    let mut r = ready(config(0, 0, 1));
    let a = r.attach("a");
    let b = r.attach("b");
    r.cancel_request(a);
    pump(&mut r);
    finish_creations(&mut r);
    assert!(!r.snapshot().leases().contains_key("a"));
    attached(&r, b);
}
#[test]
fn expired_waiters_do_not_consume_supply() {
    let mut r = ready(config(0, 0, 1));
    let request = r.attach("a");
    pump(&mut r);
    r.advance_by(Duration::from_secs(31));
    finish_creations(&mut r);
    assert!(r.snapshot().leases().is_empty());
    assert!(r.snapshot().waiters().is_empty());
    assert!(matches!(
        r.reply(request),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::TimedOut))
    ));
}
#[test]
fn unknown_and_duplicate_creation_results_cannot_supply_database() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("a");
    pump(&mut r);
    let (id, _) = attached(&r, a);
    for database_id in [id, DatabaseId(999)] {
        r.inject(ManagerMessage::DatabaseWorker(DatabaseWorkerMessages::CreationFinished {
            database_id,
            result: Ok(db(database_id)),
        }));
    }
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.ready().len(), 0);
    assert_eq!(r.snapshot().leases().len(), 1);
}
#[test]
fn cleanup_failure_retains_identity_and_never_restores_supply() {
    let mut r = ready(config(1, 0, 0));
    r.attach("a");
    r.release("a");
    pump(&mut r);
    let job = r.active_cleanups()[0].clone();
    assert!(job.resource_id.0.starts_with("provider/"));
    r.complete_cleanup(job.database_id, Err(failure()));
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.retiring().len(), 1);
    assert!(r.snapshot().inventory.ready().is_empty());
}
#[test]
fn cleanup_completion_cannot_remove_ready_or_assigned_resource() {
    let mut r = ready(config(2, 0, 0));
    r.attach("a");
    pump(&mut r);
    for id in [1, 2, 99] {
        r.inject(ManagerMessage::DatabaseWorker(DatabaseWorkerMessages::CleanupFinished {
            database_id: DatabaseId(id),
            result: Ok(()),
        }));
    }
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
    assert_eq!(r.snapshot().leases().len(), 1);
}
#[test]
fn creation_window_refills_and_maps_out_of_order_results() {
    let mut r =
        SimRuntime::with_options(config(5, 0, 0), NonZeroUsize::new(2).unwrap(), NonZeroUsize::MIN);
    pump(&mut r);
    assert_eq!(r.active_creations(), vec![DatabaseId(1), DatabaseId(2)]);
    r.complete_creation(DatabaseId(2), Ok(db(DatabaseId(2))));
    pump(&mut r);
    assert_eq!(r.active_creations(), vec![DatabaseId(1), DatabaseId(3)]);
    finish_creations(&mut r);
    assert_eq!(r.snapshot().inventory.ready().len(), 5);
}
#[test]
fn startup_failure_stops_without_waiting_for_remaining_results() {
    let mut r = SimRuntime::new(config(3, 0, 0));
    pump(&mut r);
    r.complete_creation(DatabaseId(1), Ok(db(DatabaseId(1))));
    pump(&mut r);
    assert!(r.startup_result().is_none());
    r.complete_creation(DatabaseId(2), Err(failure()));
    pump(&mut r);
    assert!(matches!(r.startup_result(), Some(Err(_))));
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
    assert!(r.active_creations().is_empty());
    assert_eq!(r.step(ActorId::Manager), crate::simulation::StepResult::Stopped);
    let request = r.attach("after-failure");
    assert!(matches!(r.reply(request), ReplySlot::Cancelled));
}

#[test]
fn shutdown_discards_work_cancels_sessions_and_pending_replies() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("a");
    pump(&mut r);
    let (_, key) = attached(&r, a);
    let b = r.attach("b");
    pump(&mut r);
    r.shutdown();
    assert!(r.session_cancelled(&key));
    assert!(matches!(
        r.reply(b),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::EngineUnavailable))
            | ReplySlot::Cancelled
    ));
    assert!(r.active_creations().is_empty());
}
#[test]
fn shutdown_message_stops_dispatch_and_repeated_shutdown_is_harmless() {
    let mut r = ready(config(1, 0, 0));
    r.inject(ManagerMessage::Shutdown);
    let request = r.attach("after-shutdown");
    assert_eq!(r.step(ActorId::Manager), crate::simulation::StepResult::Stopped);
    assert!(matches!(r.reply(request), ReplySlot::Cancelled));
    let trace = r.trace();
    r.shutdown();
    r.inject(ManagerMessage::DatabaseWorker(DatabaseWorkerMessages::CreationFinished {
        database_id: DatabaseId(999),
        result: Ok(db(DatabaseId(999))),
    }));
    pump(&mut r);
    assert_eq!(r.trace(), trace);
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
    assert!(r.snapshot().leases().is_empty());
}

#[test]
fn closed_worker_mailbox_stops_runtime() {
    let mut r = ready(config(1, 0, 0));
    r.close_mailbox(ActorId::Creation);
    assert_eq!(r.step(ActorId::Manager), crate::simulation::StepResult::Stopped);
    let a = r.attach("a");
    assert!(matches!(r.reply(a), ReplySlot::Cancelled));
}
#[test]
fn explicit_steps_do_not_complete_operations_or_advance_time() {
    let mut r = SimRuntime::new(config(1, 0, 0));
    let now = r.now();
    pump(&mut r);
    assert!(r.startup_result().is_none());
    assert_eq!(r.active_creations().len(), 1);
    assert_eq!(r.now(), now);
}
#[test]
fn identical_inputs_produce_identical_trace() {
    fn run() -> Vec<String> {
        let mut r = ready(config(2, 0, 0));
        r.attach("a");
        r.attach("b");
        pump(&mut r);
        r.advance_by(Duration::from_secs(30));
        pump(&mut r);
        r.trace()
    }
    assert_eq!(run(), run());
}
#[test]
fn step_limit_reports_trace_instead_of_hanging() {
    let mut r = SimRuntime::new(config(1, 0, 0));
    assert!(r.run_until_idle(1).unwrap_err().contains("step limit"));
}
#[test]
fn failed_submission_clears_reservations_without_inline_retry() {
    let mut r = ready(config(0, 0, 1));
    r.fail_next_submission(ActorId::Creation);
    r.attach("a");
    pump(&mut r);
    assert!(r.snapshot().inventory.creating().is_empty());
    assert!(r.active_creations().is_empty());
    r.attach("a");
    pump(&mut r);
    assert!(!r.active_creations().is_empty());
    assert!(r.active_creations().iter().all(|id| id.0 > 2));
}
#[test]
fn failed_cleanup_submission_retains_retirement_identity() {
    let mut r = ready(config(1, 0, 0));
    r.attach("a");
    pump(&mut r);
    r.fail_next_submission(ActorId::Cleanup);
    r.release("a");
    pump(&mut r);
    assert!(r.active_cleanups().is_empty());
    assert_eq!(r.snapshot().inventory.retiring().len(), 1);
}
#[test]
fn virtual_caller_deadline_is_independent_of_actor_progress() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("a");
    r.advance_by(Duration::from_secs(31));
    pump(&mut r);
    assert!(matches!(
        r.reply(a),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::TimedOut))
    ));
    assert!(r.snapshot().leases().is_empty());
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
    let release = r.release("closed");
    r.advance_by(Duration::from_secs(5));
    pump(&mut r);
    assert!(matches!(
        r.reply(release),
        ReplySlot::Delivered(ConsumerReply::ReleaseResult(Err(ReleaseError::ReplyTimedOut)))
    ));
    let retry = r.release("closed");
    pump(&mut r);
    assert!(matches!(r.reply(retry), ReplySlot::Delivered(ConsumerReply::ReleaseResult(Ok(())))));
}
#[test]
fn cleanup_execution_window_allows_out_of_order_progress() {
    let mut r = SimRuntime::with_options(
        config(3, 0, 0),
        NonZeroUsize::new(3).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    );
    finish_creations(&mut r);
    for lease in ["a", "b", "c"] {
        r.attach(lease);
        r.release(lease);
    }
    pump(&mut r);
    let jobs = r.active_cleanups();
    assert_eq!(jobs.len(), 2);
    r.complete_cleanup(jobs[1].database_id, Ok(()));
    pump(&mut r);
    assert_eq!(r.active_cleanups().len(), 2);
    assert!(r.active_cleanups().iter().any(|r| r.database_id == jobs[0].database_id));
}
