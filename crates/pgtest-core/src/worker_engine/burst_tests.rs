//! Behavioral tests run without an executor and with explicitly ordered
//! effects.
use std::{num::NonZeroUsize, time::Duration};

use super::{
    core::{LeaseId, WorkerEngineConfig},
    database_jobs::{DatabaseId, DatabaseWorkerMessages},
    errors::{AttachError, ReleaseError},
    messages::{ConsumerReply, EngineMessage, RequestId},
};
use crate::{
    backend::{BackendError, PgEndpoint, PgTarget, ProvisionedDatabase, ResourceId},
    simulation::{ActorId, ReplySlot, SimRuntime},
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
fn ready(config: WorkerEngineConfig) -> SimRuntime {
    let mut runtime = SimRuntime::new(config);
    finish_creations(&mut runtime);
    assert!(matches!(runtime.snapshot().startup_result(), Some(Ok(()))));
    runtime
}
fn config(initial: u16, threshold: u16, batch: u16) -> WorkerEngineConfig {
    WorkerEngineConfig {
        initial_slots: initial.into(),
        starvation_threshold: threshold.into(),
        grow_batch_size: batch.into(),
        ..WorkerEngineConfig::default()
    }
}
fn attached(runtime: &SimRuntime, request: RequestId) -> (DatabaseId, super::messages::LeaseKey) {
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
    let a = r.attach("template", "shared");
    let b = r.attach("template", "shared");
    pump(&mut r);
    assert_eq!(attached(&r, a), attached(&r, b));
    assert_eq!(r.snapshot().leases()["shared"].conns, 2);
    assert_eq!(r.snapshot().inventory.ready().len(), 3);
}
#[test]
fn last_disconnect_keeps_database_for_reconnect() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("template", "same");
    pump(&mut r);
    let old = attached(&r, a);
    r.drop_session(a);
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["same"].conns, 0);
    let b = r.attach("template", "same");
    pump(&mut r);
    assert_eq!(attached(&r, b), old);
}
#[test]
fn release_allows_new_attachments_while_cleanup_is_pending() {
    let mut r = ready(config(2, 0, 0));
    let a = r.attach("template", "a");
    pump(&mut r);
    let (_, key) = attached(&r, a);
    let release = r.release("a");
    let b = r.attach("template", "b");
    pump(&mut r);
    assert!(matches!(r.reply(release), ReplySlot::Delivered(ConsumerReply::ReleaseResult(Ok(())))));
    assert!(r.session_cancelled(&key));
    attached(&r, b);
    assert_eq!(r.active_cleanups().len(), 1);
    assert_eq!(r.snapshot().inventory.retiring().len(), 1);
}
#[test]
fn releasing_unseen_id_does_not_allocate_and_permanently_closes_id() {
    let mut r = ready(config(1, 0, 0));
    r.release("unseen");
    r.release("unseen");
    let a = r.attach("template", "unseen");
    pump(&mut r);
    assert!(matches!(
        r.reply(a),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
    ));
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
    assert!(r.active_cleanups().is_empty());
}
#[test]
fn release_fails_waiters_without_consuming_shared_creation() {
    let mut r = ready(config(0, 0, 1));
    let a = r.attach("template", "a");
    let b = r.attach("template", "a");
    pump(&mut r);
    r.release("a");
    pump(&mut r);
    for id in [a, b] {
        assert!(matches!(
            r.reply(id),
            ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
        ));
    }
    finish_creations(&mut r);
    assert!(r.snapshot().leases().is_empty());
    assert!(r.snapshot().waiters().is_empty());
}
#[test]
fn record_limit_reserves_room_for_closing_existing_leases() {
    let mut cfg = config(2, 0, 0);
    cfg.max_lease_records = NonZeroUsize::new(1).unwrap();
    let mut r = ready(cfg);
    r.attach("template", "first");
    let a = r.attach("template", "second");
    let release = r.release("first");
    let unseen = r.release("second");
    pump(&mut r);
    assert!(matches!(
        r.reply(a),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::LeaseRecordLimitReached))
    ));
    assert!(matches!(r.reply(release), ReplySlot::Delivered(ConsumerReply::ReleaseResult(Ok(())))));
    assert!(matches!(
        r.reply(unseen),
        ReplySlot::Delivered(ConsumerReply::ReleaseResult(Err(
            ReleaseError::LeaseRecordLimitReached
        )))
    ));
}
#[test]
fn lost_release_reply_does_not_undo_closure_or_cleanup() {
    let mut r = ready(config(1, 0, 0));
    r.attach("template", "a");
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
    let a = r.attach("template", "a");
    pump(&mut r);
    let (_, old) = attached(&r, a);
    r.advance_by(Duration::from_secs(30));
    pump(&mut r);
    assert!(r.session_cancelled(&old));
    let b = r.attach("template", "a");
    pump(&mut r);
    let (_, new) = attached(&r, b);
    assert_ne!(old, new);
    r.inject(EngineMessage::Detach { lease: old.lease.clone(), generation: old.generation });
    r.inject(EngineMessage::LeaseMaxTimeReached { lease: old.lease, generation: old.generation });
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["a"].conns, 1);
    assert!(!r.session_cancelled(&new));
    assert_eq!(r.active_cleanups().len(), 1);
}
#[test]
fn duplicate_expiry_schedules_cleanup_only_once() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("template", "a");
    pump(&mut r);
    let (_, key) = attached(&r, a);
    for _ in 0..2 {
        r.inject(EngineMessage::LeaseMaxTimeReached {
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
    r.attach("template", "a");
    pump(&mut r);
    r.advance_by(Duration::from_secs(1_000_000));
    pump(&mut r);
    assert!(r.snapshot().leases().contains_key("a"));
}
#[test]
fn covered_burst_does_not_schedule_redundant_batches() {
    let mut r = ready(config(0, 0, 4));
    for id in ["a", "b", "c"] {
        r.attach("template", id);
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
        r.attach("template", &id.to_string());
    }
    pump(&mut r);
    assert_eq!(r.snapshot().inventory.creating().len(), 12);
}
#[test]
fn connections_for_one_lease_share_demand_and_completion() {
    let mut r = ready(config(0, 0, 1));
    let requests: Vec<_> = (0..10).map(|_| r.attach("template", "a")).collect();
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
    let a = r.attach("template", "a");
    let b = r.attach("template", "b");
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
    let a = r.attach("template", "a");
    pump(&mut r);
    let id = r.active_creations()[0];
    r.complete_creation(id, Err(failure()));
    pump(&mut r);
    finish_creations(&mut r);
    assert_ne!(attached(&r, a).0, id);
    assert_eq!(r.snapshot().counters.template_create_failures, 1);
}
#[test]
fn failed_and_cancelled_replies_restore_fresh_supply() {
    for cancel_after_delivery in [false, true] {
        let mut r = ready(config(1, 0, 0));
        let a = r.attach("template", "a");
        if cancel_after_delivery {
            pump(&mut r);
        }
        r.cancel_request(a);
        pump(&mut r);
        if cancel_after_delivery {
            assert_eq!(r.snapshot().leases()["a"].conns, 0);
        } else {
            assert!(r.snapshot().leases().is_empty());
            assert_eq!(r.snapshot().inventory.ready().len(), 1);
        }
    }
}
#[test]
fn cancelled_join_does_not_increment_connection_count() {
    let mut r = ready(config(1, 0, 0));
    r.attach("template", "a");
    pump(&mut r);
    let b = r.attach("template", "a");
    r.cancel_request(b);
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["a"].conns, 1);
}
#[test]
fn cancelled_waiter_does_not_block_live_waiter() {
    let mut r = ready(config(0, 0, 1));
    let a = r.attach("template", "a");
    let b = r.attach("template", "b");
    r.cancel_request(a);
    pump(&mut r);
    finish_creations(&mut r);
    assert!(!r.snapshot().leases().contains_key("a"));
    attached(&r, b);
}
#[test]
fn expired_waiters_do_not_consume_supply() {
    let mut r = ready(config(0, 0, 1));
    r.attach("template", "a");
    pump(&mut r);
    r.advance_by(Duration::from_secs(31));
    finish_creations(&mut r);
    assert!(r.snapshot().leases().is_empty());
    assert!(r.snapshot().waiters().is_empty());
    assert_eq!(r.snapshot().counters.waiter_timeouts, 1);
}
#[test]
fn unknown_and_duplicate_creation_results_cannot_supply_database() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("template", "a");
    pump(&mut r);
    let (id, _) = attached(&r, a);
    for database_id in [id, DatabaseId(999)] {
        r.inject(EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CreationFinished {
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
    r.attach("template", "a");
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
    r.attach("template", "a");
    pump(&mut r);
    for id in [1, 2, 99] {
        r.inject(EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CleanupFinished {
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
    let mut r = SimRuntime::with_options(
        config(5, 0, 0),
        "template".into(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
        vec![],
    );
    pump(&mut r);
    assert_eq!(r.active_creations(), vec![DatabaseId(1), DatabaseId(2)]);
    r.complete_creation(DatabaseId(2), Ok(db(DatabaseId(2))));
    pump(&mut r);
    assert_eq!(r.active_creations(), vec![DatabaseId(1), DatabaseId(3)]);
    finish_creations(&mut r);
    assert_eq!(r.snapshot().inventory.ready().len(), 5);
}
#[test]
fn startup_partial_failure_waits_for_every_result() {
    let mut r = SimRuntime::new(config(2, 0, 0));
    pump(&mut r);
    r.complete_creation(DatabaseId(1), Err(failure()));
    pump(&mut r);
    assert!(r.snapshot().startup_result().is_none());
    r.complete_creation(DatabaseId(2), Ok(db(DatabaseId(2))));
    pump(&mut r);
    assert!(matches!(r.snapshot().startup_result(), Some(Err(_))));
    assert_eq!(r.snapshot().inventory.ready().len(), 1);
}
#[test]
fn stale_cleanup_precedes_creation_and_failure_does_not_block_startup() {
    let mut r = SimRuntime::with_options(
        config(1, 0, 0),
        "template".into(),
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        vec![ResourceId("old".into())],
    );
    pump(&mut r);
    assert!(r.active_creations().is_empty());
    let id = r.active_cleanups()[0].database_id;
    r.complete_cleanup(id, Err(failure()));
    pump(&mut r);
    finish_creations(&mut r);
    assert!(matches!(r.snapshot().startup_result(), Some(Ok(()))));
    assert_eq!(r.snapshot().inventory.retiring().len(), 1);
}
#[test]
fn shutdown_discards_work_cancels_sessions_and_pending_replies() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("template", "a");
    pump(&mut r);
    let (_, key) = attached(&r, a);
    let b = r.attach("template", "b");
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
fn closed_worker_mailbox_stops_runtime() {
    let mut r = ready(config(1, 0, 0));
    r.close_mailbox(ActorId::Creation);
    assert!(r.snapshot().is_stopped());
    let a = r.attach("template", "a");
    assert!(matches!(r.reply(a), ReplySlot::Cancelled));
}
#[test]
fn template_mismatch_does_not_admit_lease() {
    let mut r = ready(config(1, 0, 0));
    let a = r.attach("wrong", "a");
    pump(&mut r);
    assert!(matches!(
        r.reply(a),
        ReplySlot::Delivered(ConsumerReply::AttachRejected(AttachError::TemplateMismatch))
    ));
    assert!(r.snapshot().leases().is_empty());
}
#[test]
fn explicit_steps_do_not_complete_operations_or_advance_time() {
    let mut r = SimRuntime::new(config(1, 0, 0));
    let now = r.now();
    pump(&mut r);
    assert!(r.snapshot().startup_result().is_none());
    assert_eq!(r.active_creations().len(), 1);
    assert_eq!(r.now(), now);
}
#[test]
fn identical_inputs_produce_identical_trace() {
    fn run() -> Vec<String> {
        let mut r = ready(config(2, 0, 0));
        r.attach("template", "a");
        r.attach("template", "b");
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
fn detach_unknown_or_zero_connection_is_harmless() {
    let mut r = ready(config(1, 0, 0));
    r.inject(EngineMessage::Detach { lease: LeaseId::new("ghost").unwrap(), generation: 1 });
    let a = r.attach("template", "a");
    pump(&mut r);
    let (_, key) = attached(&r, a);
    r.drop_session(a);
    r.inject(EngineMessage::Detach { lease: key.lease, generation: key.generation });
    pump(&mut r);
    assert_eq!(r.snapshot().leases()["a"].conns, 0);
    assert_eq!(r.snapshot().counters.detach_on_zero, 1);
}

#[test]
fn failed_submission_clears_reservations_without_inline_retry() {
    let mut r = ready(config(0, 0, 1));
    r.fail_next_submission(ActorId::Creation);
    r.attach("template", "a");
    pump(&mut r);
    assert!(r.snapshot().inventory.creating().is_empty());
    assert!(r.active_creations().is_empty());
    assert_eq!(r.snapshot().counters.unable_to_start_database_slots, 2);
    r.attach("template", "a");
    pump(&mut r);
    assert!(r.active_creations().iter().all(|id| id.0 > 2));
}
#[test]
fn failed_cleanup_submission_retains_retirement_identity() {
    let mut r = ready(config(1, 0, 0));
    r.attach("template", "a");
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
    let a = r.attach("template", "a");
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
        "template".into(),
        NonZeroUsize::new(3).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        vec![],
    );
    finish_creations(&mut r);
    for lease in ["a", "b", "c"] {
        r.attach("template", lease);
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
#[test]
fn restart_can_reconcile_remote_success_after_local_cancellation() {
    let mut first = SimRuntime::new(config(1, 0, 0));
    pump(&mut first);
    let id = first.active_creations()[0];
    let remote = db(id);
    first.shutdown();
    let mut second = SimRuntime::with_options(
        config(0, 0, 0),
        "template".into(),
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        vec![remote.resource_id.clone()],
    );
    pump(&mut second);
    let cleanup = second.active_cleanups()[0].clone();
    assert_eq!(cleanup.resource_id, remote.resource_id);
    second.complete_cleanup(cleanup.database_id, Ok(()));
    pump(&mut second);
    assert!(matches!(second.snapshot().startup_result(), Some(Ok(()))));
    assert!(second.snapshot().inventory.retiring().is_empty());
}
