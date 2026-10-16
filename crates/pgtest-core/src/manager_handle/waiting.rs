//! Request-owned shared assignments. Neither the registry nor queued work
//! owns a strong receiver, so the last caller leaving closes the assignment.
use std::{
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::{
    FutureExt,
    future::{Shared, WeakShared},
};
use tokio::sync::oneshot;

use super::{LeaseId, Reply, TokioMessage, errors::AttachError, messages::ManagerMessage};

pub(crate) type Assignment = Shared<AssignmentFuture>;

struct PendingAssignment {
    generation: u64,
    assignment: WeakShared<AssignmentFuture>,
}

#[derive(Default)]
pub(crate) struct WaitingRegistry {
    entries: papaya::HashMap<LeaseId, PendingAssignment, rustc_hash::FxBuildHasher>,
    next_generation: AtomicU64,
}

impl WaitingRegistry {
    #[hotpath::measure]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[hotpath::measure]
    pub(crate) fn remove(&self, lease: &LeaseId, generation: u64) {
        let _ = self.entries.pin().remove_if(lease, |_, entry| entry.generation == generation);
    }

    /// Only the insertion winner submits allocation work. Joining callers
    /// receive independent strong clones and keep their own deadlines.
    #[hotpath::measure]
    pub(crate) fn register(self: &Arc<Self>, lease: LeaseId) -> (Assignment, Option<TokioMessage>) {
        let mut candidate = None;
        loop {
            let entries = self.entries.pin();
            if let Some(entry) = entries.get(&lease) {
                if let Some(assignment) = hotpath::measure_block!("waiting_registry::upgrade", {
                    entry.assignment.upgrade()
                }) {
                    drop(entries);
                    return (assignment, None);
                }
                let generation = entry.generation;
                let _ = hotpath::measure_block!("waiting_registry::remove_stale", {
                    entries.remove_if(&lease, |_, entry| entry.generation == generation)
                });
            }

            let (generation, assignment, sender) = candidate.get_or_insert_with(|| {
                hotpath::measure_block!("waiting_registry::create_assignment", {
                    let generation = self
                        .next_generation
                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                            value.checked_add(1)
                        })
                        .expect("waiting assignment generation exhausted");
                    let (sender, receiver) = oneshot::channel();
                    let assignment = AssignmentFuture {
                        receiver: Some(receiver),
                        registry: Arc::downgrade(self),
                        lease: lease.clone(),
                        generation,
                    }
                    .shared();
                    (generation, assignment, Some(sender))
                })
            });

            let inserted = hotpath::measure_block!("waiting_registry::insert", {
                entries
                    .try_insert(
                        lease.clone(),
                        PendingAssignment {
                            generation: *generation,
                            assignment: assignment.downgrade().expect("unpolled assignment"),
                        },
                    )
                    .is_ok()
            });
            drop(entries);
            if inserted {
                let work = ManagerMessage::AttachOrJoin {
                    lease,
                    reply: sender.take().expect("allocation work is submitted once"),
                    waiting_generation: *generation,
                };
                return (candidate.take().unwrap().1, Some(work));
            }
        }
    }
}

pub(crate) struct AssignmentFuture {
    receiver: Option<oneshot::Receiver<Reply>>,
    registry: Weak<WaitingRegistry>,
    lease: LeaseId,
    generation: u64,
}

impl Future for AssignmentFuture {
    type Output = Result<Reply, AttachError>;

    #[hotpath::measure]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(self.receiver.as_mut().expect("receiver exists until drop"))
            .poll(cx)
            .map(|reply| reply.map_err(|_| AttachError::EngineUnavailable))
    }
}

impl Drop for AssignmentFuture {
    #[hotpath::measure]
    fn drop(&mut self) {
        // Close first, so an allocator racing cleanup cannot deliver to an
        // abandoned assignment. This also works before the first poll.
        drop(self.receiver.take());
        if let Some(registry) = self.registry.upgrade() {
            registry.remove(&self.lease, self.generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use futures_util::task::{ArcWake, waker};
    use pgtest_engine_backend::{PgEndpoint, PgTarget, jobs::DatabaseId};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::manager_handle::LeaseSession;

    fn lease() -> LeaseId {
        LeaseId::new("shared").unwrap()
    }

    fn work(message: Option<TokioMessage>) -> (u64, oneshot::Sender<Reply>) {
        let Some(ManagerMessage::AttachOrJoin { waiting_generation, reply, .. }) = message else {
            panic!("expected assignment work")
        };
        (waiting_generation, reply)
    }

    fn session() -> LeaseSession {
        LeaseSession {
            database_id: DatabaseId(1),
            target: Arc::new(PgTarget {
                database: "db".into(),
                endpoint: PgEndpoint::Tcp { host: "localhost".into(), port: 5432 },
            }),
            cancellation: CancellationToken::new(),
        }
    }

    #[test]
    fn only_last_owner_removes_unpolled_assignment_and_closes_sender() {
        let registry = Arc::new(WaitingRegistry::default());
        let (first, message) = registry.register(lease());
        let (_, sender) = work(message);
        let (second, message) = registry.register(lease());
        assert!(message.is_none());
        drop(first);
        assert_eq!(registry.len(), 1);
        assert!(!sender.is_closed());
        drop(second);
        assert_eq!(registry.len(), 0);
        assert!(sender.is_closed());
    }

    #[test]
    fn old_cleanup_cannot_remove_a_replacement_registration() {
        let registry = Arc::new(WaitingRegistry::default());
        let (old, message) = registry.register(lease());
        let (generation, sender) = work(message);
        sender.send(Reply::Attached(session())).unwrap();
        // Model the allocator finishing before the old result is polled.
        registry.remove(&lease(), generation);
        let (new, message) = registry.register(lease());
        let (new_generation, sender) = work(message);
        assert_ne!(generation, new_generation);
        drop(old);
        registry.remove(&lease(), generation);
        assert_eq!(registry.len(), 1);
        assert!(!sender.is_closed());
        drop(new);
        assert_eq!(registry.len(), 0);
    }

    struct WakeCount(AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn one_send_wakes_every_subscriber_and_preserves_the_shared_session() {
        let registry = Arc::new(WaitingRegistry::default());
        let (first, message) = registry.register(lease());
        let (_, sender) = work(message);
        let mut futures = vec![first.clone(), first.clone(), first];
        let wakes: Vec<_> =
            (0..futures.len()).map(|_| Arc::new(WakeCount(AtomicUsize::new(0)))).collect();
        for (future, wake) in futures.iter_mut().zip(&wakes) {
            assert!(
                Pin::new(future).poll(&mut Context::from_waker(&waker(wake.clone()))).is_pending()
            );
        }
        let session = session();
        sender.send(Reply::Attached(session.clone())).unwrap();
        assert!(wakes.iter().all(|wake| wake.0.load(Ordering::Relaxed) > 0));
        for future in &mut futures {
            let Poll::Ready(Ok(Reply::Attached(received))) =
                Pin::new(future).poll(&mut Context::from_waker(std::task::Waker::noop()))
            else {
                panic!("all subscribers must receive the session")
            };
            assert!(Arc::ptr_eq(&received.target, &session.target));
            assert_eq!(received.database_id, session.database_id);
        }
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test]
    async fn closed_sender_resolves_every_subscriber_and_removes_registration() {
        let registry = Arc::new(WaitingRegistry::default());
        let (first, message) = registry.register(lease());
        let second = first.clone();
        drop(message);
        assert!(matches!(first.await, Err(AttachError::EngineUnavailable)));
        assert!(matches!(second.await, Err(AttachError::EngineUnavailable)));
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn simultaneous_registrations_submit_only_one_allocation() {
        let registry = Arc::new(WaitingRegistry::default());
        let barrier = Arc::new(std::sync::Barrier::new(17));
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let registry = registry.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    registry.register(lease())
                })
            })
            .collect();
        barrier.wait();
        let registrations: Vec<_> =
            threads.into_iter().map(|thread| thread.join().unwrap()).collect();
        assert_eq!(registrations.iter().filter(|(_, work)| work.is_some()).count(), 1);
        assert_eq!(registry.len(), 1);
        drop(registrations);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn last_owner_drop_racing_registration_cannot_remove_the_live_assignment() {
        for _ in 0..64 {
            let registry = Arc::new(WaitingRegistry::default());
            let (old, old_work) = registry.register(lease());
            let (_, old_sender) = work(old_work);
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let dropped = {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    drop(old);
                })
            };
            barrier.wait();
            let (new, new_work) = registry.register(lease());
            dropped.join().unwrap();
            assert_eq!(registry.len(), 1);
            if let Some(new_work) = new_work {
                assert!(old_sender.is_closed());
                let (_, sender) = work(Some(new_work));
                assert!(!sender.is_closed());
            } else {
                assert!(!old_sender.is_closed());
            }
            drop(new);
            assert_eq!(registry.len(), 0);
        }
    }
}
