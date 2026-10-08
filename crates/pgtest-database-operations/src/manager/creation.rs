use std::num::NonZeroUsize;

use futures_util::{StreamExt, stream};

// The window bounds pending futures per batch. Each operation checks out its
// own connection, so the shared pool also bounds execution across batches.
pub(super) fn run_bounded<F, T>(
    amount: usize,
    limit: NonZeroUsize,
    mut create: impl FnMut(usize) -> F + Send,
    mut on_result: impl FnMut(usize, T) + Send,
) -> impl Future<Output = ()> + Send
where
    F: Future<Output = T> + Send,
    T: Send,
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
            std::num::NonZeroUsize::new(2).unwrap(),
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
            std::num::NonZeroUsize::new(2).unwrap(),
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
}
