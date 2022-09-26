// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::{select_all, SelectAll};
use futures::FutureExt;
use itertools::Itertools;
use risingwave_hummock_sdk::HummockEpoch;
use tokio::task::{JoinError, JoinHandle};

/// Attach an extra item of type `E` to the future. The `Output` of the `AttachedFuture` will be
/// `(Fut::Output, E)`
pub(crate) struct AttachedFuture<Fut: Future + Unpin, E: Unpin> {
    /// The inner future
    inner: Fut,

    /// The attached item. Set it as `Option` so that when we `poll` the `inner` future and return
    /// `Ready`, we can take it out
    extra: Option<E>,
}

impl<Fut: Future + Unpin, E: Unpin> AttachedFuture<Fut, E> {
    pub(crate) fn new(future: Fut, extra: E) -> Self {
        Self {
            inner: future,
            extra: Some(extra),
        }
    }

    pub(crate) fn get_extra(&self) -> &E {
        self.extra.as_ref().expect("should exist")
    }

    pub(crate) fn into_inner(self) -> Fut {
        self.inner
    }
}

impl<Fut: Future + Unpin, E: Unpin> Future for AttachedFuture<Fut, E> {
    type Output = (Fut::Output, E);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.poll_unpin(cx) {
            Poll::Ready(output) => {
                Poll::Ready((output, self.extra.take().expect("cannot poll ok for twice")))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Handle the upload `JoinHandle` of each `HummockEpoch`.
///
/// Calling `upload_handle_manager.next_finished_epoch().await` will return an epoch when all the
/// upload handles of the epoch is finished, and otherwise pending.
///
/// `upload_handle_manager.next_finished_epoch()` will return a
/// `UploadHandleManagerNextFinishedEpoch` future, which is cancellation safe. In case of being
/// dropped, the pending upload join handle will be restored back to the `upload_handle_manager`.
pub(crate) struct UploadHandleManager {
    /// A list of upload join handles attached with their pending epochs.
    epoch_upload_handle: Vec<AttachedFuture<JoinHandle<()>, HummockEpoch>>,
    /// Count the number of remaining join handle of each epoch in `epoch_upload_handle`.
    remaining_handle_count: BTreeMap<HummockEpoch, usize>,
}

impl UploadHandleManager {
    pub(crate) fn new() -> Self {
        Self {
            epoch_upload_handle: Vec::new(),
            remaining_handle_count: BTreeMap::new(),
        }
    }

    /// Add some upload join handle to an `epoch`
    pub(crate) fn add_epoch_handle(
        &mut self,
        epoch: HummockEpoch,
        handles: impl Iterator<Item = JoinHandle<()>>,
    ) {
        let mut count = 0;
        for handle in handles {
            count += 1;
            self.epoch_upload_handle
                .push(AttachedFuture::new(handle, epoch));
        }
        *self.remaining_handle_count.entry(epoch).or_default() += count;
    }

    /// Drain and return the upload join handle of epochs that fall in the given `range`.
    pub(crate) fn drain_epoch_handle(
        &mut self,
        range: impl RangeBounds<HummockEpoch>,
    ) -> Vec<JoinHandle<()>> {
        let ret = self
            .epoch_upload_handle
            .drain_filter(|fut| {
                let epoch = fut.get_extra();
                range.contains(epoch)
            })
            .collect_vec();
        self.remaining_handle_count
            .drain_filter(|epoch, _| range.contains(epoch));
        ret.into_iter().map(|fut| fut.into_inner()).collect_vec()
    }

    /// Return a `UploadHandleManagerNextFinishedEpoch` future that returns an epoch when all the
    /// upload join handle of the epoch are finished, and pending otherwise.
    pub(crate) fn next_finished_epoch(&mut self) -> UploadHandleManagerNextFinishedEpoch<'_> {
        let futures = self.epoch_upload_handle.drain(..).collect_vec();
        let select_all = if futures.is_empty() {
            None
        } else {
            Some(select_all(futures))
        };
        UploadHandleManagerNextFinishedEpoch {
            manager: self,
            select_all,
        }
    }
}

/// A future which is associated to a `UploadHandleManager` by holding its mutable reference. The
/// future returns an epoch when all the upload join handle of the epoch are finished, and pending
/// otherwise.
///
/// The future is cancellation safe. In case of being dropped, it will restore the pending join
/// handle back to the `UploadHandleManager`.
pub(crate) struct UploadHandleManagerNextFinishedEpoch<'a> {
    manager: &'a mut UploadHandleManager,

    /// Wrap all pending upload join handle with a `SelectAll`. If there is no pending upload join
    /// handle, it will be `None`.
    select_all: Option<SelectAll<AttachedFuture<JoinHandle<()>, HummockEpoch>>>,
}

impl<'a> Unpin for UploadHandleManagerNextFinishedEpoch<'a> {}

impl<'a> Future for UploadHandleManagerNextFinishedEpoch<'a> {
    type Output = std::result::Result<HummockEpoch, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut select_all_fut = if let Some(select_all_fut) = self.select_all.take() {
                select_all_fut
            } else {
                assert!(self.manager.remaining_handle_count.is_empty());
                // If `self.select_all` is `None`, there is no pending epoch. Return `Pending`.
                return Poll::Pending;
            };
            match select_all_fut.poll_unpin(cx) {
                Poll::Ready(((result, epoch), _, futures)) => {
                    // Reset `self.select_all`
                    if !futures.is_empty() {
                        assert!(self.select_all.replace(select_all(futures)).is_none());
                    }

                    // Decrease the remaining count of the epoch by 1 and remove it when it reaches
                    // 0.
                    let epoch_remaining_count_mut_ref =
                        self.manager.remaining_handle_count.get_mut(&epoch).expect(
                            "a join handle just finish. prev count must not zero. should exist",
                        );
                    assert!(*epoch_remaining_count_mut_ref > 0);
                    *epoch_remaining_count_mut_ref -= 1;
                    let epoch_remaining_count = *epoch_remaining_count_mut_ref;
                    if epoch_remaining_count == 0 {
                        let _ = self.manager.remaining_handle_count.remove(&epoch);
                    } else {
                        assert!(
                            self.select_all.is_some(),
                            "an epoch has some remaining join handle, the select_all must not be empty"
                        );
                    }

                    match result {
                        Ok(_) => {
                            // If the there is no remaining join handle in this epoch, return the
                            // epoch. Otherwise, keep polling other join handle
                            if epoch_remaining_count == 0 {
                                return Poll::Ready(Ok(epoch));
                            }
                        }
                        Err(e) => {
                            // Return when we meet an error
                            return Poll::Ready(Err(e));
                        }
                    };
                }
                Poll::Pending => {
                    assert!(self.select_all.replace(select_all_fut).is_none());
                    return Poll::Pending;
                }
            };
        }
    }
}

impl<'a> Drop for UploadHandleManagerNextFinishedEpoch<'a> {
    fn drop(&mut self) {
        // In case of being dropped, restore the join handle back to `self.manager`
        if let Some(select_all) = self.select_all.take() {
            self.manager
                .epoch_upload_handle
                .extend(select_all.into_inner());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::iter::once;
    use std::task::Poll;

    use futures::FutureExt;
    use tokio::sync::oneshot;

    use crate::hummock::upload_handle_manager::{AttachedFuture, UploadHandleManager};

    async fn is_pending<F>(future: &mut F) -> bool
    where
        F: Future + Unpin,
    {
        poll_fn(|cx| match future.poll_unpin(cx) {
            Poll::Ready(_) => Poll::Ready(false),
            Poll::Pending => Poll::Ready(true),
        })
        .await
    }

    #[tokio::test]
    async fn test_attached_future() {
        let future = async move { 1 }.boxed();
        let attached_future = AttachedFuture::new(future, 2);
        assert_eq!(attached_future.get_extra(), &2);
        assert_eq!(attached_future.await, (1, 2));
    }

    #[tokio::test]
    async fn test_attached_future_into_inner() {
        let future = async move { 1 }.boxed();
        let attached_future = AttachedFuture::new(future, 2);
        assert_eq!(attached_future.get_extra(), &2);
        assert_eq!(attached_future.into_inner().await, 1);
    }

    #[tokio::test]
    async fn test_empty_pending() {
        let mut manager = UploadHandleManager::new();
        let mut select_all = manager.next_finished_epoch();
        assert!(is_pending(&mut select_all).await);
    }

    #[tokio::test]
    async fn test_normal() {
        let mut manager = UploadHandleManager::new();
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();
        let join_handle1 = tokio::spawn(async move {
            rx1.await.unwrap();
        });
        let join_handle2 = tokio::spawn(async move {
            rx2.await.unwrap();
        });
        let join_handle3 = tokio::spawn(async move {
            rx3.await.unwrap();
        });
        manager.add_epoch_handle(1, vec![join_handle1, join_handle2].into_iter());
        manager.add_epoch_handle(2, once(join_handle3));

        assert_eq!(3, manager.epoch_upload_handle.len());
        assert_eq!(2, manager.remaining_handle_count.len());
        assert_eq!(2, manager.remaining_handle_count[&1]);
        assert_eq!(1, manager.remaining_handle_count[&2]);

        let mut select_all = manager.next_finished_epoch();
        assert!(is_pending(&mut select_all).await);
        assert_eq!(2, select_all.manager.remaining_handle_count.len());
        assert_eq!(2, select_all.manager.remaining_handle_count[&1]);
        assert_eq!(1, select_all.manager.remaining_handle_count[&2]);

        tx1.send(()).unwrap();
        // Call yield_now to drive the spawned tasks.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(is_pending(&mut select_all).await);
        assert_eq!(2, select_all.manager.remaining_handle_count.len());
        assert_eq!(1, select_all.manager.remaining_handle_count[&1]);
        assert_eq!(1, select_all.manager.remaining_handle_count[&2]);

        tx3.send(()).unwrap();
        // Call yield_now to drive the spawned tasks.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(select_all.await.unwrap(), 2);
        assert_eq!(1, manager.epoch_upload_handle.len());
        assert_eq!(1, manager.remaining_handle_count.len());
        assert_eq!(1, manager.remaining_handle_count[&1]);

        let mut select_all = manager.next_finished_epoch();
        assert!(is_pending(&mut select_all).await);
        tx2.send(()).unwrap();
        // Call yield_now to drive the spawned tasks.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(select_all.await.unwrap(), 1);
        assert!(manager.epoch_upload_handle.is_empty());
        assert!(manager.remaining_handle_count.is_empty());
    }

    #[tokio::test]
    async fn test_drain_epoch_handle() {
        let mut manager = UploadHandleManager::new();
        manager.add_epoch_handle(1, once(tokio::spawn(async move {})));
        manager.add_epoch_handle(2, once(tokio::spawn(async move {})));
        manager.add_epoch_handle(3, once(tokio::spawn(async move {})));
        assert_eq!(3, manager.epoch_upload_handle.len());
        assert_eq!(3, manager.remaining_handle_count.len());
        assert_eq!(1, manager.remaining_handle_count[&1]);
        assert_eq!(1, manager.remaining_handle_count[&2]);
        assert_eq!(1, manager.remaining_handle_count[&3]);

        assert_eq!(1, manager.drain_epoch_handle(1..=1).len());
        assert_eq!(2, manager.epoch_upload_handle.len());
        assert_eq!(2, manager.remaining_handle_count.len());
        assert_eq!(1, manager.remaining_handle_count[&2]);
        assert_eq!(1, manager.remaining_handle_count[&3]);

        assert_eq!(2, manager.drain_epoch_handle(2..=3).len());
        assert!(manager.remaining_handle_count.is_empty());
        assert!(manager.epoch_upload_handle.is_empty());
    }
}
