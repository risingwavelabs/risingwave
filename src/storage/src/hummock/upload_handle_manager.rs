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

pub(crate) struct AttachedFuture<Fut: Future + Unpin, E: Unpin> {
    inner: Fut,
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

pub(crate) struct UploadHandleManager {
    epoch_upload_handle: Vec<AttachedFuture<JoinHandle<()>, HummockEpoch>>,
    remaining_handle_count: BTreeMap<HummockEpoch, usize>,
}

impl UploadHandleManager {
    pub(crate) fn new() -> Self {
        Self {
            epoch_upload_handle: Vec::new(),
            remaining_handle_count: BTreeMap::new(),
        }
    }

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
        ret.into_iter()
            .map(|fut| {
                let epoch = *fut.get_extra();
                *self
                    .remaining_handle_count
                    .get_mut(&epoch)
                    .expect("prev handle count not zero. Should exist") -= 1;
                fut.into_inner()
            })
            .collect_vec()
    }

    pub(crate) fn next_finished_epoch(&mut self) -> UploadHandleManagerSelectAll<'_> {
        let futures = self.epoch_upload_handle.drain(..);
        let select_all_fut = select_all(futures);
        UploadHandleManagerSelectAll {
            manager: self,
            select_all: Some(select_all_fut),
        }
    }
}

pub(crate) struct UploadHandleManagerSelectAll<'a> {
    manager: &'a mut UploadHandleManager,
    select_all: Option<SelectAll<AttachedFuture<JoinHandle<()>, HummockEpoch>>>,
}

impl<'a> Unpin for UploadHandleManagerSelectAll<'a> {}

impl<'a> Future for UploadHandleManagerSelectAll<'a> {
    type Output = std::result::Result<HummockEpoch, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut select_all_fut = self
            .select_all
            .take()
            .expect("should have select all when not ok before");
        loop {
            match select_all_fut.poll_unpin(cx) {
                Poll::Ready(((result, epoch), _, futures)) => {
                    let ret = match result {
                        Ok(_) => {
                            let epoch_count = self
                                .manager
                                .remaining_handle_count
                                .get_mut(&epoch)
                                .expect("prev count not zero. should exist");
                            *epoch_count -= 1;
                            if *epoch_count == 0 {
                                let _ = self.manager.remaining_handle_count.remove(&epoch);
                                Some(Ok(epoch))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(e)),
                    };
                    match ret {
                        Some(ret) => {
                            let _ = self.select_all.insert(select_all(futures));
                            return Poll::Ready(ret);
                        }
                        None => {
                            select_all_fut = select_all(futures);
                        }
                    }
                }
                Poll::Pending => {
                    let _ = self.select_all.insert(select_all_fut);
                    return Poll::Pending;
                }
            };
        }
    }
}

impl<'a> Drop for UploadHandleManagerSelectAll<'a> {
    fn drop(&mut self) {
        todo!()
    }
}
