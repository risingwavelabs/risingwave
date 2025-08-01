// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::TryFutureExt;
use futures::future::{FusedFuture, IntoFuture, TryFuture};
use futures::stream::{
    Fuse, FuturesOrdered, IntoStream, Stream, StreamExt, TryStream, TryStreamExt,
};
use pin_project_lite::pin_project;

pub trait MaybeFence {
    fn is_fence(&self) -> bool {
        false
    }
}

pin_project! {
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct TryBufferedWithFence<St>
    where
        St: TryStream,
        St::Ok: TryFuture,
    {
        #[pin]
        stream: Fuse<IntoStream<St>>,
        in_progress_queue: FuturesOrdered<IntoFuture<St::Ok>>,
        syncing: bool,
        max: usize,
    }
}

impl<St> TryBufferedWithFence<St>
where
    St: TryStream,
    St::Ok: TryFuture<Error = St::Error> + MaybeFence,
{
    pub(crate) fn new(stream: St, n: usize) -> Self {
        Self {
            stream: stream.into_stream().fuse(),
            in_progress_queue: FuturesOrdered::new(),
            syncing: false,
            max: n,
        }
    }
}

impl<St> Stream for TryBufferedWithFence<St>
where
    St: TryStream,
    St::Ok: TryFuture<Error = St::Error> + MaybeFence,
{
    type Item = Result<<St::Ok as TryFuture>::Ok, St::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.syncing && this.in_progress_queue.is_empty() {
            *this.syncing = false;
        }

        // First up, try to spawn off as many futures as possible by filling up our queue of futures, **if the stream is not in syncing**.
        // Propagate errors from the stream immediately.
        while !*this.syncing && this.in_progress_queue.len() < *this.max {
            match this.stream.as_mut().poll_next(cx)? {
                Poll::Ready(Some(fut)) => {
                    let is_fence = fut.is_fence();
                    this.in_progress_queue
                        .push_back(TryFutureExt::into_future(fut));
                    if is_fence {
                        // While receiving a fence, don't buffer more data.
                        *this.syncing = true;
                        break;
                    }
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            x @ Poll::Pending | x @ Poll::Ready(Some(_)) => return x,
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or polled them"]
    pub struct Fenced<Fut: Future> {
        #[pin]
        inner: Fut,
        is_fence: bool,
    }
}

impl<Fut> Fenced<Fut>
where
    Fut: Future,
{
    pub(crate) fn new(inner: Fut, is_fence: bool) -> Self {
        Self { inner, is_fence }
    }
}

impl<Fut> Future for Fenced<Fut>
where
    Fut: Future,
{
    type Output = <Fut as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        this.inner.poll(cx)
    }
}

impl<Fut> FusedFuture for Fenced<Fut>
where
    Fut: FusedFuture,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<Fut> MaybeFence for Fenced<Fut>
where
    Fut: Future,
{
    fn is_fence(&self) -> bool {
        self.is_fence
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream::StreamExt;

    use crate::{RwFutureExt, RwTryStreamExt};

    #[tokio::test]
    async fn test_buffered_with_fence() {
        let n = 10;
        let polled_flags: Vec<_> = (0..n).map(|_| Arc::new(Mutex::new(false))).collect();
        let futs = polled_flags.iter().cloned().enumerate().map(|(i, flag)| {
            let polled_flags2: Vec<_> = polled_flags.clone();
            let is_fence = i == 2 || i == 4 || i == 9;

            async move {
                {
                    let mut flag = flag.lock().unwrap();
                    *flag = true;
                }
                tokio::time::sleep(Duration::from_millis(10 * (n - i) as u64)).await;
                if is_fence {
                    let all_later_unpolled =
                        polled_flags2[(i + 1)..n].iter().cloned().all(|flag| {
                            let flag = flag.lock().unwrap();
                            !*flag
                        });
                    assert!(all_later_unpolled);
                }
                tokio::time::sleep(Duration::from_millis(10 * (n - i) as u64)).await;

                Ok::<_, ()>(())
            }
            .with_fence(is_fence)
        });
        let st = futures::stream::iter(futs)
            .map(Ok)
            .try_buffered_with_fence(4);
        let cnt = st.count().await;
        assert_eq!(cnt, n);
    }
}
