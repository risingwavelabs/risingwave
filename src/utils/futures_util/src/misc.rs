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
use std::pin::{Pin, pin};
use std::task::{Context, Poll, ready};

use futures::future::{Either, pending, select};
use futures::stream::Peekable;
use futures::{FutureExt, Stream, StreamExt};

/// Convert a list of streams into a [`Stream`] of results from the streams.
pub fn select_all<S: Stream + Unpin>(
    streams: impl IntoIterator<Item = S>,
) -> futures::stream::SelectAll<S> {
    // We simply forward the implementation to `futures` as it performs good enough.
    #[expect(clippy::disallowed_methods)]
    futures::stream::select_all(streams)
}

pub fn pending_on_none<I>(future: impl Future<Output = Option<I>>) -> impl Future<Output = I> {
    future.then(|opt| async move {
        match opt {
            Some(item) => item,
            None => pending::<I>().await,
        }
    })
}

pub fn drop_either_future<A, B>(
    either: Either<(A, impl Future), (B, impl Future)>,
) -> Either<A, B> {
    match either {
        Either::Left((left, _)) => Either::Left(left),
        Either::Right((right, _)) => Either::Right(right),
    }
}

/// Await on a future while monitoring on a peekable stream that may return error.
/// The peekable stream is polled at a higher priority than the future.
///
/// When the peekable stream returns with a error and end of stream, the future will
/// return the error immediately. Otherwise, it will keep polling the given future.
///
/// Return:
///     - Ok(output) as the output of the given future.
///     - Err(None) to indicate that the stream has reached the end.
///     - Err(e) to indicate that the stream returns an error.
pub async fn await_future_with_monitor_error_stream<T, E, F: Future>(
    peek_stream: &mut Peekable<impl Stream<Item = Result<T, E>> + Unpin>,
    future: F,
) -> Result<F::Output, Option<E>> {
    // Poll the response stream to early see the error
    match select(pin!(Pin::new(&mut *peek_stream).peek()), pin!(future)).await {
        Either::Left((response_result, send_future)) => match response_result {
            None => Err(None),
            Some(Err(_)) => {
                let err = match peek_stream.next().now_or_never() {
                    Some(Some(Err(err))) => err,
                    _ => unreachable!("peek has output, peek output not None, have check err"),
                };
                Err(Some(err))
            }
            Some(Ok(_)) => Ok(send_future.await),
        },
        Either::Right((output, _)) => Ok(output),
    }
}

/// Attach an item of type `T` to the future `F`. When the future is polled with ready,
/// the item will be attached to the output of future as `(F::Output, item)`.
///
/// The generated future will be similar to `future.map(|output| (output, item))`. The
/// only difference is that the `Map` future does not provide method `into_inner` to
/// get the original inner future.
pub struct AttachedFuture<F, T> {
    inner: F,
    item: Option<T>,
}

impl<F, T> AttachedFuture<F, T> {
    pub fn new(inner: F, item: T) -> Self {
        Self {
            inner,
            item: Some(item),
        }
    }

    pub fn into_inner(self) -> (F, T) {
        (
            self.inner,
            self.item.expect("should not be called after polled ready"),
        )
    }

    pub fn item(&self) -> &T {
        self.item
            .as_ref()
            .expect("should not be called after polled ready")
    }
}

impl<F: Future + Unpin, T: Unpin> Future for AttachedFuture<F, T> {
    type Output = (F::Output, T);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let output = ready!(this.inner.poll_unpin(cx));
        Poll::Ready((
            output,
            this.item
                .take()
                .expect("should not be polled ready for twice"),
        ))
    }
}
