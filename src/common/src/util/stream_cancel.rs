// Copyright 2023 Singularity Data
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

//! A library inspired by [stream-cancel](https://github.com/jonhoo/stream-cancel). This
//! difference is that it allows you to send a value to original stream when cancelled, this is
//! useful in several cases, for example when cancel query, it can be used to concat an cancel
//! error to original stream.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;

use futures::channel::oneshot::{channel, Receiver, Sender};
use futures::stream::poll_fn;
use futures::{Stream, StreamExt};

pub struct Trigger {
    sender: Sender<()>,
}

impl Trigger {
    /// Used to trigger cancelling of stream.
    pub fn abort(self) {
        if self.sender.send(()).is_err() {
            tracing::debug!("Trigger receiver closed, ignoring...");
        }
    }
}

pub struct Tripwire<R> {
    receiver: Receiver<()>,
    cancel_result: Box<dyn FnOnce() -> R + Send>,
}

pub fn stream_tripwire<F, R>(cancel_result: F) -> (Trigger, Tripwire<R>)
where
    F: FnOnce() -> R + 'static + Send,
{
    let (sender, receiver) = channel();
    (
        Trigger { sender },
        Tripwire {
            receiver,
            cancel_result: Box::new(cancel_result),
        },
    )
}

pub fn cancellable_stream<S>(
    data_stream: S,
    tripwire: Tripwire<S::Item>,
) -> impl Stream<Item = <S as Stream>::Item> + Send
where
    S: Stream + Send,
{
    let Tripwire {
        receiver,
        cancel_result,
    } = tripwire;

    let cancel_flag = Arc::new(AtomicBool::new(false));
    let s_cancel_flag = cancel_flag.clone();

    let cancel_stream = {
        let mut polled = false;
        let mut cancel_result = Some(cancel_result);
        poll_fn(move |_| -> Poll<Option<<S as Stream>::Item>> {
            if polled {
                Poll::Ready(None)
            } else {
                polled = true;
                if s_cancel_flag.load(Ordering::SeqCst) {
                    Poll::Ready(Some(cancel_result.take().unwrap()()))
                } else {
                    Poll::Ready(None)
                }
            }
        })
    };

    let cancel_future = async move {
        if receiver.await.is_err() {
            tracing::debug!("Cancel sender closed, ignoring...");
        } else {
            cancel_flag.store(true, Ordering::SeqCst);
        }
    };

    let data_stream = data_stream.take_until(cancel_future).chain(cancel_stream);

    Box::pin(data_stream)
}

#[cfg(test)]
mod tests {
    use futures::stream::iter;
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_cancellable_stream_aborted() {
        let (trigger, tripwire) = stream_tripwire(|| 5);
        let mut s = cancellable_stream(iter(vec![1i32, 2, 3, 4]), tripwire);
        assert_eq!(Some(1), s.next().await);
        assert_eq!(Some(2), s.next().await);
        trigger.abort();
        assert_eq!(Some(5), s.next().await);
        assert_eq!(None, s.next().await);
        assert_eq!(None, s.next().await);
    }

    #[tokio::test]
    async fn test_cancellable_stream_not_aborted() {
        let (trigger, tripwire) = stream_tripwire(|| 5);
        let mut s = cancellable_stream(iter(vec![1, 2, 3, 4]), tripwire);
        assert_eq!(Some(1), s.next().await);
        assert_eq!(Some(2), s.next().await);
        assert_eq!(Some(3), s.next().await);
        assert_eq!(Some(4), s.next().await);
        assert_eq!(None, s.next().await);
        trigger.abort();
        assert_eq!(None, s.next().await);
        assert_eq!(None, s.next().await);
    }
}
