// Copyright 2024 RisingWave Labs
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

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::Stream;
use pin_project_lite::pin_project;

pin_project! {
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct Pausable<St>
        where St: Stream
    {
        #[pin]
        stream: St,
        paused: Arc<AtomicBool>,
        waker: Arc<Mutex<Option<Waker>>>,
    }
}

/// A valve is a handle that can control the [`Pausable`] stream.
#[derive(Clone)]
pub struct Valve {
    paused: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Valve {
    /// Pause the stream controlled by the valve.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume the stream controlled by the valve.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        if let Some(waker) = self.waker.lock().unwrap().as_ref() {
            waker.wake_by_ref()
        }
    }
}

impl<St> Pausable<St>
where
    St: Stream,
{
    pub(crate) fn new(stream: St) -> (Self, Valve) {
        let paused = Arc::new(AtomicBool::new(false));
        let waker = Arc::new(Mutex::new(None));
        (
            Pausable {
                stream,
                paused: paused.clone(),
                waker: waker.clone(),
            },
            Valve { paused, waker },
        )
    }
}

impl<St> Stream for Pausable<St>
where
    St: Stream,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if this.paused.load(Ordering::Relaxed) {
            let mut waker = this.waker.lock().unwrap();
            *waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            this.stream.poll_next(cx)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
