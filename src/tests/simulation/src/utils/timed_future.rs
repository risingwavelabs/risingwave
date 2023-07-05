// Copyright 2023 RisingWave Labs
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
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project::pin_project;

/// Inspired by https://stackoverflow.com/a/59935743/2990323
/// A wrapper around a Future which adds timing data.
#[pin_project]
pub struct Timed<Fut, F>
where
    Fut: Future,
    F: Fn(&Fut::Output, Duration),
{
    #[pin]
    inner: Fut,
    f: F,
    start: Option<Instant>,
}

impl<Fut, F> Future for Timed<Fut, F>
where
    Fut: Future,
    F: Fn(&Fut::Output, Duration),
{
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let start = this.start.get_or_insert_with(Instant::now);

        match this.inner.poll(cx) {
            // If the inner future is still pending, this wrapper is still pending.
            Poll::Pending => Poll::Pending,

            // If the inner future is done, measure the elapsed time and finish this wrapper future.
            Poll::Ready(v) => {
                let elapsed = start.elapsed();
                (this.f)(&v, elapsed);

                Poll::Ready(v)
            }
        }
    }
}

pub trait TimedExt: Sized + Future {
    fn timed<F>(self, f: F) -> Timed<Self, F>
    where
        F: Fn(&Self::Output, Duration),
    {
        Timed {
            inner: self,
            f,
            start: None,
        }
    }
}

// All futures can use the `.timed` method defined above
impl<F: Future> TimedExt for F {}
