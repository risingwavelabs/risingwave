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

use std::future::Future;

use futures::stream::TryStream;
use futures::{Stream, TryFuture};

mod buffered_with_fence;
mod misc;
mod pausable;

use buffered_with_fence::{Fenced, MaybeFence, TryBufferedWithFence};
pub use misc::*;
pub use pausable::{Pausable, Valve};

/// Create a pausable stream, which can be paused or resumed by a valve.
pub fn pausable<St>(stream: St) -> (Pausable<St>, Valve)
where
    St: Stream,
{
    Pausable::new(stream)
}

pub trait RwTryStreamExt: TryStream {
    /// Similar to [`TryStreamExt::try_buffered`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html#method.try_buffered), but respect to fence.
    ///
    /// Fence is provided by [`Future`] that implements [`MaybeFence`] and returns `true`.
    /// When the stream receive a fenced future, it'll not do a sync operation. In brief, don't poll later futures until the current
    /// buffer is cleared.
    fn try_buffered_with_fence(self, n: usize) -> TryBufferedWithFence<Self>
    where
        Self: Sized,
        Self::Ok: TryFuture<Error = Self::Error> + MaybeFence;
}

impl<St> RwTryStreamExt for St
where
    St: TryStream,
{
    fn try_buffered_with_fence(self, n: usize) -> TryBufferedWithFence<Self>
    where
        Self: Sized,
        Self::Ok: TryFuture<Error = Self::Error> + MaybeFence,
    {
        TryBufferedWithFence::new(self, n)
    }
}

pub trait RwFutureExt: Future {
    fn with_fence(self, is_fence: bool) -> Fenced<Self>
    where
        Self: Sized;
}

impl<Fut: Future> RwFutureExt for Fut {
    fn with_fence(self, is_fence: bool) -> Fenced<Self> {
        Fenced::new(self, is_fence)
    }
}
