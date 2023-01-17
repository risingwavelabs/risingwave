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

use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use either::Either;
use futures::channel::oneshot;
use futures::stream::poll_fn;
use futures::{future, select_biased, stream_select, FutureExt, Stream, StreamExt};
use futures_async_stream::stream_block;

/// Convert a list of streams into a [`Stream`] of results from the streams.
pub fn select_all<S: Stream + Unpin>(
    streams: impl IntoIterator<Item = S>,
) -> futures::stream::SelectAll<S> {
    // We simply forward the implementation to `futures` as it performs good enough.
    #[expect(clippy::disallowed_methods)]
    futures::stream::select_all(streams)
}
