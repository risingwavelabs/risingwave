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

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use futures::{pin_mut, TryStreamExt};
use futures_async_stream::try_stream;
use task_local_stats_alloc;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Message, MessageStream};

/// Streams wrapped by `epoch_provide` is able to retrieve the current epoch pair from the functions
/// from [`epoch::task_local`].
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn alloc_provide(input: impl MessageStream) {
    pin_mut!(input);

    let allocated_bytes = Arc::new(AtomicUsize::new(0));

    while let Some(message) =
        task_local_stats_alloc::scope(allocated_bytes.clone(), input.try_next()).await?
    {
        yield message;
    }
}
