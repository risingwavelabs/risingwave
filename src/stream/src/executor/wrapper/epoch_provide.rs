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

use futures::{TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_common::util::epoch;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Message, MessageStream};

/// Streams wrapped by `epoch_provide` is able to retrieve the current epoch pair from the functions
/// from [`epoch::task_local`].
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn epoch_provide(input: impl MessageStream) {
    pin_mut!(input);

    let mut epoch = None;

    while let Some(message) = if let Some(epoch) = epoch {
        epoch::task_local::scope(epoch, input.try_next()).await?
    } else {
        input.try_next().await?
    } {
        // The inner executor has yielded a new barrier message. In next polls, we will provide the
        // updated epoch pair.
        if let Message::Barrier(b) = &message {
            epoch = Some(b.epoch);
        }
        yield message;
    }
}
