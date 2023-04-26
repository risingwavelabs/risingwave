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

use futures::{pin_mut, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::util::epoch::EpochPair;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Message, MessageStream};

tokio::task_local! {
    static EPOCH: EpochPair;
}

/// Retrieve the current epoch from the task local storage.
///
/// This value is updated after every yield of the barrier message. **Panics** if the first barrier
/// message is not yielded.
pub fn curr_epoch() -> u64 {
    EPOCH.with(|e| e.curr)
}

/// Retrieve the previous epoch from the task local storage.
///
/// This value is updated after every yield of the barrier message. **Panics** if the first barrier
/// message is not yielded.
pub fn prev_epoch() -> u64 {
    EPOCH.with(|e| e.prev)
}

/// Retrieve the epoch pair from the task local storage.
///
/// This value is updated after every yield of the barrier message. **Panics** if the first barrier
/// message is not yielded.
pub fn epoch() -> EpochPair {
    EPOCH.get()
}

/// Streams wrapped by `epoch_provide` is able to retrieve the current epoch from the `xxx`
/// function.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn epoch_provide(input: impl MessageStream) {
    pin_mut!(input);

    let mut epoch = None;

    while let Some(message) = if let Some(epoch) = epoch {
        EPOCH.scope(epoch, input.try_next()).await?
    } else {
        input.try_next().await?
    } {
        if let Message::Barrier(b) = &message {
            epoch = Some(b.epoch);
        }
        yield message;
    }
}
