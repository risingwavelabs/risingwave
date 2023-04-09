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

use std::sync::LazyLock;

use futures::{pin_mut, Stream, StreamExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// Spawn the data generator to a dedicated runtime, returns a channel receiver
/// for acquiring the generated data. This is used for the [`DatagenSplitReader`] and
/// [`NexmarkSplitReader`] in case that they are CPU intensive and may block the streaming actors.
pub fn spawn_data_generation_stream<T: Send + 'static>(
    stream: impl Stream<Item = T> + Send + 'static,
    buffer_size: usize,
) -> impl Stream<Item = T> + Send + 'static {
    static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("risingwave-data-generation")
            .enable_all()
            .build()
            .expect("failed to build data-generation runtime")
    });

    let (generation_tx, generation_rx) = mpsc::channel(buffer_size);
    RUNTIME.spawn(async move {
        pin_mut!(stream);
        while let Some(result) = stream.next().await {
            if generation_tx.send(result).await.is_err() {
                tracing::warn!("failed to send next event to reader, exit");
                break;
            }
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(generation_rx)
}
