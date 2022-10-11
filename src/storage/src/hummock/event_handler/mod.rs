// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::pin_version_response;
use tokio::sync::oneshot;

use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::HummockResult;
use crate::store::SyncResult;

mod hummock_event_handler;
pub use hummock_event_handler::{BufferTracker, HummockEventHandler};

#[derive(Debug)]
pub struct BufferWriteRequest {
    pub batch: SharedBufferBatch,
    pub epoch: HummockEpoch,
    pub grant_sender: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum HummockEvent {
    /// Notify that we may flush the shared buffer.
    BufferMayFlush,

    /// An epoch is going to be synced. Once the event is processed, there will be no more flush
    /// task on this epoch. Previous concurrent flush task join handle will be returned by the join
    /// handle sender.
    SyncEpoch {
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    },

    /// Clear shared buffer and reset all states
    Clear(oneshot::Sender<()>),

    Shutdown,

    VersionUpdate(pin_version_response::Payload),
}
