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

pub mod in_mem;
pub mod kv_log_store;

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_storage::error::StorageError;

#[derive(thiserror::Error, Debug)]
pub enum LogStoreError {
    #[error("EndOfLogStream")]
    EndOfLogStream,

    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),

    #[error("Value encoding error: {0}")]
    ValueEncoding(#[from] ValueEncodingError),
}

pub type LogStoreResult<T> = Result<T, LogStoreError>;

#[derive(Debug)]
pub enum LogStoreReadItem {
    StreamChunk(StreamChunk),
    Barrier { is_checkpoint: bool },
    UpdateVnodeBitmap(Arc<Bitmap>),
}

pub trait LogWriter {
    /// Initialize the log writer with an epoch
    fn init(&mut self, epoch: EpochPair) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Write a stream chunk to the log writer
    fn write_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Mark current epoch as finished and sealed, and flush the unconsumed log data.
    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Update the vnode bitmap of the log writer
    fn update_vnode_bitmap(
        &mut self,
        new_vnodes: Arc<Bitmap>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;
}

pub trait LogReader {
    /// Initialize the log reader. Usually function as waiting for log writer to be initialized.
    fn init(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Emit the next item.
    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_;

    /// Mark that all items emitted so far have been consumed and it is safe to truncate the log
    /// from the current offset.
    fn truncate(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_;
}

pub trait LogStoreFactory: 'static {
    type Reader: LogReader + Send + 'static;
    type Writer: LogWriter + Send + 'static;

    fn build(self) -> impl Future<Output = (Self::Reader, Self::Writer)> + Send;
}
