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

use crate::source::filesystem::opendal_source::BatchPosixFsSplit;
use crate::source::{SplitImpl, SplitMetaData};

/// # Batch Refreshable Source
///
/// A batch refreshable source can be refreshed - reload all data from the source, e.g., re-run a `SELECT *` query from the source.
/// The reloaded data will be handled by `RefreshableMaterialize` to calculate a diff to send to downstream.
pub trait BatchSourceSplit: SplitMetaData {
    fn finished(&self) -> bool;
    /// Mark the source as finished. Called after the source is exhausted.
    /// A `LoadFinish` signal will be sent by `SourceExecutor`, and the `RefreshableMaterialize` will begin to calculate the diff.
    fn finish(&mut self);
    /// Refresh the source to make it ready for re-run.
    /// A `Refresh` signal will be sent, and the `RefreshableMaterialize` will begin to write the new data into a temporary staging table.
    fn refresh(&mut self);
}

pub enum BatchSourceSplitImpl {
    BatchPosixFs(BatchPosixFsSplit),
}

/// See [`BatchSourceSplit`] for more details.
impl BatchSourceSplitImpl {
    pub fn finished(&self) -> bool {
        match self {
            BatchSourceSplitImpl::BatchPosixFs(split) => split.finished(),
        }
    }

    pub fn finish(&mut self) {
        tracing::info!("finishing batch source split");
        match self {
            BatchSourceSplitImpl::BatchPosixFs(split) => split.finish(),
        }
    }

    pub fn refresh(&mut self) {
        tracing::info!("refreshing batch source split");
        match self {
            BatchSourceSplitImpl::BatchPosixFs(split) => split.refresh(),
        }
    }
}

impl From<BatchSourceSplitImpl> for SplitImpl {
    fn from(batch_split: BatchSourceSplitImpl) -> Self {
        match batch_split {
            BatchSourceSplitImpl::BatchPosixFs(split) => SplitImpl::BatchPosixFs(split),
        }
    }
}
