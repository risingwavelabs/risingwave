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

use crate::source::{SplitImpl, SplitMetaData};

/// # Batch Refreshable Source
///
/// A batch refreshable source can be refreshed - reload all data from the source, e.g., re-run a `SELECT *` query from the source.
/// The reloaded data will be handled by `RefreshableMaterialize` to calculate a diff to send to downstream.
///
/// See <https://github.com/risingwavelabs/risingwave/issues/22690> for the whole picture of the user journey.
///
/// ## Failover
///
/// Batch source is considered stateless. i.e., it's consumption progress is not recorded, and cannot be resumed.
/// The split metadata just represent "how to load the data".
///
/// - On startup, `SourceExecutor` will load data.
/// - On `RefreshStart` barrier (from `REFRESH TABLE t` SQL command), it will re-load data.
/// - On recovery, it will *do nothing*, regardless of whether it's in the middle of loading data or not before crash.
pub trait BatchSourceSplit: SplitMetaData {
    fn finished(&self) -> bool;
    /// Mark the source as finished. Called after the source is exhausted.
    /// Then `SourceExecutor` will report to meta to send a `LoadFinish` barrier,
    /// and the `RefreshableMaterialize` will begin to calculate the diff.
    fn finish(&mut self);
    /// Refresh the source to make it ready for re-run.
    /// Called when receiving `RefreshStart` barrier.
    fn refresh(&mut self);
}

pub enum BatchSourceSplitImpl {}

/// See [`BatchSourceSplit`] for more details.
impl BatchSourceSplitImpl {
    pub fn finished(&self) -> bool {
        unreachable!()
    }

    pub fn finish(&mut self) {
        tracing::info!("finishing batch source split");
        unreachable!()
    }

    pub fn refresh(&mut self) {
        tracing::info!("refreshing batch source split");
        unreachable!()
    }
}

impl From<BatchSourceSplitImpl> for SplitImpl {
    fn from(batch_split: BatchSourceSplitImpl) -> Self {
        match batch_split {}
    }
}
