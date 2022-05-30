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

use futures::StreamExt;
use risingwave_common::array::{DataChunk, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};

use crate::executor::join::chunked_data::{ChunkedData, RowId};
use crate::executor::BoxedExecutor;

/// `inner_table` is a buffer for all data. For all probe key, directly fetch data in `inner_table`
/// without call executor. The executor is only called when building `inner_table`.
pub(crate) struct RowLevelIter {
    data_source: Option<BoxedExecutor>,
    /// Buffering of inner table. TODO: Spill to disk or more fine-grained memory management to
    /// avoid OOM.
    data: Vec<DataChunk>,
    schema: Schema,
    /// Pos of chunk in inner table.
    chunk_idx: usize,
    /// Pos of row in current chunk.
    row_idx: usize,

    /// Used only when join remaining is required after probing (build side).
    /// See [`super::JoinType::need_join_remaining`]
    build_matched: Option<ChunkedData<bool>>,
    /// Whether current row has found matched tuples. Used in outer join (probe side).
    cur_row_matched: bool,
}

impl RowLevelIter {
    pub fn new(data_source: BoxedExecutor) -> Self {
        let schema = data_source.schema().clone();
        Self {
            data_source: Some(data_source),
            data: vec![],
            schema,
            chunk_idx: 0,
            build_matched: None,
            row_idx: 0,
            cur_row_matched: false,
        }
    }

    /// Called in the first probe and load all data of inner relation into buffer.
    pub async fn load_data(&mut self) -> Result<()> {
        let mut source_stream = self.data_source.take().unwrap().execute();
        while let Some(chunk) = source_stream.next().await {
            let chunk = chunk?;
            // Assuming all data are visible.
            if chunk.cardinality() > 0 {
                self.data.push(chunk.compact()?);
            }
        }
        self.build_matched = Some(ChunkedData::<bool>::with_chunk_sizes(
            self.data.iter().map(|c| c.capacity()),
        )?);
        Ok(())
    }

    /// Copied from hash join. Consider remove the duplication.
    pub fn set_build_matched(&mut self, build_row_id: RowId) -> Result<()> {
        match self.build_matched {
            Some(ref mut flags) => {
                flags[build_row_id] = true;
                Ok(())
            }
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    pub fn is_build_matched(&self, build_row_id: RowId) -> Result<bool> {
        match self.build_matched {
            Some(ref flags) => Ok(flags[build_row_id]),
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    pub fn get_current_chunk(&self) -> Option<&DataChunk> {
        if self.chunk_idx < self.data.len() {
            Some(&self.data[self.chunk_idx])
        } else {
            None
        }
    }

    pub fn get_current_row_ref(&self) -> Option<RowRef<'_>> {
        if self.chunk_idx >= self.data.len() {
            return None;
        }

        if self.row_idx >= self.data[self.chunk_idx].capacity() {
            return None;
        }

        Some(self.data[self.chunk_idx].row_at_unchecked_vis(self.row_idx))
    }

    pub fn get_current_row_id(&self) -> RowId {
        RowId::new(self.chunk_idx, self.row_idx)
    }

    pub fn advance_chunk(&mut self) {
        self.chunk_idx += 1;
    }

    pub fn advance_row(&mut self) {
        self.row_idx += 1;
        // if current chunk is exhausted, advance to next non-zero chunk.
        while self.chunk_idx < self.data.len()
            && self.row_idx >= self.data[self.chunk_idx].capacity()
        {
            self.row_idx = 0;
            self.chunk_idx += 1;
        }
        // The default is unmatched for new row.
        self.set_cur_row_matched(false);
    }

    pub fn reset_chunk(&mut self) {
        self.chunk_idx = 0;
    }

    pub fn get_chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn set_cur_row_matched(&mut self, val: bool) {
        self.cur_row_matched = val;
    }

    pub fn get_cur_row_matched(&mut self) -> bool {
        self.cur_row_matched
    }
}
