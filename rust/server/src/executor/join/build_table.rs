use crate::executor::join::chunked_data::{ChunkedData, RowId};
use crate::executor::BoxedExecutor;
use risingwave_common::array::{DataChunk, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};

type InnerTable = Vec<DataChunk>;

/// [`BuildTable`] contains the tuple to be probed. It is also called inner relation in join.
/// `inner_table` is a buffer for all data. For all probe key, directly fetch data in `inner_table`
/// without call executor. The executor is only called when building `inner_table`.
pub(crate) struct BuildTable {
    data_source: BoxedExecutor,
    /// Buffering of inner table. TODO: Spill to disk or more fine-grained memory management to
    /// avoid OOM.
    inner_table: InnerTable,
    /// Pos of chunk in inner table. Tracks current probing progress.
    chunk_idx: usize,
    row_idx: usize,

    /// Used only when join remaining is required after probing.
    ///
    /// See [`JoinType::need_join_remaining`]
    build_matched: Option<ChunkedData<bool>>,
}

impl BuildTable {
    pub fn new(data_source: BoxedExecutor) -> Self {
        Self {
            data_source,
            inner_table: vec![],
            chunk_idx: 0,
            build_matched: None,
            row_idx: 0,
        }
    }

    /// Called in the first probe and load all data of inner relation into buffer.
    pub async fn load_data(&mut self) -> Result<()> {
        self.data_source.open().await?;
        while let Some(chunk) = self.data_source.next().await? {
            self.inner_table.push(chunk.compact()?);
        }
        self.build_matched = Some(ChunkedData::<bool>::with_chunk_sizes(
            self.inner_table.iter().map(|c| c.capacity()),
        )?);
        self.data_source.close().await
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
        if self.chunk_idx < self.inner_table.len() {
            Some(&self.inner_table[self.chunk_idx])
        } else {
            None
        }
    }

    pub fn get_current_row_ref(&self) -> Option<RowRef<'_>> {
        if self.chunk_idx >= self.inner_table.len() {
            return None;
        }

        if self.row_idx >= self.inner_table[self.chunk_idx].capacity() {
            return None;
        }

        Some(self.inner_table[self.chunk_idx].row_at_unchecked_vis(self.row_idx))
    }

    pub fn get_current_row_id(&self) -> RowId {
        RowId::new(self.chunk_idx, self.row_idx)
    }

    pub fn advance_chunk(&mut self) {
        self.chunk_idx += 1;
    }

    pub fn advance_row(&mut self) {
        self.row_idx += 1;
        while self.chunk_idx < self.inner_table.len()
            && self.row_idx >= self.inner_table[self.chunk_idx].capacity()
        {
            self.row_idx = 0;
            self.chunk_idx += 1;
        }
    }

    pub fn reset_chunk(&mut self) {
        self.chunk_idx = 0;
    }

    pub fn get_chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    pub fn get_schema(&self) -> &Schema {
        self.data_source.schema()
    }
}
