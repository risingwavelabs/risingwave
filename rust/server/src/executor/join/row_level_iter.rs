use crate::executor::BoxedExecutor;
use risingwave_common::array::{DataChunk, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

/// This is designed for nested loop join/sort merge join to support row level iteration.
/// TODO: merge this one with [`BuildTable`].
pub struct RowLevelIter {
    outer: BoxedExecutor,
    cur_chunk: Option<DataChunk>,
    /// The row index to read in current chunk.
    row_idx: usize,
    /// Whether current row has found matched tuples. Used in outer join.
    cur_row_matched: bool,
}

impl RowLevelIter {
    pub(crate) fn new(outer: BoxedExecutor) -> Self {
        Self {
            outer,
            cur_chunk: None,
            row_idx: 0,
            cur_row_matched: false,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.outer.open().await?;
        self.cur_chunk = self
            .outer
            .next()
            .await?
            .map(|chunk| chunk.compact().unwrap());
        Ok(())
    }

    /// Return the current outer tuple.
    pub fn current_row_ref(&self) -> Result<Option<(RowRef<'_>, bool)>> {
        match &self.cur_chunk {
            Some(chunk) => {
                if self.row_idx < chunk.capacity() {
                    Some(chunk.row_at(self.row_idx)).transpose()
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Get current tuple directly without None check.
    pub fn current_row_ref_unchecked(&self) -> RowRef<'_> {
        self.current_row_ref().unwrap().unwrap().0
    }

    /// Get current tuple without know the visibility.
    pub fn current_row_ref_unchecked_vis(&self) -> Result<Option<RowRef<'_>>> {
        Ok(self.current_row_ref()?.map(|tuple| tuple.0))
    }

    /// Try advance to next outer tuple. If it is Done but still invoked, it's
    /// a developer error.
    pub async fn advance(&mut self) -> Result<()> {
        self.row_idx += 1;
        match &self.cur_chunk {
            Some(chunk) => {
                if self.row_idx >= chunk.capacity() {
                    self.cur_chunk = self.outer.next().await?;
                    self.row_idx = 0;
                }
            }
            None => {
                unreachable!("Should never advance while no more data to scan")
            }
        };
        Ok(())
    }

    pub async fn clean(&mut self) -> Result<()> {
        self.outer.close().await
    }

    pub fn get_schema(&self) -> &Schema {
        self.outer.schema()
    }

    pub fn set_cur_row_matched(&mut self, val: bool) {
        self.cur_row_matched = val;
    }

    pub fn get_cur_row_matched(&mut self) -> bool {
        self.cur_row_matched
    }
}
