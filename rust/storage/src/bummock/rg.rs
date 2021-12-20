use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;

/// `MemRowGroup` implements the in-memory part `RowGroup` of `Bummock` design.
/// The in-memory information will be encoded to underlying on-disk formats.
///
/// Design Note: Keeping `tuple_id` or not is a design choice. We tend to keep it
/// here for a few reasons:
/// 1. Separate with user defined keys because they are variant and we don't know if
/// we should optimize for partitions, sorting, ranges, or points.
/// 2. Easier for supporting constraints. Constraints (fk, uk) are harder to support when
/// there are no indexes. With `tuple_id`s, constraints would be easier to support with
/// indexes.
/// 3. Various indexes support if we want to build a `RockSet` style of "Converged Index"es.
#[derive(Debug)]
pub struct MemRowGroup {
    /// `tuple_ids` is the hidden identifier of a tuple.
    tuple_ids: Vec<u64>,

    /// `column_ids` keeps the list of column ids.
    column_ids: Arc<[i32]>,

    /// `data_chunks` stores the payload of the `RowGroup`.
    data_chunks: Vec<DataChunkRef>,

    /// `dbmp` is the delete map of deletions with the same range of transactions happened.
    dbmp: Option<Bitmap>,
    // TODO: [xiangyhu] footers
}

/// TODO: [xiangyhu] replace this with Parquet
#[derive(Debug)]
pub struct StagedRowGroup {}

#[derive(Debug)]
pub struct PartitionedRowGroup {}

impl MemRowGroup {
    pub fn new(column_count: usize) -> Self {
        Self {
            tuple_ids: Vec::new(),
            column_ids: (0..column_count as i32).collect_vec().into(),
            data_chunks: Vec::new(),
            dbmp: None,
        }
    }

    /// Append data chunks to an in-memory `RowGroup` and returns a tuple of `(end_tuple_id,
    /// cardinality)` `start_tuple_id` is the starting tuple id of the operation.
    /// `datachunk` is the data to ingest.
    pub fn append_data(
        &mut self,
        start_tuple_id: u64,
        datachunk: DataChunk,
    ) -> Result<(u64, usize)> {
        // push data
        let cardinality = datachunk.cardinality();
        self.data_chunks.push(Arc::new(datachunk));

        // push tuple ids
        let end_tuple_id: u64 = start_tuple_id + cardinality as u64;
        self.tuple_ids
            .extend((start_tuple_id..end_tuple_id).collect::<Vec<u64>>());

        Ok((end_tuple_id, cardinality))
    }

    /// Get data chunks of this `RowGroup`
    pub fn get_data(&self) -> Result<Vec<DataChunkRef>> {
        Ok(self.data_chunks.clone())
    }

    /// column related
    pub fn columns_count() {
        todo!();
    }

    pub fn column_meta() {
        todo!();
    }

    /// tuple related
    pub fn tuples_count() {
        todo!();
    }

    pub fn total_size_in_bytes() {
        todo!();
    }
}

pub type MemRowGroupRef = Arc<MemRowGroup>;

pub type StagedRowGroupRef = Arc<StagedRowGroup>;

pub type PartitionedRowGroupRef = Arc<PartitionedRowGroup>;
