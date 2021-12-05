use super::barrier_align::{AlignedMessage, BarrierAligner};
use super::{Executor, Message};
use async_trait::async_trait;
use risingwave_common::array::{ArrayBuilderImpl, Op, Row, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataTypeRef, ToOwnedDatum};
use risingwave_common::{
    array::{column::Column, DataChunk},
    error::Result,
};
use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

// The `JoinType` and `SideType` are to mimic a enum, because currently
// enum is not supported in const generic.
// TODO: Use enum to replace this once `feature(adt_const_params)` get completed
// https://github.com/rust-lang/rust/issues/44580
// https://blog.rust-lang.org/inside-rust/2021/09/06/Splitting-const-generics.html
type JoinTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
pub mod JoinType {
    use super::JoinTypePrimitive;
    pub const Inner: JoinTypePrimitive = 0;
    pub const LeftOuter: JoinTypePrimitive = 1;
    pub const RightOuter: JoinTypePrimitive = 2;
    pub const FullOuter: JoinTypePrimitive = 3;
}

/// Build a array and it's corresponding operations.
struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,
    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,
    /// The start position of the columns of the side
    /// stream coming from. If the coming side is the
    /// left, the `update_start_pos` should be 0.
    /// If the coming side is the right, the `update_start_pos`
    /// is the number of columns of the left side.
    update_start_pos: usize,
    /// The start position of the columns of the opposite side
    /// stream coming from. If the coming side is the
    /// left, the `matched_start_pos` should be the number of columns of the left side.
    /// If the coming side is the right, the `matched_start_pos`
    /// should be 0.
    matched_start_pos: usize,
}

impl StreamChunkBuilder {
    fn new(
        capacity: usize,
        data_types: &[DataTypeRef],
        update_start_pos: usize,
        matched_start_pos: usize,
    ) -> Result<Self> {
        let ops = Vec::with_capacity(capacity);
        let column_builders = data_types
            .iter()
            .map(|datatype| datatype.create_array_builder(capacity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            ops,
            column_builders,
            update_start_pos,
            matched_start_pos,
        })
    }

    /// append a row with coming update value and matched value.
    fn append_row(&mut self, op: Op, row_update: &RowRef<'_>, row_matched: &Row) -> Result<()> {
        self.ops.push(op);
        for i in 0..row_update.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(row_update[i])?;
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i])?;
        }
        Ok(())
    }

    /// append a row with coming update value and fill the other side with null.
    fn append_row_update(&mut self, op: Op, row_update: &RowRef<'_>) -> Result<()> {
        self.ops.push(op);
        for i in 0..row_update.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(row_update[i])?;
        }
        for i in 0..self.column_builders.len() - row_update.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&None)?;
        }
        Ok(())
    }

    /// append a row with matched value and fill the coming side with null.
    fn append_row_matched(&mut self, op: Op, row_matched: &Row) -> Result<()> {
        self.ops.push(op);
        for i in 0..self.column_builders.len() - row_matched.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(None)?;
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i])?;
        }
        Ok(())
    }

    // TODO: Remove this `data_types` when we replace columns with arrays
    // https://github.com/singularity-data/risingwave/issues/1373
    fn finish(self, data_types: &[DataTypeRef]) -> Result<StreamChunk> {
        let new_arrays = self
            .column_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;

        let new_columns = new_arrays
            .into_iter()
            .zip(data_types.iter())
            .map(|(array_impl, data_type)| Column::new(Arc::new(array_impl), data_type.to_owned()))
            .collect::<Vec<_>>();

        let new_chunk = StreamChunk {
            columns: new_columns,
            visibility: None,
            ops: self.ops,
        };
        Ok(new_chunk)
    }
}

type SideTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
mod SideType {
    use super::SideTypePrimitive;
    pub const Left: SideTypePrimitive = 0;
    pub const Right: SideTypePrimitive = 1;
}

#[inline(always)]
fn outer_side_keep(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Left)
        || (join_type == JoinType::RightOuter && side_type == SideType::Right)
}

#[inline(always)]
fn outer_side_null(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Right)
        || (join_type == JoinType::RightOuter && side_type == SideType::Left)
}

type HashKeyType = Row;
type HashValueItemType = Row;
type HashValueType = Vec<HashValueItemType>;

pub struct JoinParams {
    /// Indices of the join columns
    key_indices: Vec<usize>,
}

impl JoinParams {
    pub fn new(key_indices: Vec<usize>) -> Self {
        Self { key_indices }
    }
}

struct JoinSide {
    /// Store all data from a one side stream
    ht: HashMap<HashKeyType, HashValueType>,
    /// Indices of the join columns
    key_indices: Vec<usize>,
    /// The date type of each columns to join on
    col_types: Vec<DataTypeRef>,
    /// The start position for the side in output new columns
    start_pos: usize,
}

/// `HashJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct HashJoinExecutor<const T: JoinTypePrimitive> {
    /// Barrier aligner that combines two input streams and aligns their barriers
    aligner: BarrierAligner,
    // TODO: maybe remove `new_column_datatypes` and use schema
    /// the data types of the formed new columns
    new_column_datatypes: Vec<DataTypeRef>,
    /// The schema of the hash join executor
    schema: Schema,
    /// The parameters of the left join executor
    side_l: JoinSide,
    /// The parameters of the right join executor
    side_r: JoinSide,
}

#[async_trait]
impl<const T: JoinTypePrimitive> Executor for HashJoinExecutor<T> {
    async fn next(&mut self) -> Result<Message> {
        match self.aligner.next().await {
            AlignedMessage::Left(message) => match message {
                Ok(chunk) => self.consume_chunk_left(chunk),
                Err(e) => Err(e),
            },
            AlignedMessage::Right(message) => match message {
                Ok(chunk) => self.consume_chunk_right(chunk),
                Err(e) => Err(e),
            },
            AlignedMessage::Barrier(barrier) => Ok(Message::Barrier(barrier)),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<const T: JoinTypePrimitive> HashJoinExecutor<T> {
    pub fn new(
        input_l: Box<dyn Executor>,
        input_r: Box<dyn Executor>,
        params_l: JoinParams,
        params_r: JoinParams,
    ) -> Self {
        let new_column_n = input_l.schema().len() + input_r.schema().len();
        let side_l_column_n = input_l.schema().len();

        let schema_fields = input_r
            .schema()
            .fields
            .iter()
            .cloned()
            .chain(input_l.schema().fields.iter().cloned())
            .collect::<Vec<_>>();

        assert_eq!(schema_fields.len(), new_column_n);

        let new_column_datatypes = schema_fields
            .iter()
            .map(|filed| filed.data_type.clone())
            .collect();
        let col_l_datatypes = input_l
            .schema()
            .fields
            .iter()
            .map(|filed| filed.data_type.clone())
            .collect();
        let col_r_datatypes = input_r
            .schema()
            .fields
            .iter()
            .map(|filed| filed.data_type.clone())
            .collect();

        Self {
            aligner: BarrierAligner::new(input_l, input_r),
            new_column_datatypes,
            schema: Schema {
                fields: schema_fields,
            },
            side_l: JoinSide {
                ht: HashMap::new(),
                key_indices: params_l.key_indices,
                col_types: col_l_datatypes,
                start_pos: 0,
            },
            side_r: JoinSide {
                ht: HashMap::new(),
                key_indices: params_r.key_indices,
                col_types: col_r_datatypes,
                start_pos: side_l_column_n,
            },
        }
    }

    /// the data the hash table and match the coming
    /// data chunk with the executor state
    fn hash_eq_match<'a>(
        key: &Row,
        ht: &'a HashMap<HashKeyType, HashValueType>,
    ) -> Option<&'a HashValueType> {
        ht.get(key)
    }

    fn hash_key_from_row_ref(row: &RowRef, key_indices: &[usize]) -> HashKeyType {
        let mut key = Vec::with_capacity(key_indices.len());
        for i in key_indices {
            key.push(row[*i].to_owned_datum());
        }
        Row(key)
    }

    fn hash_value_item_from_row_ref(row: &RowRef) -> HashValueItemType {
        let mut value = Vec::with_capacity(row.size());
        for i in 0..row.size() {
            value.push(row[i].to_owned_datum());
        }
        Row(value)
    }

    fn consume_chunk_left(&mut self, chunk: StreamChunk) -> Result<Message> {
        self.eq_join_oneside::<{ SideType::Left }>(chunk)
    }

    fn consume_chunk_right(&mut self, chunk: StreamChunk) -> Result<Message> {
        self.eq_join_oneside::<{ SideType::Right }>(chunk)
    }

    fn eq_join_oneside<const S: SideTypePrimitive>(
        &mut self,
        chunk: StreamChunk,
    ) -> Result<Message> {
        let chunk = chunk.compact()?;
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(columns);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        let (side_update, side_match) = if S == SideType::Left {
            (&mut self.side_l, &mut self.side_r)
        } else {
            (&mut self.side_r, &mut self.side_l)
        };

        // TODO: find a better capacity number, the actual array length
        // is likely to be larger than the current capacity
        let capacity = data_chunk.capacity();

        let mut stream_chunk_builder = StreamChunkBuilder::new(
            capacity,
            &self.new_column_datatypes,
            side_update.start_pos,
            side_match.start_pos,
        )?;

        for (row, op) in data_chunk.iter().zip(ops.iter()) {
            let key = Self::hash_key_from_row_ref(&row, &side_update.key_indices);
            let value = Self::hash_value_item_from_row_ref(&row);
            let matched_rows = Self::hash_eq_match(&key, &side_match.ht);
            let mut null_row_updated = false;
            if let Some(matched_rows) = matched_rows {
                match *op {
                    Op::Insert | Op::UpdateInsert => {
                        let entry = side_update.ht.entry(key);
                        let entry = match entry {
                            Entry::Occupied(entry) => entry.into_mut(),
                            // if outer join and not its the first to insert, meaning there must be
                            // corresponding nulls.
                            Entry::Vacant(entry) => {
                                if outer_side_null(T, S) {
                                    for matched_row in matched_rows.iter() {
                                        stream_chunk_builder
                                            .append_row_matched(Op::UpdateDelete, matched_row)?;
                                        stream_chunk_builder.append_row(
                                            Op::UpdateInsert,
                                            &row,
                                            matched_row,
                                        )?;
                                    }
                                    null_row_updated = true;
                                };
                                entry.insert(Default::default())
                            }
                        };
                        entry.push(value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        let mut deleted = false;
                        if let Some(v) = side_update.ht.get_mut(&key) {
                            if let Some(pos) = v.iter().position(|row| *row == value) {
                                v.remove(pos);
                                if outer_side_null(T, S) && v.is_empty() {
                                    for matched_row in matched_rows.iter() {
                                        stream_chunk_builder.append_row(
                                            Op::UpdateDelete,
                                            &row,
                                            matched_row,
                                        )?;
                                        stream_chunk_builder
                                            .append_row_matched(Op::UpdateInsert, matched_row)?;
                                    }
                                    null_row_updated = true;
                                }
                                deleted = true;
                            }
                        }
                        if !deleted {
                            unreachable!("Deleted value does not exist: {:?}", row);
                        }
                    }
                };
                if !outer_side_null(T, S) || !null_row_updated {
                    for matched_row in matched_rows.iter() {
                        assert_eq!(matched_row.size(), side_match.col_types.len());
                        stream_chunk_builder.append_row(*op, &row, matched_row)?;
                    }
                }
            } else {
                // if there are no matched rows, just update the hash table
                match *op {
                    Op::Insert | Op::UpdateInsert => {
                        side_update.ht.entry(key).or_default().push(value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(v) = side_update.ht.get_mut(&key) {
                            if let Some(pos) = v.iter().position(|row| *row == value) {
                                v.remove(pos);
                            }
                        }
                    }
                };
                // if it's outer join and the side needs maintained.
                if outer_side_keep(T, S) {
                    stream_chunk_builder.append_row_update(*op, &row)?;
                }
            }
        }

        let new_chunk = stream_chunk_builder.finish(&self.new_column_datatypes)?;

        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use risingwave_common::{array::*, column_nonnull};

    use super::{HashJoinExecutor, JoinParams, JoinType};
    use crate::stream_op::test_utils::MockAsyncSource;
    use crate::stream_op::{Barrier, Executor, Message};
    use crate::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::Int64Type;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_streaming_hash_inner_join() {
        let chunk_l1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk_l2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 3] },
                column_nonnull! { I64Array, Int64Type, [8, 8] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let chunk_r1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [2, 4, 6] },
                column_nonnull! { I64Array, Int64Type, [7, 8, 9] },
            ],
            visibility: None,
        };
        let chunk_r2 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 6] },
                column_nonnull! { I64Array, Int64Type, [10, 11] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::new(schema.clone(), rx_l);
        let source_r = MockAsyncSource::new(schema.clone(), rx_r);

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<{ JoinType::Inner }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops.len(), 0);
            assert_eq!(chunk.columns.len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.columns[i].array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops.len(), 0);
            assert_eq!(chunk.columns.len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.columns[i].array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(7)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_barrier() {
        let chunk_l1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk_l2 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [6, 3] },
                column_nonnull! { I64Array, Int64Type, [8, 8] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let chunk_r1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [2, 4, 6] },
                column_nonnull! { I64Array, Int64Type, [7, 8, 9] },
            ],
            visibility: None,
        };
        let chunk_r2 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 6] },
                column_nonnull! { I64Array, Int64Type, [10, 11] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::new(schema.clone(), rx_l);
        let source_r = MockAsyncSource::new(schema.clone(), rx_r);

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<{ JoinType::Inner }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops.len(), 0);
            assert_eq!(chunk.columns.len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.columns[i].array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push a barrier to left side
        MockAsyncSource::push_barrier(&mut tx_l, 0, false);

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);

        // join the first right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(7)]
            );
        } else {
            unreachable!();
        }

        // push a barrier to right side
        MockAsyncSource::push_barrier(&mut tx_r, 0, false);

        // get the aligned barrier here
        assert!(matches!(
            hash_join.next().await.unwrap(),
            Message::Barrier(Barrier {
                epoch: 0,
                stop: false
            })
        ));

        // join the 2nd left chunk
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(8)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(9)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3), Some(6)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(6), Some(8), Some(8)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3), Some(6)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10), Some(11)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join() {
        let chunk_l1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk_l2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 3] },
                column_nonnull! { I64Array, Int64Type, [8, 8] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let chunk_r1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [2, 4, 6] },
                column_nonnull! { I64Array, Int64Type, [7, 8, 9] },
            ],
            visibility: None,
        };
        let chunk_r2 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 6] },
                column_nonnull! { I64Array, Int64Type, [10, 11] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::new(schema.clone(), rx_l);
        let source_r = MockAsyncSource::new(schema.clone(), rx_r);

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<{ JoinType::LeftOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(3)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(4), Some(5), Some(6)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(8), Some(8)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::UpdateDelete, Op::UpdateInsert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(2)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(2)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(7)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::UpdateDelete, Op::UpdateInsert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(6), Some(6)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(3)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join() {
        let chunk_l1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk_l2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 3] },
                column_nonnull! { I64Array, Int64Type, [8, 8] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let chunk_r1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [2, 4, 6] },
                column_nonnull! { I64Array, Int64Type, [7, 8, 9] },
            ],
            visibility: None,
        };
        let chunk_r2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [5, 5] },
                column_nonnull! { I64Array, Int64Type, [10, 10] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::new(schema.clone(), rx_l);
        let source_r = MockAsyncSource::new(schema.clone(), rx_r);

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<{ JoinType::RightOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops.len(), 0);
            assert_eq!(chunk.columns.len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.columns[i].array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops.len(), 0);
            assert_eq!(chunk.columns.len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.columns[i].array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), None, None]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), None, None]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(4), Some(6)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(7), Some(8), Some(9)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join() {
        let chunk_l1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk_l2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [3, 3] },
                column_nonnull! { I64Array, Int64Type, [8, 8] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let chunk_r1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [2, 4, 6] },
                column_nonnull! { I64Array, Int64Type, [7, 8, 9] },
            ],
            visibility: None,
        };
        let chunk_r2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [5, 5] },
                column_nonnull! { I64Array, Int64Type, [10, 10] },
            ],
            visibility: Some((vec![true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::new(schema.clone(), rx_l);
        let source_r = MockAsyncSource::new(schema.clone(), rx_r);

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<{ JoinType::FullOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(3)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(4), Some(5), Some(6)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(8), Some(8)]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(
                chunk.ops,
                vec![Op::UpdateDelete, Op::UpdateInsert, Op::Insert, Op::Insert]
            );
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(2), None, None]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5), None, None]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(2), Some(4), Some(6)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(7), Some(8), Some(9)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops, vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns.len(), 4);
            assert_eq!(
                chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10)]
            );
        } else {
            unreachable!();
        }
    }
}
