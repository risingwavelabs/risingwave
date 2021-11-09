use super::{Executor, Message, Op, StreamChunk};
use async_trait::async_trait;
use risingwave_common::array::{Row, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataTypeRef, ToOwnedDatum};
use risingwave_common::{
    array::{column::Column, DataChunk},
    error::Result,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::select;

pub enum JoinType {
    Equal,
}

type HashKeyType = Row;
type HashValueType = Vec<Row>;

pub struct JoinParams {
    /// Indices of the join columns
    key_indices: Vec<usize>,
}

struct JoinSide {
    /// Store all data from a one side stream
    ht: HashMap<HashKeyType, HashValueType>,
    /// Indices of the join columns
    key_indices: Vec<usize>,
    /// The date type of each colums to join on
    col_types: Vec<DataTypeRef>,
    /// The start position for the side in output new columns
    start_pos: usize,
}
/// `HashJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct HashJoinExecutor {
    /// The type of the join executor
    join_type: JoinType,
    /// The input from the left executor
    input_l: Box<dyn Executor>,
    /// The input from the right executor
    input_r: Box<dyn Executor>,
    // TODO: maybe remove `new_column_datatypes` and use schema
    /// the data types of the formed new colums
    new_column_datatypes: Vec<DataTypeRef>,
    /// The schema of the hash join executor
    schema: Schema,
    /// The parameters of the left join executor
    side_l: JoinSide,
    /// The parameters of the right join executor
    side_r: JoinSide,
    /// The barrier state of the executor
    state: BarrierWaitState,
}

impl JoinParams {
    pub fn new(key_indices: Vec<usize>) -> Self {
        Self { key_indices }
    }
}

#[derive(PartialEq)]
pub enum BarrierWaitState {
    Right,
    Left,
    Either,
}

#[async_trait]
impl Executor for HashJoinExecutor {
    async fn next(&mut self) -> Result<Message> {
        select! {
          message = self.input_l.next(), if self.state != BarrierWaitState::Right => {
            match message {
              Ok(message) => match message {
                Message::Chunk(chunk) => self.consume_chunk_left(chunk),
                Message::Barrier{epoch, stop} => {
                  match self.state {
                    BarrierWaitState::Left => {self.state = BarrierWaitState::Either; Ok(Message::Barrier{epoch, stop})},
                    BarrierWaitState::Either => {self.state = BarrierWaitState::Right; self.next().await},
                    _ => unreachable!("Should not reach this barrier state"),
                  }
                },
                // TODO: we should wait terminate from both side, but terminate will be delete later.
                Message::Terminate => Ok(Message::Terminate),
              },
              Err(e) => Err(e),
            }
          },
          message = self.input_r.next(), if self.state != BarrierWaitState::Left => {
            match message {
              Ok(message) => match message {
                Message::Chunk(chunk) => self.consume_chunk_right(chunk),
                Message::Barrier{epoch, stop} => {
                  match self.state {
                    BarrierWaitState::Right => {self.state = BarrierWaitState::Either; Ok(Message::Barrier{epoch, stop})},
                    BarrierWaitState::Either => {self.state = BarrierWaitState::Left; self.next().await},
                    _ => unreachable!("Should not reach this barrier state"),
                  }
                },
                // TODO: we should wait terminate from both side, but terminate will be delete later.
                Message::Terminate => Ok(Message::Terminate),
              },
              Err(e) => Err(e),
            }
          }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl HashJoinExecutor {
    fn new(
        join_type: JoinType,
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
            join_type,
            input_l,
            input_r,
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
            state: BarrierWaitState::Either,
        }
    }

    /// update the hash table
    fn update_ht(
        rows: &DataChunk,
        ops: &[Op],
        key_indices: &[usize],
        ht: &mut HashMap<HashKeyType, HashValueType>,
    ) {
        for (row, op) in rows.iter().zip(ops.iter()) {
            let mut key = Vec::with_capacity(key_indices.len());
            let mut value = Vec::with_capacity(row.size());
            for i in 0..row.size() {
                if key_indices.contains(&i) {
                    key.push(row[i].to_owned_datum());
                }
                value.push(row[i].to_owned_datum());
            }
            let key = Row(key);
            let value = Row(value);
            match *op {
                Op::Insert | Op::UpdateInsert => ht.entry(key).or_default().push(value),
                Op::Delete | Op::UpdateDelete => {
                    if let Some(v) = ht.get_mut(&key) {
                        if let Some(pos) = v.iter().position(|row| *row == value) {
                            v.remove(pos);
                        }
                    }
                }
            };
        }
    }

    /// match the coming data chunk with the executor state
    fn hash_join_eq_match<'a>(
        row: &'a RowRef<'a>,
        ht: &'a HashMap<HashKeyType, HashValueType>,
        key_indices: &'a [usize],
    ) -> Option<&'a HashValueType> {
        let mut key = Vec::with_capacity(key_indices.len());
        for i in 0..row.size() {
            if key_indices.contains(&i) {
                key.push(row[i].to_owned_datum());
            }
        }
        ht.get(&Row(key))
    }

    fn consume_chunk_left(&mut self, chunk: StreamChunk) -> Result<Message> {
        match self.join_type {
            JoinType::Equal => Self::eq_join_oneside(
                chunk,
                &self.new_column_datatypes,
                &mut self.side_l,
                &self.side_r,
            ),
        }
    }

    fn consume_chunk_right(&mut self, chunk: StreamChunk) -> Result<Message> {
        match self.join_type {
            JoinType::Equal => Self::eq_join_oneside(
                chunk,
                &self.new_column_datatypes,
                &mut self.side_r,
                &self.side_l,
            ),
        }
    }

    fn eq_join_oneside(
        chunk: StreamChunk,
        new_column_datatypes: &[DataTypeRef],
        side_update: &mut JoinSide,
        side_match: &JoinSide,
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

        Self::update_ht(
            &data_chunk,
            &ops,
            &side_update.key_indices,
            &mut side_update.ht,
        );

        // TODO: find a better capacity number, the actual array length
        // is likely to be larger than the current capacity
        let capacity = data_chunk.capacity();
        let mut new_ops = Vec::with_capacity(capacity);

        let new_column_builders = new_column_datatypes
            .iter()
            .map(|datatype| datatype.clone().create_array_builder(capacity));

        let mut new_column_builders = new_column_builders
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        for (row, op) in data_chunk.iter().zip(ops.iter()) {
            assert_eq!(row.size(), side_update.col_types.len());
            let matched_rows =
                Self::hash_join_eq_match(&row, &side_match.ht, &side_match.key_indices);
            if let Some(matched_rows) = matched_rows {
                for matched_row in matched_rows.iter() {
                    new_ops.push(*op);
                    assert_eq!(matched_row.size(), side_match.col_types.len());
                    for i in 0..side_update.col_types.len() {
                        new_column_builders[i + side_update.start_pos].append_datum_ref(row[i])?;
                    }
                    for i in 0..side_match.col_types.len() {
                        new_column_builders[i + side_match.start_pos]
                            .append_datum(&matched_row[i])?;
                    }
                }
            }
        }

        let new_arrays = new_column_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let new_columns = new_arrays
            .into_iter()
            .zip(new_column_datatypes.iter())
            .map(|(array_impl, data_type)| Column::new(Arc::new(array_impl), data_type.to_owned()))
            .collect::<Vec<_>>();

        let new_chunk = StreamChunk {
            columns: new_columns,
            visibility: None,
            ops: new_ops,
        };

        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use risingwave_common::array::I64Array;
    use risingwave_common::array::*;

    use super::{HashJoinExecutor, JoinParams, JoinType};
    use crate::stream_op::test_utils::MockAsyncSource;
    use crate::stream_op::{Executor, Message, Op, StreamChunk};
    use crate::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::Int64Type;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_hash_join() {
        let chunk_l1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
                column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk_l2 = StreamChunk {
            ops: vec![Op::UpdateInsert, Op::UpdateDelete],
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
            ops: vec![Op::Delete, Op::Delete],
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

        let join_type = JoinType::Equal;

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::new(
            join_type,
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
            assert_eq!(chunk.ops, vec![Op::Delete]);
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
    async fn test_hash_join_with_barrier() {
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

        let join_type = JoinType::Equal;

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::new(
            join_type,
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
        if let Message::Barrier { epoch, stop } = hash_join.next().await.unwrap() {
            assert_eq!(epoch, 0);
            assert!(!stop);
        } else {
            unreachable!();
        }

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
}
