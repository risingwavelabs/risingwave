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
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Array, ArrayRef, DataChunk, Op, Row, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::hash::HashKey;
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_expr::expr::RowExpression;
use risingwave_storage::{Keyspace, StateStore};

use super::barrier_align::*;
use super::error::StreamExecutorError;
use super::managed_state::join::*;
use super::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef};
use crate::common::StreamChunkBuilder;

pub const JOIN_CACHE_SIZE: usize = 1 << 16;

/// The `JoinType` and `SideType` are to mimic a enum, because currently
/// enum is not supported in const generic.
// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/95174) get completed.
pub type JoinTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
pub mod JoinType {
    use super::JoinTypePrimitive;
    pub const Inner: JoinTypePrimitive = 0;
    pub const LeftOuter: JoinTypePrimitive = 1;
    pub const RightOuter: JoinTypePrimitive = 2;
    pub const FullOuter: JoinTypePrimitive = 3;
}

pub type SideTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
pub mod SideType {
    use super::SideTypePrimitive;
    pub const Left: SideTypePrimitive = 0;
    pub const Right: SideTypePrimitive = 1;
}

const fn outer_side_keep(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Left)
        || (join_type == JoinType::RightOuter && side_type == SideType::Right)
}

const fn outer_side_null(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Right)
        || (join_type == JoinType::RightOuter && side_type == SideType::Left)
}

pub struct JoinParams {
    /// Indices of the join columns
    pub key_indices: Vec<usize>,
}

impl JoinParams {
    pub fn new(key_indices: Vec<usize>) -> Self {
        Self { key_indices }
    }
}

struct JoinSide<K: HashKey, S: StateStore> {
    /// Store all data from a one side stream
    ht: JoinHashMap<K, S>,
    /// Indices of the join key columns
    key_indices: Vec<usize>,
    /// The primary key indices of this side, used for state store
    pk_indices: Vec<usize>,
    /// The date type of each columns to join on
    col_types: Vec<DataType>,
    /// The start position for the side in output new columns
    start_pos: usize,
    /// The join side operates on this keyspace.
    keyspace: Keyspace<S>,
}

impl<K: HashKey, S: StateStore> std::fmt::Debug for JoinSide<K, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinSide")
            .field("key_indices", &self.key_indices)
            .field("pk_indices", &self.pk_indices)
            .field("col_types", &self.col_types)
            .field("start_pos", &self.start_pos)
            .finish()
    }
}

impl<K: HashKey, S: StateStore> JoinSide<K, S> {
    fn is_dirty(&self) -> bool {
        self.ht.values().any(|state| state.is_dirty())
    }

    #[allow(dead_code)]
    fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while states of hash join are dirty"
        );

        // TODO: not working with rearranged chain
        // self.ht.clear();
    }
}

/// `HashJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct HashJoinExecutor<K: HashKey, S: StateStore, const T: JoinTypePrimitive> {
    /// Left input executor.
    input_l: Option<BoxedExecutor>,
    /// Right input executor.
    input_r: Option<BoxedExecutor>,
    /// the data types of the formed new columns
    output_data_types: Vec<DataType>,
    /// The schema of the hash join executor
    schema: Schema,
    /// The primary key indices of the schema
    pk_indices: PkIndices,
    /// The parameters of the left join executor
    side_l: JoinSide<K, S>,
    /// The parameters of the right join executor
    side_r: JoinSide<K, S>,
    /// Optional non-equi join conditions
    cond: Option<RowExpression>,
    /// Identity string
    identity: String,
    /// Epoch
    epoch: u64,

    #[allow(dead_code)]
    /// Logical Operator Info
    op_info: String,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> std::fmt::Debug
    for HashJoinExecutor<K, S, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashJoinExecutor")
            .field("join_type", &T)
            .field("input_left", &self.input_l.as_ref().unwrap().identity())
            .field("input_right", &self.input_r.as_ref().unwrap().identity())
            .field("side_l", &self.side_l)
            .field("side_r", &self.side_r)
            .field("pk_indices", &self.pk_indices)
            .field("schema", &self.schema)
            .field("output_data_types", &self.output_data_types)
            .finish()
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> Executor for HashJoinExecutor<K, S, T> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive> HashJoinExecutor<K, S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_l: BoxedExecutor,
        input_r: BoxedExecutor,
        params_l: JoinParams,
        params_r: JoinParams,
        pk_indices: PkIndices,
        executor_id: u64,
        cond: Option<RowExpression>,
        op_info: String,
        key_indices: Vec<usize>,
        ks_l: Keyspace<S>,
        ks_r: Keyspace<S>,
    ) -> Self {
        let new_column_n = input_l.schema().len() + input_r.schema().len();
        let side_l_column_n = input_l.schema().len();

        let schema_fields = [
            input_l.schema().fields.clone(),
            input_r.schema().fields.clone(),
        ]
        .concat();

        assert_eq!(schema_fields.len(), new_column_n);

        let output_data_types = schema_fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect();
        let col_l_datatypes = input_l
            .schema()
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect_vec();
        let col_r_datatypes = input_r
            .schema()
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect_vec();

        let pk_indices_l = input_l.pk_indices().to_vec();
        let pk_indices_r = input_r.pk_indices().to_vec();
        Self {
            input_l: Some(input_l),
            input_r: Some(input_r),
            output_data_types,
            schema: Schema {
                fields: schema_fields,
            },
            side_l: JoinSide {
                ht: JoinHashMap::new(
                    JOIN_CACHE_SIZE,
                    pk_indices_l.clone(),
                    params_l.key_indices.clone(),
                    col_l_datatypes.clone(),
                    ks_l.clone(),
                ), // TODO: decide the target cap
                key_indices: params_l.key_indices,
                col_types: col_l_datatypes,
                pk_indices: pk_indices_l,
                start_pos: 0,
                keyspace: ks_l,
            },
            side_r: JoinSide {
                ht: JoinHashMap::new(
                    JOIN_CACHE_SIZE,
                    pk_indices_r.clone(),
                    params_r.key_indices.clone(),
                    col_r_datatypes.clone(),
                    ks_r.clone(),
                ), // TODO: decide the target cap
                key_indices: params_r.key_indices,
                col_types: col_r_datatypes,
                pk_indices: pk_indices_r,
                start_pos: side_l_column_n,
                keyspace: ks_r,
            },
            pk_indices,
            cond,
            identity: format!("HashJoinExecutor {:X}", executor_id),
            op_info,
            key_indices,
            epoch: 0,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let input_l = self.input_l.take().unwrap();
        let input_r = self.input_r.take().unwrap();
        let aligned_stream = barrier_align(input_l.execute(), input_r.execute());
        #[for_await]
        for msg in aligned_stream {
            match msg? {
                AlignedMessage::Left(chunk) => {
                    yield self
                        .eq_join_oneside::<{ SideType::Left }>(chunk)
                        .await
                        .map_err(StreamExecutorError::hash_join_error)?;
                }
                AlignedMessage::Right(chunk) => {
                    yield self
                        .eq_join_oneside::<{ SideType::Right }>(chunk)
                        .await
                        .map_err(StreamExecutorError::hash_join_error)?;
                }
                AlignedMessage::Barrier(barrier) => {
                    self.flush_data()
                        .await
                        .map_err(StreamExecutorError::hash_join_error)?;
                    let epoch = barrier.epoch.curr;
                    self.side_l.ht.update_epoch(epoch);
                    self.side_r.ht.update_epoch(epoch);
                    self.epoch = epoch;
                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    async fn flush_data(&mut self) -> Result<()> {
        let epoch = self.epoch;
        for side in [&mut self.side_l, &mut self.side_r] {
            let mut write_batch = side.keyspace.state_store().start_write_batch();
            for state in side.ht.values_mut() {
                state.flush(&mut write_batch)?;
            }
            write_batch.ingest(epoch).await.unwrap();
        }

        // evict the LRU cache
        assert!(!self.side_l.is_dirty());
        self.side_l.ht.evict_to_target_cap();
        assert!(!self.side_r.is_dirty());
        self.side_r.ht.evict_to_target_cap();
        Ok(())
    }

    /// the data the hash table and match the coming
    /// data chunk with the executor state
    async fn hash_eq_match<'a>(
        key: &'a K,
        ht: &'a mut JoinHashMap<K, S>,
    ) -> Option<&'a mut HashValueType<S>> {
        if key.has_null() {
            None
        } else {
            ht.get_mut(key).await
        }
    }

    fn row_concat(
        row_update: &RowRef<'_>,
        update_start_pos: usize,
        row_matched: &Row,
        matched_start_pos: usize,
    ) -> Row {
        let mut new_row = vec![None; row_update.size() + row_matched.size()];
        for (i, datum_ref) in row_update.values().enumerate() {
            new_row[i + update_start_pos] = datum_ref.to_owned_datum();
        }
        for i in 0..row_matched.size() {
            new_row[i + matched_start_pos] = row_matched[i].clone();
        }
        Row(new_row)
    }

    fn bool_from_array_ref(array_ref: ArrayRef) -> bool {
        let bool_array = array_ref.as_ref().as_bool();
        bool_array.value_at(0).unwrap_or_else(|| {
            panic!(
                "Some thing wrong with the expression result. Bool array: {:?}",
                bool_array
            )
        })
    }

    async fn eq_join_oneside<const SIDE: SideTypePrimitive>(
        &mut self,
        chunk: StreamChunk,
    ) -> Result<Message> {
        let epoch = self.epoch;
        let chunk = chunk.compact()?;
        let (ops, columns, visibility) = chunk.into_inner();

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(columns);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        let (side_update, side_match) = if SIDE == SideType::Left {
            (&mut self.side_l, &mut self.side_r)
        } else {
            (&mut self.side_r, &mut self.side_l)
        };

        // TODO: find a better capacity number, the actual array length
        // is likely to be larger than the current capacity
        let capacity = data_chunk.capacity();

        let mut stream_chunk_builder = StreamChunkBuilder::new(
            capacity,
            &self.output_data_types,
            side_update.start_pos,
            side_match.start_pos,
        )?;

        let keys = K::build(&side_update.key_indices, &data_chunk)?;
        for (idx, (row, op)) in data_chunk.rows().zip_eq(ops.iter()).enumerate() {
            let key = &keys[idx];
            let value = row.to_owned_row();
            let pk = row.row_by_indices(&side_update.pk_indices);
            let matched_rows = Self::hash_eq_match(key, &mut side_match.ht).await;
            if let Some(matched_rows) = matched_rows {
                match *op {
                    Op::Insert | Op::UpdateInsert => {
                        let entry_value = side_update.ht.get_or_init_without_cache(key).await?;
                        let mut degree = 0;
                        for matched_row in matched_rows.values_mut(epoch).await {
                            // TODO(yuhao-su): We should find a better way to eval the
                            // expression without concat
                            // two rows.
                            let new_row = Self::row_concat(
                                &row,
                                side_update.start_pos,
                                &matched_row.row,
                                side_match.start_pos,
                            );
                            let mut cond_match = true;
                            // if there are non-equi expressions
                            if let Some(ref mut cond) = self.cond {
                                cond_match = Self::bool_from_array_ref(
                                    cond.eval(&new_row, &self.output_data_types)?,
                                );
                            }
                            if cond_match {
                                degree += 1;
                                if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
                                    // if the matched_row does not have any current matches
                                    stream_chunk_builder
                                        .append_row_matched(Op::UpdateDelete, &matched_row.row)?;
                                    stream_chunk_builder.append_row(
                                        Op::UpdateInsert,
                                        &row,
                                        &matched_row.row,
                                    )?;
                                } else {
                                    // concat with the matched_row and append the new row
                                    // FIXME: we always use `Op::Delete` here to avoid violating
                                    // the assumption for U+ after U-.
                                    stream_chunk_builder.append_row(
                                        Op::Insert,
                                        &row,
                                        &matched_row.row,
                                    )?;
                                }
                                matched_row.inc_degree();
                            } else {
                                // not matched
                                if outer_side_keep(T, SIDE) {
                                    stream_chunk_builder.append_row_update(*op, &row)?;
                                }
                            }
                        }
                        entry_value.insert(pk, JoinRow::new(value, degree));
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(v) = side_update.ht.get_mut_without_cached(key).await? {
                            // remove the row by it's primary key
                            v.remove(pk);

                            for matched_row in matched_rows.values_mut(epoch).await {
                                let new_row = Self::row_concat(
                                    &row,
                                    side_update.start_pos,
                                    &matched_row.row,
                                    side_match.start_pos,
                                );

                                let mut cond_match = true;
                                // if there are non-equi expressions
                                if let Some(ref mut cond) = self.cond {
                                    cond_match = Self::bool_from_array_ref(
                                        cond.eval(&new_row, &self.output_data_types)?,
                                    );
                                }
                                if cond_match {
                                    if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
                                        // if the matched_row does not have any current matches
                                        stream_chunk_builder.append_row(
                                            Op::UpdateDelete,
                                            &row,
                                            &matched_row.row,
                                        )?;
                                        stream_chunk_builder.append_row_matched(
                                            Op::UpdateInsert,
                                            &matched_row.row,
                                        )?;
                                    } else {
                                        // concat with the matched_row and append the new row
                                        // FIXME: we always use `Op::Delete` here to avoid violating
                                        // the assumption for U+ after U-.
                                        stream_chunk_builder.append_row(
                                            Op::Delete,
                                            &row,
                                            &matched_row.row,
                                        )?;
                                    }
                                    matched_row.dec_degree();
                                } else {
                                    // not matched
                                    if outer_side_keep(T, SIDE) {
                                        stream_chunk_builder.append_row_update(*op, &row)?;
                                    }
                                }
                            }
                        }
                    }
                };
            } else {
                // if there are no matched rows, just update the hash table
                //
                // FIXME: matched rows can still be there but just evicted from the memory cache, we
                // should handle this!
                match *op {
                    Op::Insert | Op::UpdateInsert => {
                        let state = side_update.ht.get_or_init_without_cache(key).await?;
                        state.insert(pk, JoinRow::new(value, 0));
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(v) = side_update.ht.get_mut_without_cached(key).await? {
                            v.remove(pk);
                        }
                    }
                };
                // if it's outer join and the side needs maintained.
                if outer_side_keep(T, SIDE) {
                    stream_chunk_builder.append_row_update(*op, &row)?;
                }
            }
        }

        let new_chunk = stream_chunk_builder.finish()?;

        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::hash::Key64;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{InputRefExpression, RowExpression};
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_storage::memory::MemoryStateStore;

    use super::{HashJoinExecutor, JoinParams, JoinType, *};
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::{Barrier, Epoch, Message};

    fn create_in_memory_keyspace() -> (Keyspace<MemoryStateStore>, Keyspace<MemoryStateStore>) {
        let mem_state = MemoryStateStore::new();
        (
            Keyspace::table_root(mem_state.clone(), &TableId::new(0)),
            Keyspace::table_root(mem_state, &TableId::new(1)),
        )
    }

    fn create_cond() -> RowExpression {
        let left_expr = InputRefExpression::new(DataType::Int64, 1);
        let right_expr = InputRefExpression::new(DataType::Int64, 3);
        let cond = new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            Box::new(left_expr),
            Box::new(right_expr),
        );
        RowExpression::new(cond)
    }

    fn create_executor<const T: JoinTypePrimitive>(
        with_condition: bool,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let (tx_l, source_l) = MockSource::channel(schema.clone(), vec![0, 1]);
        let (tx_r, source_r) = MockSource::channel(schema, vec![0, 1]);
        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);
        let cond = with_condition.then(create_cond);

        let (ks_l, ks_r) = create_in_memory_keyspace();

        let executor = HashJoinExecutor::<Key64, MemoryStateStore, T>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![1],
            1,
            cond,
            "HashJoinExecutor".to_string(),
            vec![],
            ks_l,
            ks_r,
        );
        (tx_l, tx_r, Box::new(executor).execute())
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::Inner }>(false);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10"
            )
        );
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_barrier() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 6 8
             + 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::Inner }>(false);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push a barrier to left side
        tx_l.push_barrier(2, false);

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);

        // join the first right chunk
        tx_r.push_chunk(chunk_r1);

        // Consume stream chunk
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7"
            )
        );

        // push a barrier to right side
        tx_r.push_barrier(2, false);

        // get the aligned barrier here
        let expected_epoch = Epoch::new_test_epoch(2);
        assert!(matches!(
            hash_join.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier {
                epoch,
                mutation: None,
                ..
            }) if epoch == expected_epoch
        ));

        // join the 2nd left chunk
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 6 8 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10
                + 3 8 3 10
                + 6 8 6 11"
            )
        );
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::LeftOuter }>(false);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . .
                - 3 8 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                U- 2 5 . .
                U+ 2 5 2 7"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                U- 3 6 . .
                U+ 3 6 3 10"
            )
        );
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) =
            create_executor::<{ JoinType::RightOuter }>(false);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 2 5 2 7
                + . . 4 8
                + . . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10
                - . . 5 10"
            )
        );
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::FullOuter }>(false);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . .
                - 3 8 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                U- 2 5 . .
                U+ 2 5 2 7
                +  . . 4 8
                +  . . 6 9"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10
                - . . 5 10"
            )
        );
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join_with_nonequi_condition() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 5
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 6
             + 4 8
             + 3 4",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 5 10
             - 5 10",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::FullOuter }>(true);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 4 . .
                + 2 5 . .
                + 3 6 . ."
            )
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 3 8 . .
                - 3 8 . ."
            )
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                U- 2 5 . .
                U+ 2 5 2 6
                +  . . 4 8
                +  . . 3 4"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + . . 5 10
                - . . 5 10"
            )
        );
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_nonequi_condition() {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 4
             + 2 10
             + 3 6",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I I
             + 3 8
             - 3 8",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I I
             + 2 7
             + 4 8
             + 6 9",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I  I
             + 3 10
             + 6 11",
        );
        let (mut tx_l, mut tx_r, mut hash_join) = create_executor::<{ JoinType::Inner }>(true);

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        hash_join.next().await.unwrap().unwrap();

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty("I I I I")
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);
        let chunk = hash_join.next().await.unwrap().unwrap();
        assert_eq!(
            chunk.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I I I
                + 3 6 3 10"
            )
        );
    }
}
