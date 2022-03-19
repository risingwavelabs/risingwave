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
//
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{Row, RowRef, StreamChunk};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::{Keyspace, Segment};

use super::sides::{stream_lookup_arrange_prev_epoch, stream_lookup_arrange_this_epoch};
use super::*;
use crate::common::StreamChunkBuilder;

/// Parameters for [`LookupExecutor`].
pub struct LookupExecutorParams<S: StateStore> {
    /// The side for arrangement. Currently, it should be a
    /// [`MaterializeExecutor`].
    ///
    /// [`MaterializeExecutor`]: crate::executor::mview::MaterializeExecutor
    pub arrangement: Box<dyn Executor>,

    /// The side for stream. It can be any stream, but it will generally be a
    /// [`MaterializeExecutor`].
    ///
    /// [`MaterializeExecutor`]: crate::executor::mview::MaterializeExecutor
    pub stream: Box<dyn Executor>,

    /// The keyspace for arrangement. [`LookupExecutor`] will use this keyspace to read the state
    /// of arrangement side.
    pub arrangement_keyspace: Keyspace<S>,

    /// Should be the same as [`ColumnDesc`] in the arrangement.
    ///
    /// From the perspective of arrangements, `arrangement_col_descs` include all columns of the
    /// [`MaterializeExecutor`]. For example, if we already have a table with 3 columns: `a, b,
    /// _row_id`, and we create an arrangement with join key `a` on it. `arrangement_col_descs`
    /// should contain all 3 columns.
    ///
    /// [`MaterializeExecutor`]: crate::executor::mview::MaterializeExecutor
    pub arrangement_col_descs: Vec<ColumnDesc>,

    /// Should be the same as [`OrderPair`] in the arrangement.
    ///
    /// Still using the above `a, b, _row_id` example. If we create an arrangement with join key
    /// `a`, there will be two elements in `arrangement_order_rules`.
    ///
    /// * The first element is the order rule for `a`, which is the join key. Join keys should
    ///   always come first.
    /// * The second element is the original primary key for the MV.
    ///
    /// These two parts now compose the arrange keys for the arrangement.
    pub arrangement_order_rules: Vec<OrderPair>,

    /// Primary key indices of the lookup result.
    ///
    /// [`LookupExecutor`] will lookup a row from the stream using the join key in the arrangement.
    /// Therefore, the output of the [`LookupExecutor`] will be:
    ///
    /// ```plain
    /// | stream columns | arrangement columns |
    /// ```
    ///
    /// The optimizer should select pk with pk of the stream columns, and pk of the original
    /// materialized view (upstream of arrangement).
    pub pk_indices: PkIndices,

    /// Whether to use the current epoch of the arrangement to lookup.
    ///
    /// [`LookupExecutor`] will act differently on whether `use_current_epoch` is set to true. In a
    /// nutshell, lookup in the previous epoch (this option set to false) will be more efficient.
    /// Therefore, the optimizer should choose an optimal order for lookup executors.
    ///
    /// See [`stream_lookup_arrange_this_epoch`] and [`stream_lookup_arrange_prev_epoch`] for more
    /// information.
    pub use_current_epoch: bool,

    /// The join keys on the stream side.
    pub stream_join_key_indices: Vec<usize>,

    /// The join keys on the arrangement side.
    pub arrange_join_key_indices: Vec<usize>,
}

impl<S: StateStore> LookupExecutor<S> {
    pub fn new(params: LookupExecutorParams<S>) -> Self {
        let LookupExecutorParams {
            arrangement,
            stream,
            arrangement_keyspace,
            arrangement_col_descs,
            arrangement_order_rules,
            pk_indices,
            use_current_epoch,
            stream_join_key_indices,
            arrange_join_key_indices,
        } = params;

        let output_column_length = stream.schema().len() + arrangement.schema().len();

        // output schema: | arrange | stream |
        let schema_fields = arrangement
            .schema()
            .fields
            .iter()
            .chain(stream.schema().fields.iter())
            .cloned()
            .collect_vec();

        assert_eq!(schema_fields.len(), output_column_length);

        let schema = Schema::new(schema_fields);

        let output_data_types = schema.data_types();
        let arrangement_datatypes = arrangement.schema().data_types();
        let stream_datatypes = stream.schema().data_types();

        let arrangement_pk_indices = arrangement.pk_indices().to_vec();
        let stream_pk_indices = stream.pk_indices().to_vec();

        let arrangement_order_types = arrangement_order_rules
            .iter()
            .map(|x| x.order_type)
            .collect_vec();

        // check if arrange join key is the prefix of the ordered pairs
        {
            let mut arrange_join_key_indices = arrange_join_key_indices.clone();
            arrange_join_key_indices.sort_unstable();
            assert!(
                arrange_join_key_indices
                    .iter()
                    .enumerate()
                    .all(|(idx, pos)| idx == *pos),
                "invalid join key"
            );
        }

        // compute the arrange keys used for the lookup
        let arrangement_order_types = arrange_join_key_indices
            .iter()
            .map(|idx| arrangement_order_types[*idx])
            .collect_vec();

        // check whether join keys are of the same length.
        assert_eq!(
            stream_join_key_indices.len(),
            arrange_join_key_indices.len()
        );

        Self {
            output_data_types,
            schema,
            pk_indices,
            last_barrier: None,
            input: if use_current_epoch {
                Box::pin(stream_lookup_arrange_this_epoch(stream, arrangement))
            } else {
                Box::pin(stream_lookup_arrange_prev_epoch(stream, arrangement))
            },
            stream: StreamJoinSide {
                key_indices: stream_join_key_indices,
                pk_indices: stream_pk_indices,
                col_types: stream_datatypes,
            },
            arrangement: ArrangeJoinSide {
                pk_indices: arrangement_pk_indices,
                col_types: arrangement_datatypes,
                deserializer: CellBasedRowDeserializer::new(arrangement_col_descs.clone()),
                col_descs: arrangement_col_descs,
                serializer: OrderedRowSerializer::new(arrangement_order_types),
                order_rules: arrangement_order_rules,
                join_key_indices: arrange_join_key_indices,
                keyspace: arrangement_keyspace,
                use_current_epoch,
            },
        }
    }

    /// Try produce one stream message from [`LookupExecutor`]. If there's no message to produce, it
    /// will return `None`, and the `next` function of [`LookupExecutor`] will continously polling
    /// messages until there's one.
    ///
    /// If we can use `async_stream` to write this part, things could be easier.
    pub async fn next_inner(&mut self) -> Result<Option<Message>> {
        match self.input.next().await.expect("unexpected end of stream")? {
            ArrangeMessage::Barrier(barrier) => {
                self.process_barrier(barrier.clone()).await?;
                Ok(Some(Message::Barrier(barrier)))
            }
            ArrangeMessage::Arrange(_) => {
                // TODO: replicate batch
                //
                // As we assume currently all lookups are on the same worker node of arrangements,
                // the data would always be available in the local shared buffer. Therefore, there's
                // no need to replicate batch.
                Ok(None)
            }
            ArrangeMessage::Stream(chunk) => Ok(Some(Message::Chunk(self.lookup(chunk).await?))),
        }
    }

    /// Store the barrier.
    async fn process_barrier(&mut self, barrier: Barrier) -> Result<()> {
        self.last_barrier = Some(barrier);
        Ok(())
    }

    /// Lookup the data in the shared buffer.
    async fn lookup(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        let last_barrier = self
            .last_barrier
            .as_ref()
            .expect("data received before a barrier");
        let lookup_epoch = if self.arrangement.use_current_epoch {
            last_barrier.epoch.curr
        } else {
            last_barrier.epoch.prev
        };
        let chunk = chunk.compact()?;
        let (chunk, ops) = chunk.into_parts();

        let mut builder = StreamChunkBuilder::new(
            chunk.capacity(),
            &self.output_data_types,
            0,
            self.stream.col_types.len(),
        )?;

        for (op, row) in ops.iter().zip_eq(chunk.rows()) {
            for matched_row in self.lookup_one_row(&row, lookup_epoch).await? {
                builder.append_row(*op, &row, &matched_row)?;
            }
            // TODO: support outer join (return null if no rows are matched)
        }

        builder.finish()
    }

    /// Lookup all rows corresponding to a join key in shared buffer.
    async fn lookup_one_row(&mut self, row: &RowRef<'_>, lookup_epoch: u64) -> Result<Vec<Row>> {
        // TODO: add a cache for arrangement in an upstream executor

        // Serialize join key to a state store key.
        let key_prefix = {
            let row = RowRef(
                self.arrangement
                    .join_key_indices
                    .iter()
                    .map(|x| row.0[*x])
                    .collect_vec(),
            );
            tracing::trace!(target: "events::stream::lookup::one_row", "{:?}", row);
            let mut key_prefix = vec![];
            self.arrangement
                .serializer
                .serialize_row_ref(&row, &mut key_prefix);
            key_prefix
        };

        let arrange_keyspace = self
            .arrangement
            .keyspace
            .with_segment(Segment::raw(key_prefix));

        let all_cells = arrange_keyspace
            .scan_strip_prefix(None, lookup_epoch)
            .await?;

        let mut all_rows = vec![];

        for (pk_with_cell_id, cell) in all_cells {
            if let Some((_, row)) = self
                .arrangement
                .deserializer
                .deserialize(&pk_with_cell_id, &cell)?
            {
                all_rows.push(row);
            }
        }

        if let Some((_, last_row)) = self.arrangement.deserializer.take() {
            all_rows.push(last_row);
        }

        Ok(all_rows)
    }
}
