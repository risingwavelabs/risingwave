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
use risingwave_common::array::{Row, RowRef};
use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_common::error::Result;
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_storage::cell_based_row_deserializer::make_cell_based_row_deserializer;
use risingwave_storage::{Keyspace, StateStore};

use super::sides::{stream_lookup_arrange_prev_epoch, stream_lookup_arrange_this_epoch};
use crate::common::StreamChunkBuilder;
use crate::executor::error::StreamExecutorError;
use crate::executor::lookup::cache::LookupCache;
use crate::executor::lookup::sides::{ArrangeJoinSide, ArrangeMessage, StreamJoinSide};
use crate::executor::lookup::LookupExecutor;
use crate::executor::{Barrier, Epoch, Executor, Message, PkIndices, PROCESSING_WINDOW_SIZE};

/// Parameters for [`LookupExecutor`].
pub struct LookupExecutorParams<S: StateStore> {
    /// The side for arrangement. Currently, it should be a
    /// `MaterializeExecutor`.
    pub arrangement: Box<dyn Executor>,

    /// The side for stream. It can be any stream, but it will generally be a
    /// `MaterializeExecutor`.
    pub stream: Box<dyn Executor>,

    /// The keyspace for arrangement. [`LookupExecutor`] will use this keyspace to read the state
    /// of arrangement side.
    pub arrangement_keyspace: Keyspace<S>,

    /// Should be the same as [`ColumnDesc`] in the arrangement.
    ///
    /// From the perspective of arrangements, `arrangement_col_descs` include all columns of the
    /// `MaterializeExecutor`. For example, if we already have a table with 3 columns: `a, b,
    /// _row_id`, and we create an arrangement with join key `a` on it. `arrangement_col_descs`
    /// should contain all 3 columns.
    pub arrangement_col_descs: Vec<ColumnDesc>,

    /// Should only contain [`OrderPair`] for arrange in the arrangement.
    ///
    /// Still using the above `a, b, _row_id` example. If we create an arrangement with join key
    /// `a`, there will be 3 elements in `arrangement_col_descs`, and only 1 element in
    /// `arrangement_order_rules`.
    ///
    /// * The only element is the order rule for `a`, which is the join key. Join keys should
    ///   always come first.
    ///
    /// For the MV pks, they will only be contained in `arrangement_col_descs`, without being part
    /// of this `arrangement_order_rules`.
    pub arrangement_order_rules: Vec<OrderPair>,

    /// Primary key indices of the lookup result (after reordering).
    ///
    /// [`LookupExecutor`] will lookup a row from the stream using the join key in the arrangement.
    /// Therefore, the output of the [`LookupExecutor`] will be:
    ///
    /// ```plain
    /// | stream columns | arrangement columns |
    /// ```
    ///
    /// ... and will be reordered by `output_column_reorder_idx`.
    ///
    /// The optimizer should select pk with pk of the stream columns, and pk of the original
    /// materialized view (upstream of arrangement).
    pub pk_indices: PkIndices,

    /// Schema of the lookup result (after reordering).
    pub schema: Schema,

    /// By default, the output of [`LookupExecutor`] is `stream columns + arrangement columns`.
    /// The executor will do a reorder of columns before producing output, so that data can be
    /// consistent among A lookup B and B lookup A.
    pub column_mapping: Vec<usize>,

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
            schema: output_schema,
            column_mapping,
        } = params;

        let output_column_length = stream.schema().len() + arrangement.schema().len();

        // internal output schema: | stream | arrange |
        // will be rewritten using `column_mapping`.

        let schema_fields = stream
            .schema()
            .fields
            .iter()
            .chain(arrangement.schema().fields.iter())
            .cloned()
            .collect_vec();

        assert_eq!(schema_fields.len(), output_column_length);

        let schema = Schema::new(schema_fields);

        let chunk_data_types = schema.data_types();
        let arrangement_datatypes = arrangement.schema().data_types();
        let stream_datatypes = stream.schema().data_types();

        let arrangement_pk_indices = arrangement.pk_indices().to_vec();
        let stream_pk_indices = stream.pk_indices().to_vec();

        // check if arrange join key is exactly the same as order rules
        {
            let mut arrange_join_key_indices = arrange_join_key_indices.clone();
            arrange_join_key_indices.sort_unstable();
            let mut arrangement_order_types_indices = arrangement_order_rules
                .iter()
                .map(|x| x.column_idx)
                .collect_vec();
            arrangement_order_types_indices.sort_unstable();
            assert_eq!(
                arrange_join_key_indices,
                &arrangement_order_types_indices[0..arrange_join_key_indices.len()],
                "invalid join key: arrange_join_key_indices = {:?}, order_rules: {:?}",
                arrange_join_key_indices,
                arrangement_order_rules
            );
        }

        // compute the arrange keys used for the lookup
        let arrangement_order_types = arrangement_order_rules
            .iter()
            .map(|x| x.order_type)
            .collect();

        // check whether join keys are of the same length.
        assert_eq!(
            stream_join_key_indices.len(),
            arrange_join_key_indices.len()
        );

        // check whether output column mapping is valid.
        assert_eq!(
            column_mapping.len(),
            output_column_length,
            "column mapping mismatched"
        );

        // resolve mapping from join keys in stream row -> joins keys for arrangement.
        let key_indices_mapping = arrangement_order_rules
            .iter()
            .map(|x| x.column_idx) // the required column idx in this position
            .map(|x| {
                arrange_join_key_indices
                    .iter()
                    .position(|y| *y == x)
                    .unwrap()
            }) // the position of the item in join keys
            .map(|x| stream_join_key_indices[x]) // the actual column idx in stream
            .collect_vec();

        // check the inferred schema is really the same as the output schema of the lookup executor.
        assert_eq!(
            output_schema
                .fields
                .iter()
                .map(|x| x.data_type())
                .collect_vec(),
            column_mapping
                .iter()
                .map(|x| schema.fields[*x].data_type())
                .collect_vec(),
            "mismatched output schema"
        );

        Self {
            chunk_data_types,
            schema: output_schema,
            pk_indices,
            last_barrier: None,
            stream_executor: Some(stream),
            arrangement_executor: Some(arrangement),
            stream: StreamJoinSide {
                key_indices: stream_join_key_indices,
                pk_indices: stream_pk_indices,
                col_types: stream_datatypes,
            },
            arrangement: ArrangeJoinSide {
                pk_indices: arrangement_pk_indices,
                col_types: arrangement_datatypes,
                // special thing about this pair of serializer and deserializer: the serializer only
                // serializes join key, while the deserializer will take join key + pk into account.
                deserializer: make_cell_based_row_deserializer(arrangement_col_descs.clone()),
                serializer: OrderedRowSerializer::new(arrangement_order_types),
                col_descs: arrangement_col_descs,
                order_rules: arrangement_order_rules,
                key_indices: arrange_join_key_indices,
                keyspace: arrangement_keyspace,
                use_current_epoch,
            },
            column_mapping,
            key_indices_mapping,
            lookup_cache: LookupCache::new(),
        }
    }

    /// Try produce one stream message from [`LookupExecutor`]. If there's no message to produce, it
    /// will return `None`, and the `next` function of [`LookupExecutor`] will continuously polling
    /// messages until there's one.
    ///
    /// If we can use `async_stream` to write this part, things could be easier.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(mut self: Box<Self>) {
        let input = if self.arrangement.use_current_epoch {
            stream_lookup_arrange_this_epoch(
                self.stream_executor.take().unwrap(),
                self.arrangement_executor.take().unwrap(),
            )
            .boxed()
        } else {
            stream_lookup_arrange_prev_epoch(
                self.stream_executor.take().unwrap(),
                self.arrangement_executor.take().unwrap(),
            )
            .boxed()
        };

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                ArrangeMessage::Barrier(barrier) => {
                    if self.arrangement.use_current_epoch {
                        // If we are using current epoch, stream barrier should always come after
                        // arrange barrier. So we flush now.
                        self.lookup_cache.flush();
                    }
                    self.process_barrier(barrier.clone())
                        .await
                        .map_err(StreamExecutorError::eval_error)?;
                    if self.arrangement.use_current_epoch {
                        // When lookup this epoch, stream side barrier always come after arrangement
                        // ready, so we can forward barrier now.
                        yield Message::Barrier(barrier);
                    }
                }
                ArrangeMessage::ArrangeReady(arrangement_chunks, barrier) => {
                    // The arrangement is ready, and we will receive a bunch of stream messages for
                    // the next poll.

                    // TODO: apply chunk as soon as we receive them, instead of batching.

                    for chunk in arrangement_chunks {
                        self.lookup_cache
                            .apply_batch(chunk, &self.arrangement.key_indices)
                    }

                    if !self.arrangement.use_current_epoch {
                        // If we are using previous epoch, arrange barrier should always come after
                        // stream barrier. So we flush now.
                        self.lookup_cache.flush();

                        // When look prev epoch, arrange ready will always come after stream
                        // barrier. So we yield barrier now.
                        yield Message::Barrier(barrier);
                    }
                }
                ArrangeMessage::Stream(chunk) => {
                    let last_barrier = self
                        .last_barrier
                        .as_ref()
                        .expect("data received before a barrier");
                    let lookup_epoch = if self.arrangement.use_current_epoch {
                        last_barrier.epoch.curr
                    } else {
                        last_barrier.epoch.prev
                    };
                    let chunk = chunk.compact().map_err(StreamExecutorError::eval_error)?;
                    let (chunk, ops) = chunk.into_parts();

                    let mut builder = StreamChunkBuilder::new(
                        PROCESSING_WINDOW_SIZE,
                        &self.chunk_data_types,
                        0,
                        self.stream.col_types.len(),
                    )
                    .map_err(StreamExecutorError::eval_error)?;

                    for (op, row) in ops.iter().zip_eq(chunk.rows()) {
                        for matched_row in self
                            .lookup_one_row(&row, lookup_epoch)
                            .await
                            .map_err(StreamExecutorError::eval_error)?
                        {
                            tracing::trace!(target: "events::stream::lookup::put", "{:?} {:?}", row, matched_row);

                            if let Some(chunk) = builder
                                .append_row(*op, &row, &matched_row)
                                .map_err(StreamExecutorError::eval_error)?
                            {
                                yield Message::Chunk(chunk.reorder_columns(&self.column_mapping));
                            }
                        }
                        // TODO: support outer join (return null if no rows are matched)
                    }

                    if let Some(chunk) = builder.take().map_err(StreamExecutorError::eval_error)? {
                        yield Message::Chunk(chunk.reorder_columns(&self.column_mapping));
                    }
                }
            }
        }
    }

    /// Store the barrier.
    async fn process_barrier(&mut self, barrier: Barrier) -> Result<()> {
        if self.last_barrier.is_none() {
            assert_ne!(barrier.epoch.prev, 0, "lookup requires prev epoch != 0");

            // This is the first barrier, and we need to take special care of it.
            //
            // **Case 1: Lookup after Chain**
            //
            // In this case, we have the full state already on shared storage, so we must set prev
            // epoch = 0, and only one lookup path will work. Otherwise, the result might be
            // duplicated.
            //
            // **Case 2: Lookup after Arrange**
            //
            // The lookup is created by delta join plan without index, and prev epoch can't have any
            // data. Therefore, it is also okay to simply set prev epoch to 0.

            self.last_barrier = Some(Barrier {
                epoch: Epoch {
                    prev: 0,
                    curr: barrier.epoch.curr,
                },
                ..barrier
            });
            return Ok(());
        } else {
            self.last_barrier = Some(barrier)
        }
        Ok(())
    }

    /// Lookup all rows corresponding to a join key in shared buffer.
    async fn lookup_one_row(
        &mut self,
        stream_row: &RowRef<'_>,
        lookup_epoch: u64,
    ) -> Result<Vec<Row>> {
        // fast-path for empty look-ups.
        if lookup_epoch == 0 {
            return Ok(vec![]);
        }

        // stream_row is the row from stream side, we need to transform into the correct order of
        // the arrangement side.
        let lookup_row = stream_row.row_by_indices(&self.key_indices_mapping);
        if let Some(result) = self.lookup_cache.lookup(&lookup_row) {
            return Ok(result.iter().cloned().collect_vec());
        }

        // Serialize join key to a state store key.
        let key_prefix = {
            let mut key_prefix = vec![];
            self.arrangement
                .serializer
                .serialize_datums(lookup_row.0.iter(), &mut key_prefix);
            key_prefix
        };

        tracing::trace!(target: "events::stream::lookup::lookup_row", "{:?}, {:?}", lookup_row, bytes::Bytes::copy_from_slice(&key_prefix));

        let arrange_keyspace = self.arrangement.keyspace.append(key_prefix);
        let all_cells = arrange_keyspace.scan(None, lookup_epoch).await?;

        let mut all_rows = vec![];

        for (pk_with_cell_id, cell) in all_cells {
            tracing::trace!(target: "events::stream::lookup::scan", "{:?} => {:?}", pk_with_cell_id, cell);
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

        tracing::trace!(target: "events::stream::lookup::result", "{:?} => {:?}", lookup_row, all_rows);

        self.lookup_cache
            .batch_update(lookup_row, all_rows.iter().cloned());

        Ok(all_rows)
    }
}
