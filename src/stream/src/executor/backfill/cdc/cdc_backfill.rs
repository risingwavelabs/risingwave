// Copyright 2023 RisingWave Labs
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

use std::pin::{pin, Pin};
use std::sync::Arc;

use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::{
    DebeziumParser, EncodingProperties, JsonProperties, ProtocolProperties,
    SourceStreamChunkBuilder, SpecificParserConfig,
};
use risingwave_connector::source::external::CdcOffset;
use risingwave_connector::source::{SourceColumnDesc, SourceContext};
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::backfill::cdc::state::CdcBackfillState;
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::cdc::upstream_table::snapshot::{
    SnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{
    get_cdc_chunk_last_offset, get_new_pos, mapping_chunk, mapping_message, mark_cdc_chunk,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::task::CreateMviewProgress;

/// `split_id`, `is_finished`, `row_count`, `cdc_offset` all occupy 1 column each.
const METADATA_STATE_LEN: usize = 4;

pub struct CdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// The external table to be backfilled
    external_table: ExternalStorageTable,

    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    upstream: BoxedExecutor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    /// User may select a subset of columns from the upstream table.
    output_indices: Vec<usize>,

    /// State table of the CdcBackfill executor
    state_table: StateTable<S>,

    progress: Option<CreateMviewProgress>,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,
}

impl<S: StateStore> CdcBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        external_table: ExternalStorageTable,
        upstream: BoxedExecutor,
        output_indices: Vec<usize>,
        progress: Option<CreateMviewProgress>,
        metrics: Arc<StreamingMetrics>,
        state_table: StateTable<S>,
        chunk_size: usize,
    ) -> Self {
        Self {
            actor_ctx,
            info,
            external_table,
            upstream,
            output_indices,
            state_table,
            progress,
            metrics,
            chunk_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        let pk_in_output_indices = self.external_table.pk_in_output_indices().unwrap();
        let pk_order = self.external_table.pk_order_types().to_vec();

        let upstream_table_id = self.external_table.table_id().table_id;
        let upstream_table_name = self.external_table.qualified_table_name();
        let upstream_table_schema = self.external_table.schema().clone();
        let upstream_table_reader = UpstreamTableReader::new(self.external_table);

        let mut upstream = self.upstream.execute();

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pk_pos: Option<OwnedRow>;

        tracing::info!(
            upstream_table_id,
            upstream_table_name,
            ?pk_in_output_indices
        );

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;

        // Check whether this parallelism has been assigned splits,
        // if not, we should bypass the backfill directly.
        let mut state_impl = CdcBackfillState::new(
            upstream_table_id,
            self.state_table,
            pk_in_output_indices.len() + METADATA_STATE_LEN,
        );

        let mut upstream = transform_upstream(upstream, &upstream_table_schema)
            .boxed()
            .peekable();

        state_impl.init_epoch(first_barrier.epoch);

        // restore backfill state
        let state = state_impl.restore_state().await?;
        current_pk_pos = state.current_pk_pos.clone();

        let to_backfill = !state.is_finished;

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        // Keep track of rows from the snapshot.
        let mut total_snapshot_row_count = state.row_count as u64;
        let mut snapshot_read_epoch;

        let mut last_binlog_offset: Option<CdcOffset> = state
            .last_cdc_offset
            .map_or(upstream_table_reader.current_binlog_offset().await?, Some);

        let mut consumed_binlog_offset: Option<CdcOffset> = None;

        tracing::info!(
            upstream_table_id,
            upstream_table_name,
            ?current_pk_pos,
            is_finished = state.is_finished,
            snapshot_row_count = total_snapshot_row_count,
            chunk_size = self.chunk_size,
            "start cdc backfill"
        );

        // CDC Backfill Algorithm:
        //
        // When the first barrier comes from upstream:
        //  - read the current binlog offset as `binlog_low`
        //  - start a snapshot read upon upstream table and iterate over the snapshot read stream
        //  - buffer the changelog event from upstream
        //
        // When a new barrier comes from upstream:
        //  - read the current binlog offset as `binlog_high`
        //  - for each row of the upstream change log, forward it to downstream if it in the range
        //    of [binlog_low, binlog_high] and its pk <= `current_pos`, otherwise ignore it
        //  - reconstruct the whole backfill stream with upstream changelog and a new table snapshot
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if to_backfill {
            // drive the upstream changelog first to ensure we can receive timely changelog event,
            // otherwise the upstream changelog may be blocked by the snapshot read stream
            let _ = Pin::new(&mut upstream).peek().await;

            // wait for a barrier to make sure the backfill starts after upstream source
            #[for_await]
            for msg in upstream.by_ref() {
                match msg? {
                    Message::Barrier(barrier) => {
                        // commit state just to bump the epoch of state table
                        state_impl.commit_state(barrier.epoch).await?;
                        yield Message::Barrier(barrier);
                        break;
                    }
                    Message::Chunk(ref chunk) => {
                        last_binlog_offset = get_cdc_chunk_last_offset(
                            upstream_table_reader.inner().table_reader(),
                            chunk,
                        )?;
                    }
                    Message::Watermark(_) => {
                        // Ignore watermark
                    }
                }
            }

            tracing::info!(upstream_table_id, initial_binlog_offset = ?last_binlog_offset, ?current_pk_pos, "start cdc backfill loop");
            'backfill_loop: loop {
                let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

                let left_upstream = upstream.by_ref().map(Either::Left);

                let args = SnapshotReadArgs::new_for_cdc(current_pk_pos.clone(), self.chunk_size);
                let right_snapshot =
                    pin!(upstream_table_reader.snapshot_read(args).map(Either::Right));

                // Prefer to select upstream, so we can stop snapshot stream when barrier comes.
                let backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;

                #[for_await]
                for either in backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    // If it is a barrier, switch snapshot and consume buffered
                                    // upstream chunk.
                                    // If no current_pos, means we did not process any snapshot yet.
                                    // In that case we can just ignore the upstream buffer chunk.
                                    if let Some(current_pos) = &current_pk_pos {
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            cur_barrier_upstream_processed_rows +=
                                                chunk.cardinality() as u64;

                                            // record the consumed binlog offset that will be
                                            // persisted later
                                            consumed_binlog_offset = get_cdc_chunk_last_offset(
                                                upstream_table_reader.inner().table_reader(),
                                                &chunk,
                                            )?;
                                            yield Message::Chunk(mapping_chunk(
                                                mark_cdc_chunk(
                                                    upstream_table_reader.inner().table_reader(),
                                                    chunk,
                                                    current_pos,
                                                    &pk_in_output_indices,
                                                    &pk_order,
                                                    last_binlog_offset.clone(),
                                                )?,
                                                &self.output_indices,
                                            ));
                                        }
                                    }

                                    self.metrics
                                        .cdc_backfill_snapshot_read_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_ctx.id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_snapshot_processed_rows);

                                    self.metrics
                                        .cdc_backfill_upstream_output_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_ctx.id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_upstream_processed_rows);

                                    // Update last seen binlog offset
                                    if consumed_binlog_offset.is_some() {
                                        last_binlog_offset = consumed_binlog_offset.clone();
                                    }

                                    // update and persist backfill state
                                    state_impl
                                        .mutate_state(
                                            current_pk_pos.clone(),
                                            last_binlog_offset.clone(),
                                            total_snapshot_row_count,
                                            false,
                                        )
                                        .await?;
                                    state_impl.commit_state(barrier.epoch).await?;

                                    snapshot_read_epoch = barrier.epoch.prev;
                                    if let Some(progress) = self.progress.as_mut() {
                                        progress.update(
                                            barrier.epoch.curr,
                                            snapshot_read_epoch,
                                            total_snapshot_row_count,
                                        );
                                    }

                                    yield Message::Barrier(barrier);
                                    // Break the for loop and start a new snapshot read stream.
                                    break;
                                }
                                Message::Chunk(chunk) => {
                                    // skip empty upstream chunk
                                    if chunk.cardinality() == 0 {
                                        continue;
                                    }

                                    let chunk_binlog_offset = get_cdc_chunk_last_offset(
                                        upstream_table_reader.inner().table_reader(),
                                        &chunk,
                                    )?;

                                    tracing::trace!(
                                        target: "events::stream::cdc_backfill",
                                        "recv changelog chunk: chunk_offset {:?}, capactiy {}",
                                        chunk_binlog_offset,
                                        chunk.capacity()
                                    );

                                    // Since we don't need changelog before the
                                    // `last_binlog_offset`, skip the chunk that *only* contains
                                    // events before `last_binlog_offset`.
                                    if let Some(last_binlog_offset) = last_binlog_offset.as_ref() {
                                        if let Some(chunk_offset) = chunk_binlog_offset
                                            && chunk_offset < *last_binlog_offset
                                        {
                                            tracing::trace!(
                                                target: "events::stream::cdc_backfill",
                                                "skip changelog chunk: chunk_offset {:?}, capacity {}",
                                                chunk_offset,
                                                chunk.capacity()
                                            );
                                            continue;
                                        }
                                    }
                                    // Buffer the upstream chunk.
                                    upstream_chunk_buffer.push(chunk.compact());
                                }
                                Message::Watermark(_) => {
                                    // Ignore watermark during backfill.
                                }
                            }
                        }
                        // Snapshot read
                        Either::Right(msg) => {
                            match msg? {
                                None => {
                                    tracing::info!(
                                        upstream_table_id,
                                        ?last_binlog_offset,
                                        ?current_pk_pos,
                                        "snapshot read stream ends"
                                    );
                                    // End of the snapshot read stream.
                                    // We should not mark the chunk anymore,
                                    // otherwise, we will ignore some rows
                                    // in the buffer. Here we choose to never mark the chunk.
                                    // Consume with the renaming stream buffer chunk without mark.
                                    for chunk in upstream_chunk_buffer.drain(..) {
                                        yield Message::Chunk(mapping_chunk(
                                            chunk,
                                            &self.output_indices,
                                        ));
                                    }

                                    state_impl
                                        .mutate_state(
                                            current_pk_pos,
                                            last_binlog_offset.clone(),
                                            total_snapshot_row_count,
                                            true,
                                        )
                                        .await?;
                                    break 'backfill_loop;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    current_pk_pos =
                                        Some(get_new_pos(&chunk, &pk_in_output_indices));

                                    tracing::trace!(
                                        "got a snapshot chunk: len {}, current_pk_pos {:?}",
                                        chunk.cardinality(),
                                        current_pk_pos
                                    );
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_row_count += chunk_cardinality;
                                    yield Message::Chunk(mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // drop reader to release db connection
        drop(upstream_table_reader);

        tracing::info!(
            upstream_table_id,
            "CdcBackfill has already finished and will forward messages directly to the downstream"
        );

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg {
                    // persist the backfill state
                    state_impl.commit_state(barrier.epoch).await?;

                    // mark progress as finished
                    if let Some(progress) = self.progress.as_mut() {
                        progress.finish(barrier.epoch.curr, total_snapshot_row_count);
                    }
                    yield msg;
                    // break after the state have been saved
                    break;
                }
                yield msg;
            }
        }

        // After backfill progress finished
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            // upstream offsets will be removed from the message before forwarding to
            // downstream
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    // commit state just to bump the epoch of state table
                    state_impl.commit_state(barrier.epoch).await?;
                }
                yield msg;
            }
        }
    }
}

#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn transform_upstream(upstream: BoxedMessageStream, schema: &Schema) {
    let props = SpecificParserConfig {
        key_encoding_config: None,
        encoding_config: EncodingProperties::Json(JsonProperties {
            use_schema_registry: false,
        }),
        protocol_config: ProtocolProperties::Debezium,
    };
    let mut parser = DebeziumParser::new(
        props,
        get_rw_columns(schema),
        Arc::new(SourceContext::default()),
    )
    .await
    .map_err(StreamExecutorError::connector_error)?;

    pin_mut!(upstream);
    #[for_await]
    for msg in upstream {
        let mut msg = msg?;
        if let Message::Chunk(chunk) = &mut msg {
            let parsed_chunk = parse_debezium_chunk(&mut parser, chunk, schema).await?;
            let _ = std::mem::replace(chunk, parsed_chunk);
        }
        yield msg;
    }
}

async fn parse_debezium_chunk(
    parser: &mut DebeziumParser,
    chunk: &StreamChunk,
    schema: &Schema,
) -> StreamExecutorResult<StreamChunk> {
    // here we transform the input chunk in (payload varchar, _rw_offset varchar, _rw_table_name varchar) schema
    // to chunk with downstream table schema `info.schema` of MergeNode contains the schema of the
    // table job with `_rw_offset` in the end
    // see `gen_create_table_plan_for_cdc_source` for details
    let column_descs = get_rw_columns(schema);
    let mut builder = SourceStreamChunkBuilder::with_capacity(column_descs, chunk.capacity());

    // The schema of input chunk (payload varchar, _rw_offset varchar, _rw_table_name varchar, _row_id)
    // We should use the debezium parser to parse the first column,
    // then chain the parsed row with `_rw_offset` row to get a new row.
    let payloads = chunk.data_chunk().project(vec![0].as_slice());
    let offset_columns = chunk.data_chunk().project(vec![1].as_slice());

    // TODO: preserve the transaction semantics
    for payload in payloads.rows() {
        let ScalarRefImpl::Jsonb(jsonb_ref) = payload.datum_at(0).expect("payload must exist")
        else {
            unreachable!("payload must be jsonb");
        };

        parser
            .parse_inner(
                None,
                Some(jsonb_ref.to_string().as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await
            .unwrap();
    }

    let parsed_chunk = builder.finish();
    let (data_chunk, ops) = parsed_chunk.into_parts();

    // concat the rows in the parsed chunk with the _rw_offset column, we should also retain the Op column
    let mut new_rows = Vec::with_capacity(chunk.capacity());
    let offset_columns = offset_columns.compact();
    for (data_row, offset_row) in data_chunk
        .rows_with_holes()
        .zip_eq_fast(offset_columns.rows_with_holes())
    {
        let combined = data_row.chain(offset_row);
        new_rows.push(combined);
    }

    let data_types = schema
        .fields
        .iter()
        .map(|field| field.data_type.clone())
        .chain(std::iter::once(DataType::Varchar)) // _rw_offset column
        .collect_vec();

    Ok(StreamChunk::from_parts(
        ops,
        DataChunk::from_rows(new_rows.as_slice(), data_types.as_slice()),
    ))
}

fn get_rw_columns(schema: &Schema) -> Vec<SourceColumnDesc> {
    schema
        .fields
        .iter()
        .map(|field| {
            let column_desc = ColumnDesc::named(
                field.name.clone(),
                ColumnId::placeholder(),
                field.data_type.clone(),
            );
            SourceColumnDesc::from(&column_desc)
        })
        .collect_vec()
}

impl<S: StateStore> Executor for CdcBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::{pin_mut, StreamExt};
    use risingwave_common::array::{DataChunk, Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, Datum, JsonbVal};
    use risingwave_common::util::iter_util::ZipEqFast;

    use crate::executor::backfill::cdc::cdc_backfill::transform_upstream;
    use crate::executor::test_utils::MockSource;
    use crate::executor::Executor;

    #[tokio::test]
    async fn test_transform_upstream_chunk() {
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Jsonb),   // debezium json payload
            Field::unnamed(DataType::Varchar), // _rw_offset
            Field::unnamed(DataType::Varchar), // _rw_table_name
        ]);
        let pk_indices = vec![1];
        let (mut tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());
        // let payload = r#"{"before": null,"after":{"O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" },"source":{"version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null},"op":"r","ts_ms":1695277757017,"transaction":null}"#.to_string();
        let payload = r#"{ "payload": { "before": null, "after": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" }, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null }, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#;

        let datums: Vec<Datum> = vec![
            Some(JsonbVal::from_str(payload).unwrap().into()),
            Some("file: 1.binlog, pos: 100".to_string().into()),
            Some("mydb.orders".to_string().into()),
        ];

        println!("datums: {:?}", datums[1]);

        let mut builders = schema.create_array_builders(8);
        for (builder, datum) in builders.iter_mut().zip_eq_fast(datums.iter()) {
            builder.append(datum.clone());
        }
        let columns = builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();

        // one row chunk
        let chunk = StreamChunk::from_parts(vec![Op::Insert], DataChunk::new(columns, 1));

        tx.push_chunk(chunk);
        let upstream = Box::new(source).execute();

        // schema of the CDC table
        let rw_schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "O_ORDERKEY"), // orderkey
            Field::with_name(DataType::Int64, "O_CUSTKEY"),  // custkey
            Field::with_name(DataType::Varchar, "O_ORDERSTATUS"), // orderstatus
            Field::with_name(DataType::Decimal, "O_TOTALPRICE"), // totalprice
            Field::with_name(DataType::Date, "O_ORDERDATE"), // orderdate
        ]);

        let parsed_stream = transform_upstream(upstream, &rw_schema);
        pin_mut!(parsed_stream);
        // the output chunk must contain the offset column
        if let Some(message) = parsed_stream.next().await {
            println!("chunk: {:#?}", message.unwrap());
        }
    }
}
