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

use std::default::Default;
use std::pin::{pin, Pin};
use std::sync::Arc;

use anyhow::anyhow;
use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use maplit::hashmap;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, JsonbVal, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::{
    DebeziumParser, EncodingProperties, JsonProperties, ProtocolProperties,
    SourceStreamChunkBuilder, SpecificParserConfig,
};
use risingwave_connector::source::external::{CdcOffset, DebeziumOffset, DebeziumSourceOffset};
use risingwave_connector::source::{
    SourceColumnDesc, SourceContext, SplitId, SplitImpl, SplitMetaData,
};
use risingwave_storage::StateStore;
use serde_json::Value;

use crate::executor::backfill::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::upstream_table::snapshot::{
    SnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{
    get_cdc_chunk_last_offset, get_new_pos, mapping_chunk, mapping_message, mark_cdc_chunk,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, MessageStream, Mutation, PkIndices, PkIndicesRef,
    SourceStateTableHandler, StreamExecutorError, StreamExecutorResult,
};
use crate::task::{ActorId, CreateMviewProgress};

const BACKFILL_STATE_KEY_SUFFIX: &str = "_backfill";

pub struct CdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Upstream external table
    upstream_table: ExternalStorageTable,

    /// Upstream changelog with the same schema with the external table.
    upstream: BoxedExecutor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    /// User may select a subset of columns from the upstream table.
    output_indices: Vec<usize>,

    actor_id: ActorId,

    info: ExecutorInfo,

    /// Stores the backfill done flag
    source_state_handler: SourceStateTableHandler<S>,

    upstream_is_shared: bool,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,
}

impl<S: StateStore> CdcBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        upstream_table: ExternalStorageTable,
        upstream: BoxedExecutor,
        output_indices: Vec<usize>,
        _progress: Option<CreateMviewProgress>,
        schema: Schema,
        pk_indices: PkIndices,
        metrics: Arc<StreamingMetrics>,
        source_state_handler: SourceStateTableHandler<S>,
        upstream_is_shared: bool,
        chunk_size: usize,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "CdcBackfillExecutor".to_owned(),
            },
            upstream_table,
            upstream,
            output_indices,
            actor_id: 0,
            metrics,
            chunk_size,
            actor_ctx,
            source_state_handler,
            upstream_is_shared,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();
        let pk_order = self.upstream_table.pk_order_types().to_vec();

        let upstream_table_id = self.upstream_table.table_id().table_id;
        let upstream_table_reader = UpstreamTableReader::new(self.upstream_table);

        let mut upstream = self.upstream.execute();

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pk_pos: Option<OwnedRow>;

        tracing::info!(upstream_table_id, ?pk_in_output_indices);

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;

        // Check whether this parallelism has been assigned splits,
        // if not, we should bypass the backfill directly.
        let mut invalid_backfill = false;
        let mut split_id: Option<SplitId> = None;
        let mut cdc_split: Option<SplitImpl> = None;
        if let Some(mutation) = first_barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add { splits, .. }
                | Mutation::Update {
                    actor_splits: splits,
                    ..
                } => {
                    invalid_backfill = match splits.get(&self.actor_ctx.id) {
                        None => true,
                        Some(splits) => {
                            let is_empty = splits.is_empty();
                            if !is_empty {
                                let split = splits.iter().exactly_one().map_err(|err| {
                                    StreamExecutorError::from(anyhow!(
                                        "fail to get cdc split {}",
                                        err
                                    ))
                                })?;
                                split_id = Some(split.id());
                                cdc_split = Some(split.clone());
                            }
                            is_empty
                        }
                    }
                }
                _ => {}
            }
        }

        let mut upstream = if self.upstream_is_shared {
            transform_upstream(upstream, &self.info.schema)
                .boxed()
                .peekable()
        } else {
            upstream.peekable()
        };

        if invalid_backfill {
            // The first barrier message should be propagated.
            yield Message::Barrier(first_barrier);
            #[for_await]
            for msg in upstream {
                if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                    yield msg;
                }
            }
            // exit the executor
            return Ok(());
        }

        tracing::debug!("start cdc backfill: actor {:?}", self.actor_ctx.id);

        // self.source_state_handler.init_epoch(first_barrier.epoch);

        // start from the beginning
        // TODO(siyuan): restore backfill offset from state store
        let backfill_offset = None;

        current_pk_pos = backfill_offset;

        let is_finished = {
            // restore backfill done flag from state store
            if let Some(split_id) = split_id.as_ref() {
                let mut key = split_id.to_string();
                key.push_str(BACKFILL_STATE_KEY_SUFFIX);
                // match self.source_state_handler.get(key.into()).await? {
                //     Some(row) => match row.datum_at(1) {
                //         Some(ScalarRefImpl::Jsonb(jsonb_ref)) => jsonb_ref.as_bool()?,
                //         _ => unreachable!("invalid backfill persistent state"),
                //     },
                //     None => false,
                // }
                false
            } else {
                false
            }
        };

        // If the snapshot is empty, we don't need to backfill.
        let is_snapshot_empty: bool = {
            if is_finished {
                // It is finished, so just assign a value to avoid accessing storage table again.
                false
            } else {
                let args = SnapshotReadArgs::new(init_epoch, None, false, self.chunk_size);
                let snapshot = upstream_table_reader.snapshot_read(args);
                pin_mut!(snapshot);
                snapshot.try_next().await?.unwrap().is_none()
            }
        };

        // | backfill_is_finished | snapshot_empty | need_to_backfill |
        // | t                    | t/f            | f                |
        // | f                    | t              | f                |
        // | f                    | f              | t                |
        let to_backfill = !is_finished && !is_snapshot_empty;

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        // Keep track of rows from the snapshot.
        #[allow(unused_variables)]
        let mut total_snapshot_processed_rows: u64 = 0;

        let mut last_binlog_offset: Option<CdcOffset>;

        let mut consumed_binlog_offset: Option<CdcOffset> = None;

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
            last_binlog_offset = upstream_table_reader.current_binlog_offset().await?;
            // drive the upstream changelog first to ensure we can receive timely changelog event,
            // otherwise the upstream changelog may be blocked by the snapshot read stream
            let _ = Pin::new(&mut upstream).peek().await;

            tracing::info!(
                upstream_table_id,
                initial_binlog_offset = ?last_binlog_offset,
                "start the bacfill loop");

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
                                        .backfill_snapshot_read_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_snapshot_processed_rows);

                                    self.metrics
                                        .backfill_upstream_output_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_upstream_processed_rows);

                                    // Update last seen binlog offset
                                    if consumed_binlog_offset.is_some() {
                                        last_binlog_offset = consumed_binlog_offset.clone();
                                    }

                                    // seal current epoch even though there is no data
                                    // Self::persist_state(
                                    //     &mut self.source_state_handler,
                                    //     barrier.epoch,
                                    // )
                                    // .await?;

                                    yield Message::Barrier(barrier);
                                    // Break the for loop and start a new snapshot read stream.
                                    break;
                                }
                                Message::Chunk(chunk) => {
                                    let chunk_binlog_offset = get_cdc_chunk_last_offset(
                                        upstream_table_reader.inner().table_reader(),
                                        &chunk,
                                    )?;

                                    tracing::trace!(
                                        "recv changelog chunk: bin offset {:?}, capactiy {}",
                                        chunk_binlog_offset,
                                        chunk.capacity()
                                    );

                                    // Since we don't need changelog before the
                                    // `last_binlog_offset`, skip the chunk that *only* contains
                                    // events before `last_binlog_offset`.
                                    if let Some(last_binlog_offset) = &last_binlog_offset {
                                        if let Some(chunk_binlog_offset) = chunk_binlog_offset {
                                            if chunk_binlog_offset < *last_binlog_offset {
                                                tracing::trace!(
                                                    "skip changelog chunk: offset {:?}, capacity {}",
                                                    chunk_binlog_offset,
                                                    chunk.capacity()
                                                );
                                                continue;
                                            }
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
                                        let chunk_cardinality = chunk.cardinality() as u64;
                                        cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                        total_snapshot_processed_rows += chunk_cardinality;
                                        yield Message::Chunk(mapping_chunk(
                                            chunk,
                                            &self.output_indices,
                                        ));
                                    }

                                    // Self::write_backfill_state(
                                    //     &mut self.source_state_handler,
                                    //     upstream_table_id,
                                    //     &split_id,
                                    //     &mut cdc_split,
                                    //     last_binlog_offset.clone(),
                                    // )
                                    // .await?;
                                    break 'backfill_loop;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    current_pk_pos =
                                        Some(get_new_pos(&chunk, &pk_in_output_indices));

                                    tracing::trace!(
                                        "current backfill progress: {:?}",
                                        current_pk_pos
                                    );
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_processed_rows += chunk_cardinality;
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
        } else {
            // Self::write_backfill_state(
            //     &mut self.source_state_handler,
            //     upstream_table_id,
            //     &split_id,
            //     &mut cdc_split,
            //     None,
            // )
            // .await?;
        }

        tracing::debug!(
            actor = self.actor_id,
            "CdcBackfill has already finished and forward messages directly to the downstream"
        );

        // After backfill progress finished
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            // upstream offsets will be removed from the message before forwarding to
            // downstream
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                // persist the backfill state if any
                if let Message::Barrier(barrier) = &msg {
                    // Self::persist_state(&mut self.source_state_handler, barrier.epoch).await?;
                }
                yield msg;
            }
        }
    }

    /// When snapshot read stream ends, we should persist two states:
    /// 1) a backfill finish flag to denote the backfill has done
    /// 2) a consumed binlog offset to denote the last binlog offset
    /// which will be committed to the state store upon next barrier.
    async fn write_backfill_state(
        source_state_handler: &mut SourceStateTableHandler<S>,
        upstream_table_id: u32,
        split_id: &Option<SplitId>,
        cdc_split: &mut Option<SplitImpl>,
        last_binlog_offset: Option<CdcOffset>,
    ) -> StreamExecutorResult<()> {
        if let Some(split_id) = split_id.as_ref() {
            let mut key = split_id.to_string();
            key.push_str(BACKFILL_STATE_KEY_SUFFIX);
            source_state_handler
                .set(key.into(), JsonbVal::from(Value::Bool(true)))
                .await?;

            if let Some(SplitImpl::MysqlCdc(split)) = cdc_split.as_mut()
                && let Some(s) = split.mysql_split.as_mut() {
                let start_offset =
                    last_binlog_offset.as_ref().map(|cdc_offset| {
                        let source_offset =
                            if let CdcOffset::MySql(o) = cdc_offset
                            {
                                DebeziumSourceOffset {
                                    file: Some(o.filename.clone()),
                                    pos: Some(o.position),
                                    ..Default::default()
                                }
                            } else {
                                DebeziumSourceOffset::default()
                            };

                        let mut server = "RW_CDC_".to_string();
                        server.push_str(
                            upstream_table_id.to_string().as_str(),
                        );
                        DebeziumOffset {
                            source_partition: hashmap! {
                                "server".to_string() => server
                            },
                            source_offset,
                        }
                    });

                // persist the last binlog offset into split state
                s.inner.start_offset = start_offset.map(|o| {
                    let value = serde_json::to_value(o).unwrap();
                    value.to_string()
                });
                s.inner.snapshot_done = true;
            }
            if let Some(split_impl) = cdc_split {
                source_state_handler
                    .set(split_impl.id(), split_impl.encode_to_json())
                    .await?
            }
        }
        Ok(())
    }

    async fn persist_state(
        source_state_handler: &mut SourceStateTableHandler<S>,
        new_epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        source_state_handler.state_store.commit(new_epoch).await
    }
}

#[try_stream(ok = Message, error = StreamExecutorError)]
async fn transform_upstream(upstream: BoxedMessageStream, schema: &Schema) {
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
    .map_err(|err| StreamExecutorError::connector_error(err))?;

    pin_mut!(upstream);
    #[for_await]
    for msg in upstream {
        let mut msg = msg?;
        if let Message::Chunk(chunk) = &mut msg {
            let mut parsed_chunk = parse_debezium_chunk(&mut parser, chunk, schema).await?;
            *chunk = parsed_chunk;
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

    // The schema of input chunk (payload varchar, _rw_offset varchar, _rw_table_name varchar)
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
    use risingwave_common::array::{DataChunk, Op, StreamChunk, Vis};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, Datum, JsonbVal};
    use risingwave_common::util::iter_util::ZipEqFast;

    use crate::executor::backfill::cdc_backfill::transform_upstream;
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

        let mut datums: Vec<Datum> = vec![
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
        let chunk =
            StreamChunk::from_parts(vec![Op::Insert], DataChunk::new(columns, Vis::Compact(1)));

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
