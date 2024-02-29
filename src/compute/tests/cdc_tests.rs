// Copyright 2024 RisingWave Labs
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

#![feature(let_chains)]
#![feature(coroutines)]

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch::executor::{Executor as BatchExecutor, RowSeqScanExecutor, ScanRange};
use risingwave_common::array::{Array, ArrayBuilder, DataChunk, Op, StreamChunk, Utf8ArrayBuilder};
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId};
use risingwave_common::types::{Datum, JsonbVal};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_connector::source::cdc::external::mock_external_table::MockExternalTableReader;
use risingwave_connector::source::cdc::external::{
    DebeziumOffset, DebeziumSourceOffset, ExternalTableReaderImpl, MySqlOffset, SchemaTableName,
};
use risingwave_connector::source::cdc::DebeziumCdcSplit;
use risingwave_connector::source::SplitImpl;
use risingwave_hummock_sdk::to_committed_batch_query_epoch;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::MockSource;
use risingwave_stream::executor::{
    expect_first_barrier, ActorContext, AddMutation, Barrier, BoxedMessageStream,
    CdcBackfillExecutor, Execute, Executor as StreamExecutor, ExecutorInfo, ExternalStorageTable,
    MaterializeExecutor, Message, Mutation, StreamExecutorError,
};

// mock upstream binlog offset starting from "1.binlog, pos=0"
pub struct MockOffsetGenExecutor {
    upstream: Option<StreamExecutor>,

    start_offset: u32,
}

impl MockOffsetGenExecutor {
    pub fn new(upstream: StreamExecutor) -> Self {
        Self {
            upstream: Some(upstream),
            start_offset: 0,
        }
    }

    fn next_offset(&mut self) -> anyhow::Result<String> {
        let start_offset = self.start_offset;
        let dbz_offset = DebeziumOffset {
            source_partition: Default::default(),
            source_offset: DebeziumSourceOffset {
                last_snapshot_record: None,
                snapshot: None,
                file: Some("1.binlog".to_string()),
                pos: Some(start_offset as _),
                lsn: None,
                txid: None,
                tx_usec: None,
            },
            is_heartbeat: false,
        };

        self.start_offset += 1;
        let out = serde_json::to_string(&dbz_offset)?;
        Ok(out)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // fake offset in stream chunk
        let mut upstream = self.upstream.take().unwrap().execute();

        // The first barrier mush propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Chunk(chunk) => {
                    let mut offset_builder = Utf8ArrayBuilder::new(chunk.cardinality());
                    assert!(chunk.is_compacted());
                    let (ops, mut columns, vis) = chunk.into_inner();

                    for _ in 0..ops.len() {
                        let offset_str = self.next_offset()?;
                        offset_builder.append(Some(&offset_str));
                    }

                    let offsets = offset_builder.finish();
                    columns.push(offsets.into_ref());
                    yield Message::Chunk(StreamChunk::with_visibility(ops, columns, vis));
                }
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => yield Message::Watermark(watermark),
            }
        }
    }
}

impl Execute for MockOffsetGenExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[tokio::test]
async fn test_cdc_backfill() -> StreamResult<()> {
    use risingwave_common::types::DataType;
    let memory_state_store = MemoryStateStore::new();

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(
        Schema::new(vec![
            Field::unnamed(DataType::Jsonb), // payload
        ]),
        vec![0],
    );

    // mock upstream offset (start from "1.binlog, pos=0") for ingested chunks
    let mock_offset_executor = StreamExecutor::new(
        ExecutorInfo {
            schema: Schema::new(vec![
                Field::unnamed(DataType::Jsonb),   // payload
                Field::unnamed(DataType::Varchar), // _rw_offset
            ]),
            pk_indices: vec![0],
            identity: "MockOffsetGenExecutor".to_string(),
        },
        MockOffsetGenExecutor::new(source).boxed(),
    );

    let binlog_file = String::from("1.binlog");
    // mock binlog watermarks for backfill
    // initial low watermark: 1.binlog, pos=2 and expected behaviors:
    // - ignore events before (1.binlog, pos=2);
    // - apply events in the range of (1.binlog, pos=2, 1.binlog, pos=4) to the snapshot
    let binlog_watermarks = vec![
        MySqlOffset::new(binlog_file.clone(), 2), // binlog low watermark
        MySqlOffset::new(binlog_file.clone(), 4),
        MySqlOffset::new(binlog_file.clone(), 6),
        MySqlOffset::new(binlog_file.clone(), 8),
        MySqlOffset::new(binlog_file.clone(), 10),
    ];

    let table_name = SchemaTableName::new("mock_table".to_string(), "public".to_string());
    let table_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "id"), // primary key
        Field::with_name(DataType::Float64, "price"),
    ]);
    let table_pk_indices = vec![0];
    let table_pk_order_types = vec![OrderType::ascending()];
    let external_table = ExternalStorageTable::new(
        TableId::new(1234),
        table_name,
        ExternalTableReaderImpl::Mock(MockExternalTableReader::new(binlog_watermarks)),
        table_schema.clone(),
        table_pk_order_types,
        table_pk_indices.clone(),
        vec![0, 1],
    );

    let actor_id = 0x1a;

    let state_schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "split_id"),
        Field::with_name(DataType::Int64, "id"), // pk
        Field::with_name(DataType::Boolean, "backfill_finished"),
        Field::with_name(DataType::Int64, "row_count"),
        Field::with_name(DataType::Jsonb, "cdc_offset"),
    ]);

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), state_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), state_schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), state_schema[2].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(3), state_schema[3].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(4), state_schema[4].data_type.clone()),
    ];

    let state_table = StateTable::new_without_distribution(
        memory_state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0_usize],
    )
    .await;

    let cdc_backfill = StreamExecutor::new(
        ExecutorInfo {
            schema: table_schema.clone(),
            pk_indices: table_pk_indices,
            identity: "CdcBackfillExecutor".to_string(),
        },
        CdcBackfillExecutor::new(
            ActorContext::for_test(actor_id),
            external_table,
            mock_offset_executor,
            vec![0, 1],
            None,
            Arc::new(StreamingMetrics::unused()),
            state_table,
            4, // 4 rows in a snapshot chunk
            false,
        )
        .boxed(),
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let materialize_table_id = TableId::new(5678);
    let mut materialize = MaterializeExecutor::for_test(
        cdc_backfill,
        memory_state_store.clone(),
        materialize_table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        vec![0.into(), 1.into()],
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::Overwrite,
    )
    .await
    .boxed()
    .execute();

    let chunk1_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 10.01}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 2, "price": 2.02}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 3, "price": 3.03}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 4, "price": 4.04}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 5, "price": 5.05}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 6.06}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];

    let chunk2_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 10.08}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 199, "price": 40.5}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 978, "price": 72.6}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 134, "price": 41.7}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];

    let chunk1_datums: Vec<Datum> = chunk1_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();

    let chunk2_datums: Vec<Datum> = chunk2_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();

    let chunk_schema = Schema::new(vec![
        Field::unnamed(DataType::Jsonb), // payload
    ]);

    let stream_chunk1 = create_stream_chunk(chunk1_datums, &chunk_schema);
    let stream_chunk2 = create_stream_chunk(chunk2_datums, &chunk_schema);

    // The first barrier
    let curr_epoch = 11;
    let mut splits = HashMap::new();
    splits.insert(
        actor_id,
        vec![SplitImpl::MysqlCdc(DebeziumCdcSplit::new(0, None, None))],
    );
    let init_barrier =
        Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add(AddMutation {
            adds: HashMap::new(),
            added_actors: HashSet::new(),
            splits,
            pause: false,
        }));

    tx.send_barrier(init_barrier);

    // assert barrier is forwarded to mview
    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: Some(_),
            ..
        }) if epoch.curr == curr_epoch
    ));

    // start the stream pipeline src -> backfill -> mview
    let mview_handle = tokio::spawn(consume_message_stream(materialize));

    // ingest data and barrier
    let interval = Duration::from_millis(10);
    tx.push_chunk(stream_chunk1);

    tokio::time::sleep(interval).await;
    tx.push_barrier(curr_epoch + 1, false);

    tx.push_chunk(stream_chunk2);

    tokio::time::sleep(interval).await;
    tx.push_barrier(curr_epoch + 2, false);

    tokio::time::sleep(interval).await;
    tx.push_barrier(curr_epoch + 3, true);

    // scan the final result of the mv table
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), table_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), table_schema[1].data_type.clone()),
    ];
    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = StorageTable::for_test(
        memory_state_store.clone(),
        materialize_table_id,
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        value_indices,
    );

    let scan = Box::new(RowSeqScanExecutor::new(
        table.clone(),
        vec![ScanRange::full()],
        true,
        to_committed_batch_query_epoch(u64::MAX),
        1024,
        "RowSeqExecutor2".to_string(),
        None,
        None,
    ));
    let mut stream = scan.execute();
    while let Some(message) = stream.next().await {
        println!("[scan] chunk: {:#?}", message.unwrap());
    }

    mview_handle.await.unwrap()?;

    Ok(())
}

fn create_stream_chunk(datums: Vec<Datum>, schema: &Schema) -> StreamChunk {
    let mut builders = schema.create_array_builders(8);
    for datum in &datums {
        builders[0].append(datum);
    }
    let columns = builders
        .into_iter()
        .map(|builder| builder.finish().into())
        .collect();

    let ops = vec![Op::Insert; datums.len()];
    StreamChunk::from_parts(ops, DataChunk::new(columns, datums.len()))
}

async fn consume_message_stream(mut stream: BoxedMessageStream) -> StreamResult<()> {
    while let Some(message) = stream.next().await {
        let message = message?;
        match message {
            Message::Watermark(_) => {
                break;
            }
            Message::Chunk(c) => {
                println!("[mv] chunk: {:#?}", c);
            }
            Message::Barrier(b) => {
                if let Some(m) = b.mutation
                    && matches!(*m, Mutation::Stop(_))
                {
                    println!("encounter stop barrier");
                    break;
                }
            }
        }
    }
    Ok(())
}
