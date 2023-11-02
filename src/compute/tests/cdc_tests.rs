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

#![feature(let_chains)]
#![feature(coroutines)]

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch::executor::{Executor as BatchExecutor, RowSeqScanExecutor, ScanRange};
use risingwave_common::array::{
    Array, ArrayBuilder, StreamChunk, StreamChunkTestExt, Utf8ArrayBuilder,
};
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_connector::source::cdc::{CdcSplitBase, DebeziumCdcSplit, MySqlCdcSplit};
use risingwave_connector::source::external::{
    DebeziumOffset, DebeziumSourceOffset, ExternalTableReaderImpl, MySqlOffset, SchemaTableName,
};
use risingwave_connector::source::{MockExternalTableReader, SplitImpl};
use risingwave_hummock_sdk::to_committed_batch_query_epoch;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::external::ExternalStorageTable;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::MockSource;
use risingwave_stream::executor::{
    default_source_internal_table, expect_first_barrier, ActorContext, Barrier,
    BoxedExecutor as StreamBoxedExecutor, BoxedMessageStream, CdcBackfillExecutor, Executor,
    MaterializeExecutor, Message, Mutation, PkIndices, PkIndicesRef, SourceStateTableHandler,
    StreamExecutorError,
};

// mock upstream binlog offset starting from "1.binlog, pos=0"
pub struct MockOffsetGenExecutor {
    upstream: Option<StreamBoxedExecutor>,

    schema: Schema,

    pk_indices: PkIndices,

    identity: String,

    start_offset: u32,
}

impl MockOffsetGenExecutor {
    pub fn new(upstream: StreamBoxedExecutor, schema: Schema, pk_indices: PkIndices) -> Self {
        Self {
            upstream: Some(upstream),
            schema,
            pk_indices,
            identity: "MockOffsetGenExecutor".to_string(),
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

impl Executor for MockOffsetGenExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[tokio::test]
async fn test_cdc_backfill() -> StreamResult<()> {
    use risingwave_common::types::DataType;
    let memory_state_store = MemoryStateStore::new();

    let table_id = TableId::new(1002);
    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int64), // primary key
        Field::unnamed(DataType::Float64),
        Field::unnamed(DataType::Varchar),
    ]);
    let column_ids = vec![0.into(), 1.into(), 2.into()];

    let pk_indices = vec![0];

    let (mut tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());
    let _actor_ctx = ActorContext::create(0x3a3a3a);

    // mock upstream offset (start from "1.binlog, pos=0") for ingested chunks
    let mock_offset_executor =
        MockOffsetGenExecutor::new(Box::new(source), schema.clone(), pk_indices.clone());

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
    let external_table = ExternalStorageTable::new(
        table_id,
        table_name,
        ExternalTableReaderImpl::Mock(MockExternalTableReader::new(binlog_watermarks)),
        schema.clone(),
        vec![OrderType::ascending()],
        pk_indices,
        vec![0, 1],
    );

    let source_state_handler = SourceStateTableHandler::from_table_catalog(
        &default_source_internal_table(0x2333),
        MemoryStateStore::new(),
    )
    .await;

    let actor_id = 0x1a;
    let cdc_backfill = CdcBackfillExecutor::new(
        ActorContext::create(actor_id),
        external_table,
        Box::new(mock_offset_executor),
        vec![0, 1, 2],
        None,
        schema.clone(),
        vec![0],
        Arc::new(StreamingMetrics::unused()),
        source_state_handler,
        false,
        4, // 4 rows in a snapshot chunk
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let mut materialize = MaterializeExecutor::for_test(
        Box::new(cdc_backfill),
        memory_state_store.clone(),
        table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        column_ids.clone(),
        4,
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::Overwrite,
    )
    .await
    .boxed()
    .execute();

    // push upstream chunks
    let stream_chunk1 = StreamChunk::from_pretty(
        " I F
            + 1 10.01
            + 2 2.02
            + 3 3.03 // binlog pos=2
            + 4 4.04
            + 5 5.05 // binlog pos=4
            + 6 6.06",
    );
    let stream_chunk2 = StreamChunk::from_pretty(
        " I F
            + 6 10.08
            + 199 40.5
            + 978 72.6
            + 134 41.7",
    );

    // The first barrier
    let curr_epoch = 11;
    let mut splits = HashMap::new();
    splits.insert(
        actor_id,
        vec![SplitImpl::MysqlCdc(DebeziumCdcSplit {
            mysql_split: Some(MySqlCdcSplit {
                inner: CdcSplitBase {
                    split_id: 0,
                    start_offset: None,
                    snapshot_done: false,
                },
            }),
            pg_split: None,
            _phantom: PhantomData,
        })],
    );
    let init_barrier = Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add {
        adds: HashMap::new(),
        added_actors: HashSet::new(),
        splits,
        pause: false,
    });

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
        ColumnDesc::unnamed(ColumnId::from(0), schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), schema[1].data_type.clone()),
    ];
    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = StorageTable::for_test(
        memory_state_store.clone(),
        table_id,
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
    ));
    let mut stream = scan.execute();
    while let Some(message) = stream.next().await {
        println!("[scan] chunk: {:#?}", message.unwrap());
    }

    mview_handle.await.unwrap()?;

    Ok(())
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
