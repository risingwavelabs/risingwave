#![feature(return_position_impl_trait_in_trait)]
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
#![feature(coroutines)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]

use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::future::{poll_fn, Future};
use risingwave_common::array::error::anyhow;
use futures::prelude::stream::PollNext;
use futures::stream::select_with_strategy;
use futures_async_stream::{try_stream, for_await};
use futures::{Stream, StreamExt, TryStreamExt, select};

use risingwave_connector::dispatch_sink;
use futures::prelude::future::{try_join_all, Either};
use risingwave_common::catalog::TableId;
use risingwave_common::error::{RwError, anyhow_error};
use risingwave_connector::sink::catalog::SinkType;
use risingwave_connector::sink::log_store::{TruncateOffset, LogStoreResult, LogStoreReadItem};
use risingwave_connector::source::StreamChunkWithState;
use risingwave_connector::{sink::{build_sink, Sink, SinkWriterParam, log_store::LogReader, SinkParam, LogSinker}, source::{datagen::{DatagenSplitEnumerator, DatagenProperties, DatagenSplitReader}, SplitEnumerator, create_split_reader, SplitReader, Column, DataType}, parser::{ParserConfig, SpecificParserConfig, EncodingProperties, ProtocolProperties}};
use risingwave_stream::executor::{Message, StreamExecutorError, Barrier, SinkExecutor, StreamExecutorResult};
use tokio::sync::{RwLock, oneshot};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Receiver;
use tokio::time::sleep;

pub struct MockRangeLogReader{
    mock_source: MockDatagenSource,
    current_epoch: u64,
}

impl LogReader for MockRangeLogReader {
    async fn init(&mut self) -> LogStoreResult<()> {
        Ok(())
    }

    async fn next_item(
        &mut self,
    ) -> LogStoreResult<(u64, LogStoreReadItem)>{
        match Box::pin(self.mock_source.into_stream()).next().await.unwrap().unwrap(){
            Message::Barrier(barrier) => {
                self.current_epoch = barrier.epoch.curr;
                return Ok((self.current_epoch, LogStoreReadItem::Barrier{is_checkpoint: true}));
            },
            Message::Chunk(chunk) => {
                return Ok((self.current_epoch, LogStoreReadItem::StreamChunk{chunk, chunk_id: 0 }));
            },
            _ => return Err(anyhow_error!("Can't assert message type".to_string())),
            Message::Watermark(_) => todo!(),
            
        }
    }

    async fn truncate(
        &mut self,
        offset: TruncateOffset,
    ) -> LogStoreResult<()>{
        Ok(())
    }
    
}

pub struct MockDatagenSource{
    datagen_split_reader: DatagenSplitReader,
    epoch: AtomicU64,
    rx: RwLock<Receiver<()>>,
}
impl MockDatagenSource{
    pub async fn new(rx: Receiver<()>) -> MockDatagenSource{
        let properties= DatagenProperties{
            split_num: Some("1".to_string()),
            rows_per_second: 10000,
            fields: HashMap::default(),
        };
        let mut datagen_enumerator = DatagenSplitEnumerator::new(properties.clone(), Default::default()).await.unwrap();
        let parser_config = ParserConfig {
            specific: SpecificParserConfig {
                key_encoding_config: None,
                encoding_config: EncodingProperties::Native,
                protocol_config: ProtocolProperties::Native,
            },
            ..Default::default()
        };
        let mock_datum = vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int64,
                is_visible: true,
            },
            Column {
                name: "random_int".to_string(),
                data_type: DataType::Int32,
                is_visible: true,
            },
            Column {
                name: "random_int2".to_string(),
                data_type: DataType::Int32,
                is_visible: true,
            },
        ];
        
        let splits = datagen_enumerator.list_splits().await.unwrap();
        let datagen_split_reader = DatagenSplitReader::new(properties.clone(), splits, parser_config.clone(), Default::default(), Some(mock_datum.clone())).await.unwrap();

        MockDatagenSource{ epoch: AtomicU64::new(0), rx: RwLock::new(rx), datagen_split_reader }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_stream_data(&self){
        while let item = self.datagen_split_reader.into_stream().next().await.unwrap().unwrap(){
            yield Message::Chunk(item.chunk);
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_stream(&mut self){
        let stream = select_with_strategy(self.barrier_to_message_stream().map_ok(Either::Left), self.into_stream_data().map_ok(Either::Right), |_: &mut PollNext| PollNext::Left);
        #[for_await]
        for message in stream{
            match message.unwrap() {
                Either::Left(Message::Barrier(barrier)) => {
                    if barrier.is_with_stop_mutation() {
                        break;
                    }
                    yield Message::Barrier(barrier);
                },
                Either::Right(Message::Chunk(chunk)) => yield Message::Chunk(chunk),
                _ => return Err(StreamExecutorError::from("Can't assert message type".to_string())),
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn barrier_to_message_stream(&self) {
        loop {
            let prev_epoch = self.epoch.fetch_sub(1, Ordering::SeqCst);
            let mut barrier = Barrier::with_prev_epoch_for_test(prev_epoch,self.epoch.load(Ordering::SeqCst));
            if let Ok(())  = self.rx.write().await.try_recv(){
                barrier = barrier.with_mutation(risingwave_stream::executor::Mutation::Stop(HashSet::default()));
            }
            yield Message::Barrier(barrier);
            sleep(Duration::from_secs(1)).await;

        }
    }
}

async fn consume_log_stream<S: Sink> (sink: S, log_reader: MockRangeLogReader, sink_writer_param: SinkWriterParam) -> Result<(),String>{
    let log_sinker = sink.new_log_sinker(sink_writer_param).await.unwrap();
    if let Err(e) = log_sinker.consume_log_and_sink(log_reader).await{
        return Err(e.to_string());
    }
    return Err("Stream closed".to_string());
}

#[tokio::main]
async fn main() {
    // let sinkParam = SinkParam{ sink_id: todo!(), properties: todo!(), columns: todo!(), downstream_pk: todo!(), sink_type: todo!(), format_desc: todo!(), db_name: todo!(), sink_from_name: todo!(), target_table: todo!() };
    // let sink = build_sink(sinkParam).unwrap(); 
    let (tx, rx) = oneshot::channel();
    let mock_datagen_source = MockDatagenSource::new(rx).await;
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int64,
            is_visible: true,
        },
        Column {
            name: "random_int".to_string(),
            data_type: DataType::Int32,
            is_visible: true,
        },
        Column {
            name: "random_int2".to_string(),
            data_type: DataType::Int32,
            is_visible: true,
        },
    ];
    let sink_param = SinkParam {
        sink_id: "test".to_string(),
        properties: HashMap::default(),
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int64,
                is_visible: true,
            },
            Column {
                name: "random_int".to_string(),
                data_type: DataType::Int32,
                is_visible: true,
            },
            Column {
                name: "random_int2".to_string(),
                data_type: DataType::Int32,
                is_visible: true,
            },
        ],
        downstream_pk: vec!["id".to_string()],
        sink_type: SinkType::AppendOnly,
        format_desc: None,
        db_name: "test".to_string(),
        sink_from_name: "test".to_string(),
        target_table: Some(TableId::new(1)),
    };
    let sink = build_sink(sink_param).unwrap();
    let sink_writer_param = SinkWriterParam::for_test();
    dispatch_sink!(sink, sink, {
        execute_consume_log(
            sink,
            mock_datagen_source,
            sink_writer_param
        )
    })
}

async fn execute_consume_log<S: Sink, R: LogReader>(
    sink: S,
    log_reader: R,
    sink_writer_param: SinkWriterParam,
) -> StreamExecutorResult<Message> {
    let metrics = sink_writer_param.sink_metrics.clone();
    let log_sinker = sink.new_log_sinker(sink_writer_param).await?;

    let log_reader = log_reader
        .transform_chunk(move |chunk| {
                chunk
        });
    log_sinker.consume_log_and_sink(log_reader).await?;
    Err(anyhow!("end of stream").into())
}
