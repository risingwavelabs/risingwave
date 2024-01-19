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
#![feature(coroutines)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(let_chains)]

use core::str::FromStr;
use std::collections::HashMap;

use clap::Parser;
use futures::prelude::future::Either;
use futures::prelude::stream::{BoxStream, PollNext};
use futures::stream::select_with_strategy;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::anyhow_error;
use risingwave_connector::dispatch_sink;
use risingwave_connector::parser::{
    EncodingProperties, ParserConfig, ProtocolProperties, SpecificParserConfig,
};
use risingwave_connector::sink::catalog::{
    SinkEncode, SinkFormat, SinkFormatDesc, SinkId, SinkType,
};
use risingwave_connector::sink::log_store::{
    LogReader, LogStoreReadItem, LogStoreResult, TruncateOffset,
};
use risingwave_connector::sink::mock_coordination_client::MockMetaClient;
use risingwave_connector::sink::{
    build_sink, LogSinker, Sink, SinkError, SinkMetaClient, SinkParam, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT,
};
use risingwave_connector::source::datagen::{
    DatagenProperties, DatagenSplitEnumerator, DatagenSplitReader,
};
use risingwave_connector::source::{Column, DataType, SplitEnumerator, SplitReader};
use risingwave_pb::connector_service::SinkPayloadFormat;
use risingwave_stream::executor::test_utils::prelude::ColumnDesc;
use risingwave_stream::executor::{Barrier, Message, MessageStreamItem, StreamExecutorError};
use serde::{Deserialize, Deserializer};
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::time::{sleep, Instant};

const CHECKPOINT_INTERVAL: u64 = 1000;
const THROUGHPUT_METRIC_RECORD_INTERVAL: u128 = 500;
const BENCH_TIME: u64 = 20;
const BENCH_TEST: &str = "bench_test";

pub struct MockRangeLogReader {
    upstreams: BoxStream<'static, MessageStreamItem>,
    current_epoch: u64,
    chunk_id: usize,
    throughput_metric: Option<ThroughputMetric>,
    stop_rx: tokio::sync::mpsc::Receiver<()>,
    result_tx: Option<Sender<ThroughputMetric>>,
}

impl LogReader for MockRangeLogReader {
    async fn init(&mut self) -> LogStoreResult<()> {
        self.throughput_metric.as_mut().unwrap().add_metric(0);
        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        tokio::select! {
            _ = self.stop_rx.recv() => {
                self.result_tx
                        .take()
                        .unwrap()
                        .send(self.throughput_metric.take().unwrap())
                        .map_err(|_| anyhow_error!("Can't send throughput_metric"))?;
                    futures::future::pending().await
                },
            item = self.upstreams.next() => {
                match item.unwrap().unwrap() {
                    Message::Barrier(barrier) => {
                        let prev_epoch = self.current_epoch;
                        self.current_epoch = barrier.epoch.curr;
                        Ok((
                            prev_epoch,
                            LogStoreReadItem::Barrier {
                                is_checkpoint: true,
                            },
                        ))
                    }
                    Message::Chunk(chunk) => {
                        self.throughput_metric.as_mut().unwrap().add_metric(chunk.capacity());
                        self.chunk_id += 1;
                        Ok((
                            self.current_epoch,
                            LogStoreReadItem::StreamChunk {
                                chunk,
                                chunk_id: self.chunk_id,
                            },
                        ))
                    }
                    _ => Err(anyhow_error!("Can't assert message type".to_string())),
                }
            }
        }
    }

    async fn truncate(&mut self, _offset: TruncateOffset) -> LogStoreResult<()> {
        Ok(())
    }

    async fn rewind(&mut self) -> LogStoreResult<(bool, Option<Bitmap>)> {
        Ok((false, None))
    }
}

impl MockRangeLogReader {
    fn new(
        mock_source: MockDatagenSource,
        throughput_metric: ThroughputMetric,
        stop_rx: tokio::sync::mpsc::Receiver<()>,
        result_tx: Sender<ThroughputMetric>,
    ) -> MockRangeLogReader {
        MockRangeLogReader {
            upstreams: mock_source.into_stream().boxed(),
            current_epoch: 0,
            chunk_id: 0,
            throughput_metric: Some(throughput_metric),
            stop_rx,
            result_tx: Some(result_tx),
        }
    }
}

struct ThroughputMetric {
    chunk_size_list: Vec<(u64, Instant)>,
    accumulate_chunk_size: u64,
    // Record every `record_interval` ms
    record_interval: u128,
    last_record_time: Instant,
}

impl ThroughputMetric {
    pub fn new() -> Self {
        Self {
            chunk_size_list: vec![],
            accumulate_chunk_size: 0,
            record_interval: THROUGHPUT_METRIC_RECORD_INTERVAL,
            last_record_time: Instant::now(),
        }
    }

    pub fn add_metric(&mut self, chunk_size: usize) {
        self.accumulate_chunk_size += chunk_size as u64;
        if Instant::now()
            .duration_since(self.last_record_time)
            .as_millis()
            > self.record_interval
        {
            self.chunk_size_list
                .push((self.accumulate_chunk_size, Instant::now()));
            self.last_record_time = Instant::now();
        }
    }

    pub fn get_throughput(&self) -> Vec<String> {
        #[allow(clippy::disallowed_methods)]
        self.chunk_size_list
            .iter()
            .zip(self.chunk_size_list.iter().skip(1))
            .map(|(current, next)| {
                let throughput = (next.0 - current.0) * 1000
                    / (next.1.duration_since(current.1).as_millis() as u64);
                format!("{} rows/s", throughput)
            })
            .collect()
    }
}

pub struct MockDatagenSource {
    datagen_split_readers: Vec<DatagenSplitReader>,
}
impl MockDatagenSource {
    pub async fn new(
        rows_per_second: u64,
        source_schema: Vec<Column>,
        split_num: String,
    ) -> MockDatagenSource {
        let properties = DatagenProperties {
            split_num: Some(split_num),
            rows_per_second,
            fields: HashMap::default(),
        };
        let mut datagen_enumerator =
            DatagenSplitEnumerator::new(properties.clone(), Default::default())
                .await
                .unwrap();
        let parser_config = ParserConfig {
            specific: SpecificParserConfig {
                key_encoding_config: None,
                encoding_config: EncodingProperties::Native,
                protocol_config: ProtocolProperties::Native,
            },
            ..Default::default()
        };
        let mut datagen_split_readers = vec![];
        let mut datagen_splits = datagen_enumerator.list_splits().await.unwrap();
        while let Some(splits) = datagen_splits.pop() {
            datagen_split_readers.push(
                DatagenSplitReader::new(
                    properties.clone(),
                    vec![splits],
                    parser_config.clone(),
                    Default::default(),
                    Some(source_schema.clone()),
                )
                .await
                .unwrap(),
            );
        }
        MockDatagenSource {
            datagen_split_readers,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn source_to_data_stream(mut self) {
        let mut readers = vec![];
        while let Some(reader) = self.datagen_split_readers.pop() {
            readers.push(reader.into_stream());
        }
        loop {
            for i in &mut readers {
                let item = i.next().await.unwrap().unwrap();
                yield Message::Chunk(item.chunk);
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_stream(self) {
        let stream = select_with_strategy(
            Self::barrier_to_message_stream().map_ok(Either::Left),
            self.source_to_data_stream().map_ok(Either::Right),
            |_: &mut PollNext| PollNext::Left,
        );
        #[for_await]
        for message in stream {
            match message.unwrap() {
                Either::Left(Message::Barrier(barrier)) => {
                    yield Message::Barrier(barrier);
                }
                Either::Right(Message::Chunk(chunk)) => yield Message::Chunk(chunk),
                _ => {
                    return Err(StreamExecutorError::from(
                        "Can't assert message type".to_string(),
                    ))
                }
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn barrier_to_message_stream() {
        let mut epoch = 0_u64;
        loop {
            let prev_epoch = epoch;
            epoch += 1;
            let barrier = Barrier::with_prev_epoch_for_test(epoch, prev_epoch);
            yield Message::Barrier(barrier);
            sleep(tokio::time::Duration::from_millis(CHECKPOINT_INTERVAL)).await;
        }
    }
}

async fn consume_log_stream<S: Sink>(
    sink: S,
    mut log_reader: MockRangeLogReader,
    mut sink_writer_param: SinkWriterParam,
) -> Result<(), String>
where
    <S as risingwave_connector::sink::Sink>::Coordinator: std::marker::Send,
    <S as risingwave_connector::sink::Sink>::Coordinator: 'static,
{
    if let Ok(coordinator) = sink.new_coordinator().await {
        sink_writer_param.meta_client = Some(SinkMetaClient::MockMetaClient(MockMetaClient::new(
            Box::new(coordinator),
        )));
        sink_writer_param.vnode_bitmap = Some(Bitmap::ones(1));
    }
    let log_sinker = sink.new_log_sinker(sink_writer_param).await.unwrap();
    if let Err(e) = log_sinker.consume_log_and_sink(&mut log_reader).await {
        return Err(e.to_report_string());
    }
    Err("Stream closed".to_string())
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TableSchemaFromYml {
    table_name: String,
    pk_indices: Vec<usize>,
    columns: Vec<ColumnDescFromYml>,
}

impl TableSchemaFromYml {
    pub fn get_source_schema(&self) -> Vec<Column> {
        self.columns
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                data_type: column.r#type.clone(),
                is_visible: true,
            })
            .collect()
    }

    pub fn get_sink_schema(&self) -> Vec<ColumnDesc> {
        self.columns
            .iter()
            .map(|column| {
                ColumnDesc::named(column.name.clone(), ColumnId::new(1), column.r#type.clone())
            })
            .collect()
    }
}
#[derive(Debug, Deserialize)]
struct ColumnDescFromYml {
    name: String,
    #[serde(deserialize_with = "deserialize_datatype")]
    r#type: DataType,
}

fn deserialize_datatype<'de, D>(deserializer: D) -> Result<DataType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    DataType::from_str(s).map_err(serde::de::Error::custom)
}

fn read_table_schema_from_yml(path: &str) -> TableSchemaFromYml {
    let data = std::fs::read_to_string(path).unwrap();
    let table: TableSchemaFromYml = serde_yaml::from_str(&data).unwrap();
    table
}

fn read_sink_option_from_yml(path: &str) -> HashMap<String, HashMap<String, String>> {
    let data = std::fs::read_to_string(path).unwrap();
    let sink_option: HashMap<String, HashMap<String, String>> =
        serde_yaml::from_str(&data).unwrap();
    sink_option
}

#[derive(Parser, Debug)]
pub struct Config {
    #[clap(long, default_value = "./sink_bench/schema.yml")]
    schema_path: String,

    #[clap(short, long, default_value = "./sink_bench/sink_option.yml")]
    option_path: String,

    #[clap(short, long, default_value = BENCH_TEST)]
    sink: String,

    #[clap(short, long)]
    rows_per_second: u64,

    #[clap(long, default_value = "10")]
    split_num: String,
}

fn mock_from_legacy_type(
    connector: &str,
    r#type: &str,
) -> Result<Option<SinkFormatDesc>, SinkError> {
    use risingwave_connector::sink::redis::RedisSink;
    use risingwave_connector::sink::Sink as _;
    if connector.eq(RedisSink::SINK_NAME) {
        let format = match r#type {
            SINK_TYPE_APPEND_ONLY => SinkFormat::AppendOnly,
            SINK_TYPE_UPSERT => SinkFormat::Upsert,
            _ => {
                return Err(SinkError::Config(risingwave_common::array::error::anyhow!(
                    "sink type unsupported: {}",
                    r#type
                )))
            }
        };
        Ok(Some(SinkFormatDesc {
            format,
            encode: SinkEncode::Json,
            options: Default::default(),
        }))
    } else {
        SinkFormatDesc::from_legacy_type(connector, r#type)
    }
}

fn print_throughput_result(throughput_metric: ThroughputMetric) {
    let throughput_result = throughput_metric.get_throughput();
    if throughput_result.is_empty() {
        println!("Throughput Sink: Don't get Throughput, please check");
    } else {
        println!("Throughput Sink: {:?}", throughput_result);
    }
}

#[tokio::main]
async fn main() {
    let cfg = Config::parse();
    let table_schema = read_table_schema_from_yml(&cfg.schema_path);
    let mock_datagen_source = MockDatagenSource::new(
        cfg.rows_per_second,
        table_schema.get_source_schema(),
        cfg.split_num,
    )
    .await;
    let (data_size_tx, data_size_rx) = tokio::sync::oneshot::channel::<ThroughputMetric>();
    let (stop_tx, stop_rx) = tokio::sync::mpsc::channel::<()>(5);
    let throughput_metric = ThroughputMetric::new();

    let mut mock_range_log_reader = MockRangeLogReader::new(
        mock_datagen_source,
        throughput_metric,
        stop_rx,
        data_size_tx,
    );
    if cfg.sink.eq(&BENCH_TEST.to_string()) {
        println!("Start Sink Bench!, Wait {:?}s", BENCH_TIME);
        tokio::spawn(async move {
            mock_range_log_reader.init().await.unwrap();
            loop {
                mock_range_log_reader.next_item().await.unwrap();
            }
        });
    } else {
        let properties = read_sink_option_from_yml(&cfg.option_path)
            .get(&cfg.sink)
            .expect("Sink type error")
            .clone();

        let connector = properties.get("connector").unwrap().clone();
        let format_desc = mock_from_legacy_type(
            &connector.clone(),
            properties.get("type").unwrap_or(&"append-only".to_string()),
        )
        .unwrap();
        let sink_param = SinkParam {
            sink_id: SinkId::new(1),
            properties,
            columns: table_schema.get_sink_schema(),
            downstream_pk: table_schema.pk_indices,
            sink_type: SinkType::AppendOnly,
            format_desc,
            db_name: "not_need_set".to_string(),
            sink_from_name: "not_need_set".to_string(),
        };
        let sink = build_sink(sink_param).unwrap();
        let mut sink_writer_param = SinkWriterParam::for_test();
        println!("Start Sink Bench!, Wait {:?}s", BENCH_TIME);
        sink_writer_param.connector_params.sink_payload_format = SinkPayloadFormat::StreamChunk;
        tokio::spawn(async move {
            dispatch_sink!(sink, sink, {
                consume_log_stream(sink, mock_range_log_reader, sink_writer_param).boxed()
            })
            .await
            .unwrap();
        });
    }
    sleep(tokio::time::Duration::from_secs(BENCH_TIME)).await;
    println!("Bench Over!");
    stop_tx.send(()).await.unwrap();
    print_throughput_result(data_size_rx.await.unwrap());
}
