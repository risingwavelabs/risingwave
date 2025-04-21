// Copyright 2025 RisingWave Labs
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
#![recursion_limit = "256"]

use core::str::FromStr;
use core::sync::atomic::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::anyhow;
use clap::Parser;
use futures::channel::oneshot;
use futures::prelude::future::Either;
use futures::prelude::stream::{BoxStream, PollNext};
use futures::stream::select_with_strategy;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use plotters::backend::SVGBackend;
use plotters::chart::ChartBuilder;
use plotters::drawing::IntoDrawingArea;
use plotters::element::{Circle, EmptyElement};
use plotters::series::{LineSeries, PointSeries};
use plotters::style::{IntoFont, RED, WHITE};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::ColumnId;
use risingwave_common::types::DataType;
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
    LogSinker, SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT, Sink, SinkError, SinkMetaClient, SinkParam,
    SinkWriterParam, build_sink,
};
use risingwave_connector::source::datagen::{
    DatagenProperties, DatagenSplitEnumerator, DatagenSplitReader,
};
use risingwave_connector::source::{
    Column, SourceContext, SourceEnumeratorContext, SplitEnumerator, SplitReader,
};
use risingwave_stream::executor::test_utils::prelude::ColumnDesc;
use risingwave_stream::executor::{Barrier, Message, MessageStreamItem, StreamExecutorError};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Deserializer};
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::time::sleep;

const CHECKPOINT_INTERVAL: u64 = 1000;
const THROUGHPUT_METRIC_RECORD_INTERVAL: u64 = 500;
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
                        .map_err(|_| anyhow!("Can't send throughput_metric"))?;
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
                                new_vnode_bitmap: None,
                                is_stop: false,
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
                    _ => Err(anyhow!("Can't assert message type".to_owned())),
                }
            }
        }
    }

    fn truncate(&mut self, _offset: TruncateOffset) -> LogStoreResult<()> {
        Ok(())
    }

    async fn rewind(&mut self) -> LogStoreResult<()> {
        Err(anyhow!("should not call rewind"))
    }

    async fn start_from(&mut self, _start_offset: Option<u64>) -> LogStoreResult<()> {
        Ok(())
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
    accumulate_chunk_size: Arc<AtomicU64>,
    stop_tx: oneshot::Sender<()>,
    vec_rx: oneshot::Receiver<Vec<u64>>,
}

impl ThroughputMetric {
    pub fn new() -> Self {
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let (vec_tx, vec_rx) = oneshot::channel::<Vec<u64>>();
        let accumulate_chunk_size = Arc::new(AtomicU64::new(0));
        let accumulate_chunk_size_clone = accumulate_chunk_size.clone();
        tokio::spawn(async move {
            let mut chunk_size_list = vec![];
            loop {
                tokio::select! {
                    _ = sleep(tokio::time::Duration::from_millis(
                        THROUGHPUT_METRIC_RECORD_INTERVAL,
                    )) => {
                        chunk_size_list.push(accumulate_chunk_size_clone.load(Ordering::Relaxed));
                    }
                    _ = &mut stop_rx => {
                        vec_tx.send(chunk_size_list).unwrap();
                        break;
                    }
                }
            }
        });

        Self {
            accumulate_chunk_size,
            stop_tx,
            vec_rx,
        }
    }

    pub fn add_metric(&mut self, chunk_size: usize) {
        self.accumulate_chunk_size
            .fetch_add(chunk_size as u64, Ordering::Relaxed);
    }

    pub async fn print_throughput(self) {
        self.stop_tx.send(()).unwrap();
        let throughput_sum_vec = self.vec_rx.await.unwrap();
        #[allow(clippy::disallowed_methods)]
        let throughput_vec = throughput_sum_vec
            .iter()
            .zip(throughput_sum_vec.iter().skip(1))
            .map(|(current, next)| (next - current) * 1000 / THROUGHPUT_METRIC_RECORD_INTERVAL)
            .collect_vec();
        if throughput_vec.is_empty() {
            println!("Throughput Sink: Don't get Throughput, please check");
            return;
        }
        let avg = throughput_vec.iter().sum::<u64>() / throughput_vec.len() as u64;
        let throughput_vec_sorted = throughput_vec.iter().sorted().collect_vec();
        let p90 = throughput_vec_sorted[throughput_vec_sorted.len() * 90 / 100];
        let p95 = throughput_vec_sorted[throughput_vec_sorted.len() * 95 / 100];
        let p99 = throughput_vec_sorted[throughput_vec_sorted.len() * 99 / 100];
        println!("Throughput Sink:");
        println!("avg: {:?} rows/s ", avg);
        println!("p90: {:?} rows/s ", p90);
        println!("p95: {:?} rows/s ", p95);
        println!("p99: {:?} rows/s ", p99);
        let draw_vec: Vec<(f32, f32)> = throughput_vec
            .iter()
            .enumerate()
            .map(|(index, &value)| {
                (
                    (index as f32) * (THROUGHPUT_METRIC_RECORD_INTERVAL as f32 / 1000_f32),
                    value as f32,
                )
            })
            .collect();

        let root = SVGBackend::new("throughput.svg", (640, 480)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let root = root.margin(10, 10, 10, 10);
        let mut chart = ChartBuilder::on(&root)
            .caption("Throughput Sink", ("sans-serif", 40).into_font())
            .x_label_area_size(20)
            .y_label_area_size(40)
            .build_cartesian_2d(
                0.0..BENCH_TIME as f32,
                **throughput_vec_sorted.first().unwrap() as f32
                    ..**throughput_vec_sorted.last().unwrap() as f32,
            )
            .unwrap();

        chart
            .configure_mesh()
            .x_labels(5)
            .y_labels(5)
            .y_label_formatter(&|x| format!("{:.0}", x))
            .draw()
            .unwrap();

        chart
            .draw_series(LineSeries::new(draw_vec.clone(), &RED))
            .unwrap();
        chart
            .draw_series(PointSeries::of_element(draw_vec, 5, &RED, &|c, s, st| {
                EmptyElement::at(c) + Circle::new((0, 0), s, st.filled())
            }))
            .unwrap();
        root.present().unwrap();

        println!(
            "Throughput Sink: {:?}",
            throughput_vec
                .iter()
                .map(|a| format!("{} rows/s", a))
                .collect_vec()
        );
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
        let mut datagen_enumerator = DatagenSplitEnumerator::new(
            properties.clone(),
            SourceEnumeratorContext::dummy().into(),
        )
        .await
        .unwrap();
        let parser_config = ParserConfig {
            specific: SpecificParserConfig {
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
                    SourceContext::dummy().into(),
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
                yield Message::Chunk(item);
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
                        "Can't assert message type".to_owned(),
                    ));
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
    if let Ok(coordinator) = sink.new_coordinator(DatabaseConnection::Disconnected).await {
        sink_writer_param.meta_client = Some(SinkMetaClient::MockMetaClient(MockMetaClient::new(
            Box::new(coordinator),
        )));
        sink_writer_param.vnode_bitmap = Some(Bitmap::ones(1));
    }
    let log_sinker = sink.new_log_sinker(sink_writer_param).await.unwrap();
    match log_sinker.consume_log_and_sink(&mut log_reader).await {
        Ok(_) => Err("Stream closed".to_owned()),
        Err(e) => Err(e.to_report_string()),
    }
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

fn read_sink_option_from_yml(path: &str) -> HashMap<String, BTreeMap<String, String>> {
    let data = std::fs::read_to_string(path).unwrap();
    let sink_option: HashMap<String, BTreeMap<String, String>> =
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
    use risingwave_connector::sink::Sink as _;
    use risingwave_connector::sink::redis::RedisSink;
    if connector.eq(RedisSink::SINK_NAME) {
        let format = match r#type {
            SINK_TYPE_APPEND_ONLY => SinkFormat::AppendOnly,
            SINK_TYPE_UPSERT => SinkFormat::Upsert,
            _ => {
                return Err(SinkError::Config(anyhow!(
                    "sink type unsupported: {}",
                    r#type
                )));
            }
        };
        Ok(Some(SinkFormatDesc {
            format,
            encode: SinkEncode::Json,
            options: Default::default(),
            secret_refs: Default::default(),
            key_encode: None,
            connection_id: None,
        }))
    } else {
        SinkFormatDesc::from_legacy_type(connector, r#type)
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
    if cfg.sink.eq(&BENCH_TEST.to_owned()) {
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
            properties.get("type").unwrap_or(&"append-only".to_owned()),
        )
        .unwrap();
        let sink_param = SinkParam {
            sink_id: SinkId::new(1),
            sink_name: cfg.sink.clone(),
            properties,
            columns: table_schema.get_sink_schema(),
            downstream_pk: table_schema.pk_indices,
            sink_type: SinkType::AppendOnly,
            format_desc,
            db_name: "not_need_set".to_owned(),
            sink_from_name: "not_need_set".to_owned(),
        };
        let sink = build_sink(sink_param).unwrap();
        let sink_writer_param = SinkWriterParam::for_test();
        println!("Start Sink Bench!, Wait {:?}s", BENCH_TIME);
        tokio::spawn(async move {
            dispatch_sink!(sink, sink, {
                consume_log_stream(*sink, mock_range_log_reader, sink_writer_param).boxed()
            })
            .await
            .unwrap();
        });
    }
    sleep(tokio::time::Duration::from_secs(BENCH_TIME)).await;
    println!("Bench Over!");
    stop_tx.send(()).await.unwrap();
    data_size_rx.await.unwrap().print_throughput().await;
}
