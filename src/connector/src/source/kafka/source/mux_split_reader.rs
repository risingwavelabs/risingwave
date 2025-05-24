// use std::collections::HashMap;
// use std::sync::Arc;
// use std::time::Duration;
//
// use async_trait::async_trait;
// use futures::StreamExt;
// use futures_async_stream::try_stream;
// use rdkafka::error::KafkaError;
// use rdkafka::{Offset, TopicPartitionList};
// use tokio::sync::mpsc;
//
// use crate::error::ConnectorResult as Result;
// use crate::parser::ParserConfig;
// use crate::source::base::SourceMessage;
// use crate::source::kafka::source::mux_reader::{ConsumerRecord, KafkaConfig, KafkaMuxReader};
// use crate::source::kafka::{KafkaMeta, KafkaProperties, KafkaSplit};
// use crate::source::{
//     BackfillInfo, BoxSourceChunkStream, Column, SourceContextRef, SplitId, SplitImpl,
//     SplitMetaData, SplitReader, into_chunk_stream,
// };
//
// /// 基于 `KafkaMuxReader` 的 `SplitReader` 实现
// pub struct KafkaMuxSplitReader {
//     mux_reader: Arc<KafkaMuxReader>,
//     receivers: Vec<mpsc::Receiver<ConsumerRecord>>,
//     offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
//     backfill_info: HashMap<SplitId, BackfillInfo>,
//     splits: Vec<KafkaSplit>,
//     sync_call_timeout: Duration,
//     bytes_per_second: usize,
//     max_num_messages: usize,
//     parser_config: ParserConfig,
//     source_ctx: SourceContextRef,
// }
//
// #[async_trait]
// impl SplitReader for KafkaMuxSplitReader {
//     type Properties = KafkaProperties;
//     type Split = KafkaSplit;
//
//     async fn new(
//         properties: KafkaProperties,
//         splits: Vec<KafkaSplit>,
//         parser_config: ParserConfig,
//         source_ctx: SourceContextRef,
//         _columns: Option<Vec<Column>>, // 暂未使用
//     ) -> Result<Self> {
//         let reader_key = source_ctx.fragment_id.to_string();
//
//         let kafka_cfg = KafkaConfig {
//             brokers: properties.connection.brokers.clone(),
//             group_id: properties.group_id(source_ctx.fragment_id),
//         };
//
//         let mux_reader = KafkaMuxReader::get_or_create(reader_key, kafka_cfg).await;
//
//         let mut offsets = HashMap::new();
//         let mut backfill_info = HashMap::new();
//         let mut receivers = Vec::with_capacity(splits.len());
//
//         for split in splits.clone() {
//             let (low, high) = mux_reader
//                 .fetch_watermarks(
//                     &split.topic,
//                     split.partition,
//                     properties.common.sync_call_timeout,
//                 )
//                 .await?;
//
//             offsets.insert(split.id(), (split.start_offset, split.stop_offset));
//             if low == high || split.start_offset.map(|o| o + 1 >= high).unwrap_or(false) {
//                 backfill_info.insert(split.id(), BackfillInfo::NoDataToBackfill);
//             } else {
//                 backfill_info.insert(
//                     split.id(),
//                     BackfillInfo::HasDataToBackfill {
//                         latest_offset: (high - 1).to_string(),
//                     },
//                 );
//             }
//
//             let rx = mux_reader
//                 .register_split(split.topic.clone(), split.partition)
//                 .await;
//             receivers.push(rx);
//         }
//
//         let bytes_per_second = properties
//             .bytes_per_second
//             .as_deref()
//             .map(|s| s.parse().expect("bytes.per.second expect usize"))
//             .unwrap_or(usize::MAX);
//         let max_num_messages = properties
//             .max_num_messages
//             .as_deref()
//             .map(|s| s.parse().expect("max.num.messages expect usize"))
//             .unwrap_or(usize::MAX);
//
//         Ok(Self {
//             mux_reader,
//             receivers,
//             offsets,
//             backfill_info,
//             splits,
//             sync_call_timeout: properties.common.sync_call_timeout,
//             bytes_per_second,
//             max_num_messages,
//             parser_config,
//             source_ctx,
//         })
//     }
//
//     fn into_stream(self) -> BoxSourceChunkStream {
//         // 合并所有 split 的 Receiver
//         let merged = futures::stream::select_all(self.receivers.into_iter());
//         let config = self.parser_config.clone();
//         let source_ctx = self.source_ctx.clone();
//         into_chunk_stream(self.read_from_mux(merged), config, source_ctx)
//     }
//
//     fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
//         self.backfill_info.clone()
//     }
//
//     async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
//         // 对于 MuxReader，我们无法直接全量 assign；
//         // 这里简单返回最新 offset 信息，由上层决定是否重建 reader。
//         let mut latest_splits = Vec::new();
//         for mut split in self.splits.clone() {
//             let (_low, high) = self
//                 .mux_reader
//                 .fetch_watermarks(&split.topic, split.partition, self.sync_call_timeout)
//                 .await?;
//             split.start_offset = Some(high - 1);
//             latest_splits.push(split.into());
//         }
//         Ok(latest_splits)
//     }
// }
//
// impl KafkaMuxSplitReader {
//     #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
//     async fn read_from_mux<St>(mut self, mut merged_rx: St)
//     where
//         St: futures::Stream<Item = ConsumerRecord> + Unpin + Send + 'static,
//     {
//         // TODO: 把原 KafkaSplitReader::into_data_stream 逻辑迁移到这里，
//         //       使用 self.bytes_per_second / self.max_num_messages / self.offsets 控制输出。
//         while let Some(rec) = merged_rx.next().await {
//             // 占位：直接构造 SourceMessage 并 yield
//             let msg = SourceMessage {
//                 split_id: rec.partition.to_string(),
//                 offset: rec.offset.to_string(),
//                 key: rec.key,
//                 payload: rec.payload,
//             };
//             yield vec![msg];
//         }
//     }
// }
