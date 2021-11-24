use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, StreamConsumer};
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::metadata::Metadata;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

use crate::source::{
    BatchSourceReader, SourceColumnDesc, SourceImpl, SourceParser, SourceWriter, StreamSourceReader,
};
use crate::stream_op::{Op, StreamChunk};

/// `KAFKA_SYNC_CALL_TIMEOUT` provides a timeout parameter for `rdkafka` calls, note that currently
/// we only use `committed_offsets` and `fetch_metadata` for synchronization calls, these two calls
/// are only called when initializing and ad hoc queries and no messages are coming in, so it is
/// fine to use a long timeout for now, this parameter should be configurable or removed in the
/// future
const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// `HighLevelKafkaSource` is an external source that uses Kafka's own consumer group mechanism to
/// allocate partitions without the need for a centralized partition manager, compared to
/// `LowLevelKafkaSource`
#[derive(Clone, Debug)]
pub struct HighLevelKafkaSource {
    pub config: HighLevelKafkaSourceConfig,
    pub column_descs: Arc<Vec<SourceColumnDesc>>,
    pub parser: Arc<dyn SourceParser>,
}

/// `HighLevelKafkaSourceConfig` is the configuration for `HighLevelKafkaSource`, providing the
/// necessary Kafka Server addresses and topics, as well as customizable properties, note that for
/// now we will ignore "bootstrap.servers" and "group.id" configuration in `properties`
#[derive(Clone, Debug)]
pub struct HighLevelKafkaSourceConfig {
    pub bootstrap_servers: Vec<String>,
    pub topic: String,
    pub properties: HashMap<String, String>,
}

/// `HighLevelKafkaSourceReaderContext` is used to provide additional information to
/// `HighLevelKafkaSource` when generating the Reader, such as `query_id` for consumer group
/// generation and `bound_timestamp_ms` for synchronizing end bounds.
pub struct HighLevelKafkaSourceReaderContext {
    pub query_id: Option<String>,
    pub bound_timestamp_ms: Option<i64>,
}

/// `HighLevelKafkaSourceWriter` currently does not have any implementation
pub struct HighLevelKafkaSourceWriter {}

/// `HighLevelKafkaSourceStreamReader` is used to generate `StreamChunk` messages, when there are
/// messages will be stacked up to `DEFAULT_CHUNK_BUFFER_SIZE` messages and generate `StreamChunk`,
/// when there is no message will stop here
pub struct HighLevelKafkaSourceStreamReader {
    consumer: StreamConsumer<DefaultConsumerContext>,
    parser: Arc<dyn SourceParser>,
    columns: Arc<Vec<SourceColumnDesc>>,
}

/// `HighLevelKafkaSourceStreamReader` is used to generate `DataChunk` messages, when there are
/// messages, it will stack up to `DEFAULT_CHUNK_BUFFER_SIZE` messages and generate `DataChunk`,
/// when there are no messages coming in, it will regularly get the metadata to check the overall
/// consumption status, when it is sure that the consumer group has consumed the predefined bounds,
/// it will return None to end the generation.
pub struct HighLevelKafkaSourceBatchReader {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    parser: Arc<dyn SourceParser>,
    columns: Arc<Vec<SourceColumnDesc>>,
    bounds: HashMap<i32, (i64, i64)>,
    topic: String,
    metadata: Arc<Metadata>,
}

impl HighLevelKafkaSource {
    pub fn new(
        config: HighLevelKafkaSourceConfig,
        column_descs: Arc<Vec<SourceColumnDesc>>,
        parser: Arc<dyn SourceParser>,
    ) -> Self {
        HighLevelKafkaSource {
            config,
            column_descs,
            parser,
        }
    }

    fn fetch_bounds(
        consumer: &StreamConsumer<DefaultConsumerContext>,
        topic: &str,
        timestamp: Option<i64>,
        timeout: Duration,
    ) -> Result<HashMap<i32, (i64, i64)>> {
        // First we get the current watermark of this topic through metadata,
        // define a low bound and high bound
        let mut watermarks = HashMap::new();

        let metadata = consumer
            .fetch_metadata(Some(topic), timeout)
            .map_err(|e| RwError::from(InternalError(e.to_string())))?;

        for meta_topic in metadata.topics() {
            let name = meta_topic.name();

            for part in meta_topic.partitions() {
                let (low, high) = consumer
                    .fetch_watermarks(name, part.id(), timeout)
                    .map_err(|e| RwError::from(InternalError(e.to_string())))?;
                watermarks.insert(part.id(), (low, high));
            }
        }

        let timestamp = if let Some(timestamp) = timestamp {
            timestamp
        } else {
            return Ok(watermarks);
        };

        // After that we use the provided timestamp to generate a specific upper bound,
        // because the timestamp may occur after the last message,
        // for which Kafka returns Offset::End,
        // so it needs to be complemented with the previous watermark
        let mut partition_timestamps = TopicPartitionList::with_capacity(
            metadata.topics().iter().map(|t| t.partitions().len()).sum(),
        );

        for topic in metadata.topics().iter() {
            for partition in topic.partitions() {
                // we never need this result
                let _ = partition_timestamps.add_partition_offset(
                    topic.name(),
                    partition.id(),
                    Offset::Offset(timestamp),
                );
            }
        }

        let offset_for_times = consumer
            .offsets_for_times(partition_timestamps, timeout)
            .map_err(|e| RwError::from(InternalError(e.to_string())))?;

        let mut bounds = HashMap::with_capacity(watermarks.len());

        for elem in offset_for_times.elements_for_topic(topic) {
            if let Some((low, high)) = watermarks.remove(&elem.partition()) {
                let target = match elem.offset() {
                    Offset::Beginning => low,
                    Offset::End => high,
                    Offset::Offset(offset) => offset + 1,
                    Offset::OffsetTail(offset_tail) => high - offset_tail,
                    _ => high,
                };

                if low != high {
                    bounds.insert(elem.partition(), (low, target));
                }
            }
        }

        Ok(bounds)
    }

    fn create_consumer(
        &self,
        context: &HighLevelKafkaSourceReaderContext,
    ) -> Result<StreamConsumer> {
        let mut config = ClientConfig::new();

        config.set(
            "group.id",
            match context.query_id.as_ref() {
                Some(id) => format!("consumer-query-{}", id),
                None => format!(
                    "consumer-{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                ),
            },
        );

        config.set("bootstrap.servers", self.config.bootstrap_servers.join(","));

        config.set("topic.metadata.refresh.interval.ms", "30000");
        config.set("fetch.message.max.bytes", "134217728");

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("auto.offset.reset", "earliest");
        config.set("enable.auto.commit", "false");

        config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(DefaultConsumerContext)
            .map_err(|e| RwError::from(InternalError(format!("consumer creation failed {}", e))))
    }
}

#[async_trait]
impl SourceWriter for HighLevelKafkaSourceWriter {
    async fn write(&mut self, _chunk: &StreamChunk) -> Result<()> {
        todo!()
    }

    async fn flush(&mut self, _chunk: &StreamChunk) -> Result<()> {
        todo!()
    }
}

impl SourceImpl for HighLevelKafkaSource {
    type ReaderContext = HighLevelKafkaSourceReaderContext;
    type BatchReader = HighLevelKafkaSourceBatchReader;
    type StreamReader = HighLevelKafkaSourceStreamReader;
    type Writer = HighLevelKafkaSourceWriter;

    fn batch_reader(
        &self,
        context: HighLevelKafkaSourceReaderContext,
    ) -> Result<Self::BatchReader> {
        let consumer = self.create_consumer(&context)?;

        let topics: &[&str] = &[&self.config.topic];

        consumer.subscribe(&topics.to_vec()).map_err(|e| {
            RwError::from(InternalError(format!(
                "subscribe to topic {} failed {}",
                self.config.topic, e
            )))
        })?;

        let bounds = Self::fetch_bounds(
            &consumer,
            self.config.topic.as_str(),
            context.bound_timestamp_ms,
            Duration::from_millis(1000),
        )?;

        let metadata = consumer
            .fetch_metadata(Some(self.config.topic.as_str()), KAFKA_SYNC_CALL_TIMEOUT)
            .map_err(|e| RwError::from(InternalError(e.to_string())))?;

        Ok(HighLevelKafkaSourceBatchReader {
            consumer: Arc::new(consumer),
            parser: self.parser.clone(),
            columns: self.column_descs.clone(),
            bounds,
            topic: self.config.topic.to_string(),
            metadata: Arc::new(metadata),
        })
    }

    fn stream_reader(
        &self,
        context: HighLevelKafkaSourceReaderContext,
    ) -> Result<Self::StreamReader> {
        let consumer = self.create_consumer(&context)?;

        let topics: &[&str] = &[&self.config.topic];

        consumer.subscribe(&topics.to_vec()).map_err(|e| {
            RwError::from(InternalError(format!(
                "subscribe to topic {} failed {}",
                self.config.topic, e
            )))
        })?;

        Ok(HighLevelKafkaSourceStreamReader {
            consumer,
            parser: self.parser.clone(),
            columns: self.column_descs.clone(),
        })
    }

    fn create_writer(&self) -> Result<Self::Writer> {
        todo!()
    }
}

trait KafkaSourceChunkBuilder {
    fn build_columns(
        column_descs: &[SourceColumnDesc],
        rows: &[Vec<Datum>],
    ) -> Result<Vec<Column>> {
        let mut builders = column_descs
            .iter()
            .map(|k| k.data_type.create_array_builder(DEFAULT_CHUNK_BUFFER_SIZE))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        for row in rows {
            row.iter()
                .zip(&mut builders)
                .try_for_each(|(datum, builder)| builder.append_datum(datum))?
        }

        builders
            .into_iter()
            .zip(column_descs.iter().map(|c| c.data_type.clone()))
            .map(|(builder, data_type)| {
                builder
                    .finish()
                    .map(|arr| Column::new(Arc::new(arr), data_type.clone()))
            })
            .collect::<Result<Vec<Column>>>()
    }

    fn build_datachunk(column_desc: &[SourceColumnDesc], rows: &[Vec<Datum>]) -> Result<DataChunk> {
        let columns = Self::build_columns(column_desc, rows)?;
        Ok(DataChunk::builder().columns(columns).build())
    }
}

impl KafkaSourceChunkBuilder for HighLevelKafkaSourceStreamReader {}

impl KafkaSourceChunkBuilder for HighLevelKafkaSourceBatchReader {}

#[async_trait]
impl StreamSourceReader for HighLevelKafkaSourceStreamReader {
    async fn open(&mut self) -> Result<()> {
        todo!()
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        match self
            .consumer
            .stream()
            .ready_chunks(DEFAULT_CHUNK_BUFFER_SIZE)
            .next()
            .await
        {
            None => Ok(StreamChunk::default()),
            Some(batch) => {
                let mut rows = Vec::with_capacity(batch.len());

                for msg in batch {
                    let msg = msg.map_err(|e| RwError::from(InternalError(e.to_string())))?;
                    if let Some(payload) = msg.payload() {
                        rows.push(self.parser.parse(payload, &self.columns)?);
                    }
                }

                let columns = Self::build_columns(&self.columns, &rows)?;

                Ok(StreamChunk::new(
                    vec![Op::Insert; rows.len()],
                    columns,
                    None,
                ))
            }
        }
    }
}

impl HighLevelKafkaSourceBatchReader {
    async fn check_bounds(
        consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
        topic: &str,
        metadata: Arc<Metadata>,
        bounds: &HashMap<i32, (i64, i64)>,
        timeout: Duration,
    ) -> Result<bool> {
        let mut tpl = TopicPartitionList::new();

        for topic_meta in metadata.topics().iter() {
            if topic_meta.name() == topic {
                for partition in topic_meta.partitions() {
                    tpl.add_partition(topic_meta.name(), partition.id());
                }
                break;
            }
        }

        let offsets = consumer
            .committed_offsets(tpl, timeout)
            .map_err(|e| RwError::from(InternalError(e.to_string())))?;

        for elem in offsets.elements_for_topic(topic) {
            if let Some(bound) = bounds.get(&elem.partition()) {
                if let Offset::Offset(offset) = elem.offset() {
                    if offset < bound.1 {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

#[async_trait]
impl BatchSourceReader for HighLevelKafkaSourceBatchReader {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let loop_timeout = Duration::from_millis(100);

        loop {
            if self.bounds.is_empty() {
                return Ok(None);
            }

            match tokio::time::timeout(
                loop_timeout,
                self.consumer
                    .stream()
                    .ready_chunks(DEFAULT_CHUNK_BUFFER_SIZE)
                    .next(),
            )
            .await
            {
                Ok(Some(batch)) => {
                    let mut rows = Vec::with_capacity(batch.len());

                    for msg in batch {
                        let msg = msg.map_err(|e| RwError::from(InternalError(e.to_string())))?;
                        let partition = msg.partition();
                        let offset = msg.offset();

                        match self.bounds.get(&partition) {
                            // new created partition, skip
                            None => continue,
                            Some((_, high)) => {
                                // Skip out-of-bounds message
                                if offset + 1 > *(high) {
                                    continue;
                                }
                            }
                        }

                        if let Some(payload) = msg.payload() {
                            rows.push(self.parser.parse(payload, &self.columns)?);
                        }

                        self.consumer
                            .store_offset_from_message(&msg)
                            .map_err(|e| RwError::from(InternalError(e.to_string())))?;
                    }

                    self.consumer
                        .commit_consumer_state(CommitMode::Sync)
                        .map_err(|e| RwError::from(InternalError(e.to_string())))?;

                    if rows.is_empty()
                        && Self::check_bounds(
                            self.consumer.clone(),
                            self.topic.as_str(),
                            self.metadata.clone(),
                            &self.bounds,
                            KAFKA_SYNC_CALL_TIMEOUT,
                        )
                        .await?
                    {
                        self.bounds.clear()
                    }

                    let columns = Self::build_columns(&self.columns, &rows)?;

                    return Ok(Some(DataChunk::new(columns, None)));
                }
                _ => {
                    if Self::check_bounds(
                        self.consumer.clone(),
                        self.topic.as_str(),
                        self.metadata.clone(),
                        &self.bounds,
                        KAFKA_SYNC_CALL_TIMEOUT,
                    )
                    .await?
                    {
                        return Ok(None);
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
