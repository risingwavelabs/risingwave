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

use std::collections::HashMap;

use anyhow::Context;
use arrow_array_iceberg::{Int32Array, Int64Array, RecordBatch};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use icelake::catalog::{load_catalog, CATALOG_NAME, CATALOG_TYPE};
use icelake::io::FileScanStream;
use icelake::types::{Any, AnyValue, StructValueBuilder};
use icelake::{Table, TableIdentifier};
use itertools::Itertools;
use pulsar::consumer::InitialPosition;
use pulsar::message::proto::MessageIdData;
use pulsar::{Consumer, ConsumerBuilder, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::ROWID_PREFIX;
use risingwave_common::{bail, ensure};
use thiserror_ext::AsReport;

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::source::{
    into_chunk_stream, BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SplitId,
    SplitMetaData, SplitReader,
};

const PULSAR_DEFAULT_SUBSCRIPTION_PREFIX: &str = "rw-consumer";

pub enum PulsarSplitReader {
    Broker(PulsarBrokerReader),
    Iceberg(PulsarIcebergReader),
}

#[async_trait]
impl SplitReader for PulsarSplitReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        if props.iceberg_loader_enabled.unwrap_or(false)
            && matches!(split.start_offset, PulsarEnumeratorOffset::Earliest)
            && !topic.starts_with("non-persistent://")
        {
            tracing::debug!("Creating iceberg reader for pulsar split topic {}", topic);
            Ok(Self::Iceberg(PulsarIcebergReader::new(
                props,
                split,
                source_ctx,
                parser_config,
            )))
        } else {
            Ok(Self::Broker(
                PulsarBrokerReader::new(props, vec![split], parser_config, source_ctx, None)
                    .await?,
            ))
        }
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        match self {
            Self::Broker(reader) => {
                let (parser_config, source_context) =
                    (reader.parser_config.clone(), reader.source_ctx.clone());
                Box::pin(into_chunk_stream(
                    reader.into_data_stream(),
                    parser_config,
                    source_context,
                ))
            }
            Self::Iceberg(reader) => Box::pin(reader.into_stream()),
        }
    }
}

/// This reader reads from pulsar broker
pub struct PulsarBrokerReader {
    #[expect(dead_code)]
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    #[expect(dead_code)]
    split: PulsarSplit,
    #[expect(dead_code)]
    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

// {ledger_id}:{entry_id}:{partition}:{batch_index}
fn parse_message_id(id: &str) -> ConnectorResult<MessageIdData> {
    let splits = id.split(':').collect_vec();

    if splits.len() < 2 || splits.len() > 4 {
        bail!("illegal message id string {}", id);
    }

    let ledger_id = splits[0].parse::<u64>().context("illegal ledger id")?;
    let entry_id = splits[1].parse::<u64>().context("illegal entry id")?;

    let mut message_id = MessageIdData {
        ledger_id,
        entry_id,
        partition: None,
        batch_index: None,
        ack_set: vec![],
        batch_size: None,
        first_chunk_message_id: None,
    };

    if splits.len() > 2 {
        let partition = splits[2].parse::<i32>().context("illegal partition")?;
        message_id.partition = Some(partition);
    }

    if splits.len() == 4 {
        let batch_index = splits[3].parse::<i32>().context("illegal batch index")?;
        message_id.batch_index = Some(batch_index);
    }

    Ok(message_id)
}

#[async_trait]
impl SplitReader for PulsarBrokerReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let pulsar = props
            .common
            .build_client(&props.oauth, &props.aws_auth_props)
            .await?;
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        let builder: ConsumerBuilder<TokioExecutor> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(format!(
                "{}-{}-{}",
                props
                    .subscription_name_prefix
                    .unwrap_or(PULSAR_DEFAULT_SUBSCRIPTION_PREFIX.to_string()),
                source_ctx.fragment_id,
                source_ctx.actor_id
            ));

        let builder = match split.start_offset.clone() {
            PulsarEnumeratorOffset::Earliest => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!("Earliest offset is not supported for non-persistent topic, use Latest instead");
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
                    )
                } else {
                    builder.with_options(
                        ConsumerOptions::default()
                            .with_initial_position(InitialPosition::Earliest)
                            .durable(false),
                    )
                }
            }
            PulsarEnumeratorOffset::Latest => builder.with_options(
                ConsumerOptions::default()
                    .with_initial_position(InitialPosition::Latest)
                    .durable(false),
            ),
            PulsarEnumeratorOffset::MessageId(m) => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!("MessageId offset is not supported for non-persistent topic, use Latest instead");
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
                    )
                } else {
                    builder.with_options(pulsar::ConsumerOptions {
                        durable: Some(false),
                        start_message_id: parse_message_id(m.as_str()).ok(),
                        ..Default::default()
                    })
                }
            }

            PulsarEnumeratorOffset::Timestamp(_) => builder,
        };

        let consumer: Consumer<Vec<u8>, _> = builder.build().await?;
        if let PulsarEnumeratorOffset::Timestamp(_ts) = split.start_offset {
            // FIXME: Here we need pulsar-rs to support the send + sync consumer
            // consumer
            //     .seek(None, None, Some(ts as u64), pulsar.clone())
            //     .await?;
        }

        Ok(Self {
            pulsar,
            consumer,
            split_id: split.id(),
            split,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl PulsarBrokerReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        #[for_await]
        for msgs in self.consumer.ready_chunks(max_chunk_size) {
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                let msg = SourceMessage::from(msg?);
                res.push(msg);
            }
            yield res;
        }
    }
}

#[expect(dead_code)]
const META_COLUMN_TOPIC: &str = "__topic";
#[expect(dead_code)]
const META_COLUMN_KEY: &str = "__key";
const META_COLUMN_LEDGER_ID: &str = "__ledgerId";
const META_COLUMN_ENTRY_ID: &str = "__entryId";
const META_COLUMN_BATCH_INDEX: &str = "__batchIndex";
const META_COLUMN_PARTITION: &str = "__partition";

/// Read history data from iceberg table
pub struct PulsarIcebergReader {
    props: PulsarProperties,
    split: PulsarSplit,
    source_ctx: SourceContextRef,
    parser_config: ParserConfig,
}

impl PulsarIcebergReader {
    fn new(
        props: PulsarProperties,
        split: PulsarSplit,
        source_ctx: SourceContextRef,
        parser_config: ParserConfig,
    ) -> Self {
        Self {
            props,
            split,
            source_ctx,
            parser_config,
        }
    }

    async fn scan(&self) -> ConnectorResult<FileScanStream> {
        let table = self.create_iceberg_table().await?;
        let schema = table.current_table_metadata().current_schema()?;
        tracing::debug!("Created iceberg pulsar table, schema is: {:?}", schema,);

        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;

        let partition_value = match &self.split.topic.partition_index {
            Some(partition_id) => {
                let (partition_type, partition_field) = match table.current_partition_type()? {
                    Any::Struct(s) => {
                        let field = s
                            .fields()
                            .iter()
                            .find(|f| f.name == META_COLUMN_PARTITION)
                            .context("Partition field not found in partition spec")?;
                        (s.clone(), field.clone())
                    }
                    _ => {
                        bail!(
                            "Partition type is not struct in iceberg table: {}",
                            table.table_name()
                        );
                    }
                };

                let mut partition_value_builder = StructValueBuilder::new(partition_type);
                partition_value_builder.add_field(
                    partition_field.id,
                    Some(AnyValue::Primitive(icelake::types::PrimitiveValue::Int(
                        *partition_id,
                    ))),
                )?;
                Some(partition_value_builder.build()?)
            }
            None => None,
        };

        // TODO: Add partition
        Ok(table
            .new_scan_builder()
            .with_partition_value(partition_value)
            .with_batch_size(max_chunk_size)
            .build()
            .context("failed to build iceberg table scan")?
            .scan(&table)
            .await?)
    }

    async fn create_iceberg_table(&self) -> ConnectorResult<Table> {
        let catalog = load_catalog(&self.build_iceberg_configs()?)
            .await
            .context("Unable to load iceberg catalog")?;

        let table_id =
            TableIdentifier::new(vec![self.split.topic.topic_str_without_partition()?])
                .context("Unable to parse table name")?;

        let table = catalog.load_table(&table_id).await?;

        Ok(table)
    }

    #[try_stream(ok = (StreamChunk, HashMap<SplitId, String>), error = crate::error::ConnectorError)]
    async fn as_stream_chunk_stream(&self) {
        #[for_await]
        for file_scan in self.scan().await? {
            let file_scan = file_scan?;

            #[for_await]
            for record_batch in file_scan.scan().await? {
                let batch = record_batch?;
                let msgs = self.convert_record_batch_to_source_with_state(&batch)?;
                yield msgs;
            }
        }
    }

    #[try_stream(ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream(self) {
        let (props, mut split, parser_config, source_ctx) = (
            self.props.clone(),
            self.split.clone(),
            self.parser_config.clone(),
            self.source_ctx.clone(),
        );
        tracing::info!("Starting to read pulsar message from iceberg");
        let mut last_msg_id = None;

        #[for_await]
        for msg in self.as_stream_chunk_stream() {
            let (_chunk, mapping) = msg.inspect_err(
                |e| tracing::error!(error = %e.as_report(), "Failed to read message from iceberg"),
            )?;
            last_msg_id = mapping.get(self.split.topic.to_string().as_str()).cloned();
        }

        tracing::info!("Finished reading pulsar message from iceberg");
        // We finished reading all the data from iceberg table, now we need to start from broker.
        if let Some(msg_id) = last_msg_id {
            tracing::info!("Last iceberg message id is {}", msg_id);
            split.start_offset = PulsarEnumeratorOffset::MessageId(msg_id);
        }

        tracing::info!(
            "Switching from pulsar iceberg reader to broker reader with offset: {:?}",
            split.start_offset
        );
        let broker_reader = PulsarSplitReader::Broker(
            PulsarBrokerReader::new(props, vec![split], parser_config, source_ctx, None).await?,
        );

        #[for_await]
        for msg in broker_reader.into_stream() {
            yield msg?;
        }
    }

    fn build_iceberg_configs(&self) -> ConnectorResult<HashMap<String, String>> {
        let mut iceberg_configs = HashMap::new();

        let bucket = self
            .props
            .iceberg_bucket
            .as_ref()
            .context("Iceberg bucket is not configured")?;

        iceberg_configs.insert(CATALOG_TYPE.to_string(), "storage".to_string());
        iceberg_configs.insert(CATALOG_NAME.to_string(), "pulsar".to_string());
        iceberg_configs.insert(
            "iceberg.catalog.pulsar.warehouse".to_string(),
            format!(
                "s3://{}/{}/{}",
                bucket, self.split.topic.tenant, self.split.topic.namespace,
            ),
        );

        if let Some(region) = &self.props.aws_auth_props.region {
            iceberg_configs.insert("iceberg.table.io.region".to_string(), region.to_string());
        }
        if let Some(access_key) = &self.props.aws_auth_props.access_key {
            iceberg_configs.insert(
                "iceberg.table.io.access_key_id".to_string(),
                access_key.to_string(),
            );
        }
        if let Some(secret_key) = &self.props.aws_auth_props.secret_key {
            iceberg_configs.insert(
                "iceberg.table.io.secret_access_key".to_string(),
                secret_key.to_string(),
            );
        }

        iceberg_configs.insert("iceberg.table.io.bucket".to_string(), bucket.to_string());
        iceberg_configs.insert(
            "iceberg.table.io.root".to_string(),
            format!(
                "/{}/{}",
                self.split.topic.tenant, self.split.topic.namespace
            ),
        );
        // #TODO
        // Support load config file
        iceberg_configs.insert(
            "iceberg.table.io.disable_config_load".to_string(),
            "true".to_string(),
        );

        Ok(iceberg_configs)
    }

    // Converts arrow record batch to stream chunk.
    fn convert_record_batch_to_source_with_state(
        &self,
        record_batch: &RecordBatch,
    ) -> ConnectorResult<(StreamChunk, HashMap<SplitId, String>)> {
        let mut offsets = Vec::with_capacity(record_batch.num_rows());

        let ledger_id_array = record_batch
            .column_by_name(META_COLUMN_LEDGER_ID)
            .context("Ledger id not found in iceberg table")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Ledger id is not i64 in iceberg table")?;

        let entry_id_array = record_batch
            .column_by_name(META_COLUMN_ENTRY_ID)
            .context("Entry id not found in iceberg table")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("Entry id is not i64 in iceberg table")?;

        let partition_array = record_batch
            .column_by_name(META_COLUMN_PARTITION)
            .map(|arr| {
                arr.as_any()
                    .downcast_ref::<Int32Array>()
                    .context("Partition is not i32 in iceberg table")
            })
            .transpose()?;

        let batch_index_array = record_batch
            .column_by_name(META_COLUMN_BATCH_INDEX)
            .map(|arr| {
                arr.as_any()
                    .downcast_ref::<Int64Array>()
                    .context("Batch index is not i64 in iceberg table")
            })
            .transpose()?;

        let field_indices: Vec<_> = self
            .parser_config
            .common
            .rw_columns
            .iter()
            .filter(|col| col.name != ROWID_PREFIX)
            .map(|col| record_batch.schema().index_of(col.name.as_str()))
            .try_collect()
            .context("failed to look up column name in arrow record batch")?;

        for row in 0..record_batch.num_rows() {
            let offset = format!(
                "{}:{}:{}:{}",
                ledger_id_array.value(row),
                entry_id_array.value(row),
                partition_array.map(|arr| arr.value(row)).unwrap_or(-1),
                batch_index_array.map(|arr| arr.value(row)).unwrap_or(-1)
            );

            offsets.push(offset);
        }

        let data_chunk = IcebergArrowConvert
            .chunk_from_record_batch(&record_batch.project(&field_indices)?)
            .context("failed to convert arrow record batch to data chunk")?;

        let stream_chunk = StreamChunk::from(data_chunk);

        let state = HashMap::from([(
            self.split.topic.to_string().into(),
            offsets.last().unwrap().clone(),
        )]);

        Ok((stream_chunk, state))
    }
}
