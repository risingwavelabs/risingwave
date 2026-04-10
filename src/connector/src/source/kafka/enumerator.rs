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

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use moka::future::Cache as MokaCache;
use moka::ops::compute::Op;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use regex::Regex;
use risingwave_common::bail;
use risingwave_common::id::FragmentId;
use risingwave_common::metrics::LabelGuardedIntGauge;

use crate::connector_common::read_kafka_log_level;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::SourceEnumeratorContextRef;
use crate::source::base::{SplitEnumerator, SplitMetaData};
use crate::source::kafka::split::KafkaSplit;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaConnectionProps, KafkaContextCommon, KafkaProperties,
    RwConsumerContext,
};

type KafkaConsumer = BaseConsumer<RwConsumerContext>;
type KafkaAdmin = AdminClient<RwConsumerContext>;

/// Consumer client is shared, and the cache doesn't manage the lifecycle, so we store `Weak` and no eviction.
pub static SHARED_KAFKA_CONSUMER: LazyLock<MokaCache<KafkaConnectionProps, Weak<KafkaConsumer>>> =
    LazyLock::new(|| moka::future::Cache::builder().build());
/// Admin client is short-lived, so we store `Arc` and sets a time-to-idle eviction policy.
pub static SHARED_KAFKA_ADMIN: LazyLock<MokaCache<KafkaConnectionProps, Arc<KafkaAdmin>>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .time_to_idle(Duration::from_secs(5 * 60))
            .build()
    });

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum KafkaEnumeratorOffset {
    Earliest,
    Latest,
    Timestamp(i64),
    None,
}

pub struct KafkaSplitEnumerator {
    context: SourceEnumeratorContextRef,
    broker_address: String,
    topic: Option<String>,
    topic_regex: Option<Regex>,
    client: Arc<KafkaConsumer>,
    start_offset: KafkaEnumeratorOffset,

    // maybe used in the future for batch processing
    stop_offset: KafkaEnumeratorOffset,

    sync_call_timeout: Duration,
    high_watermark_metrics: HashMap<String, LabelGuardedIntGauge>,

    properties: KafkaProperties,
    config: rdkafka::ClientConfig,
}

impl KafkaSplitEnumerator {
    async fn drop_consumer_groups(&self, fragment_ids: Vec<FragmentId>) -> ConnectorResult<()> {
        let admin = Box::pin(SHARED_KAFKA_ADMIN.try_get_with_by_ref(
            &self.properties.connection,
            async {
                tracing::info!("build new kafka admin for {}", self.broker_address);
                Ok(Arc::new(
                    build_kafka_admin(&self.config, &self.properties).await?,
                ))
            },
        ))
        .await?;

        let group_ids = fragment_ids
            .iter()
            .map(|fragment_id| self.properties.group_id(*fragment_id))
            .collect::<Vec<_>>();
        let group_ids: Vec<&str> = group_ids.iter().map(|s| s.as_str()).collect();
        let res = admin
            .delete_groups(&group_ids, &AdminOptions::default())
            .await?;
        tracing::debug!(
            topic = ?self.topic,
            topic_regex = self.topic_regex.as_ref().map(Regex::as_str),
            ?fragment_ids,
            "delete groups result: {res:?}"
        );
        Ok(())
    }
}

#[async_trait]
impl SplitEnumerator for KafkaSplitEnumerator {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(
        properties: KafkaProperties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<KafkaSplitEnumerator> {
        let mut config = rdkafka::ClientConfig::new();
        let common_props = &properties.common;
        properties.validate_topic_selector()?;

        let broker_address = properties.connection.brokers.clone();
        let topic = common_props.topic.clone();
        let topic_regex = common_props
            .topic_regex
            .as_deref()
            .map(Regex::new)
            .transpose()
            .with_context(|| "invalid kafka topic regex")?;
        config.set("bootstrap.servers", &broker_address);
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        if let Some(log_level) = read_kafka_log_level() {
            config.set_log_level(log_level);
        }
        properties.connection.set_security_properties(&mut config);
        properties.set_client(&mut config);
        let mut scan_start_offset = match properties
            .scan_startup_mode
            .as_ref()
            .map(|s| s.to_lowercase())
            .as_deref()
        {
            Some("earliest") => KafkaEnumeratorOffset::Earliest,
            Some("latest") => KafkaEnumeratorOffset::Latest,
            None => KafkaEnumeratorOffset::Earliest,
            _ => bail!(
                "properties `scan_startup_mode` only supports earliest and latest or leaving it empty"
            ),
        };

        if let Some(s) = &properties.time_offset {
            let time_offset = s.parse::<i64>().map_err(|e| anyhow!(e))?;
            scan_start_offset = KafkaEnumeratorOffset::Timestamp(time_offset)
        }

        let mut client: Option<Arc<KafkaConsumer>> = None;
        SHARED_KAFKA_CONSUMER
            .entry_by_ref(&properties.connection)
            .and_try_compute_with::<_, _, ConnectorError>(|maybe_entry| async {
                if let Some(entry) = maybe_entry {
                    let entry_value = entry.into_value();
                    if let Some(client_) = entry_value.upgrade() {
                        // return if the client is already built
                        tracing::info!("reuse existing kafka client for {}", broker_address);
                        client = Some(client_);
                        return Ok(Op::Nop);
                    }
                }
                tracing::info!("build new kafka client for {}", broker_address);
                client = Some(build_kafka_client(&config, &properties).await?);
                Ok(Op::Put(Arc::downgrade(client.as_ref().unwrap())))
            })
            .await?;

        Ok(Self {
            context,
            broker_address,
            topic,
            topic_regex,
            client: client.unwrap(),
            start_offset: scan_start_offset,
            stop_offset: KafkaEnumeratorOffset::None,
            sync_call_timeout: properties.common.sync_call_timeout,
            high_watermark_metrics: HashMap::new(),
            properties,
            config,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<KafkaSplit>> {
        let topic_partitions = self.fetch_topic_partitions().await.with_context(|| {
            format!(
                "failed to fetch metadata from kafka ({})",
                self.broker_address
            )
        })?;

        let watermarks = self.get_watermarks(topic_partitions.as_ref()).await?;
        let mut start_offsets = self
            .fetch_start_offset(topic_partitions.as_ref(), &watermarks)
            .await?;

        let mut stop_offsets = self
            .fetch_stop_offset(topic_partitions.as_ref(), &watermarks)
            .await?;

        let ret: Vec<_> = topic_partitions
            .into_iter()
            .map(|mut split| {
                let split_id = split.id();
                split.start_offset = start_offsets.remove(&split_id).unwrap();
                split.stop_offset = stop_offsets.remove(&split_id).unwrap();
                split
            })
            .collect();

        Ok(ret)
    }

    async fn on_drop_fragments(&mut self, fragment_ids: Vec<FragmentId>) -> ConnectorResult<()> {
        self.drop_consumer_groups(fragment_ids).await
    }

    async fn on_finish_backfill(&mut self, fragment_ids: Vec<FragmentId>) -> ConnectorResult<()> {
        self.drop_consumer_groups(fragment_ids).await
    }
}

async fn build_kafka_client(
    config: &ClientConfig,
    properties: &KafkaProperties,
) -> ConnectorResult<Arc<KafkaConsumer>> {
    let ctx_common = KafkaContextCommon::new(
        properties.privatelink_common.broker_rewrite_map.clone(),
        None,
        None,
        properties.aws_auth_props.clone(),
        properties.connection.is_aws_msk_iam(),
    )
    .await?;
    let client_ctx = RwConsumerContext::new(ctx_common);
    let client: KafkaConsumer = config.create_with_context(client_ctx).await?;

    // Note that before any SASL/OAUTHBEARER broker connection can succeed the application must call
    // rd_kafka_oauthbearer_set_token() once – either directly or, more typically, by invoking either
    // rd_kafka_poll(), rd_kafka_consumer_poll(), rd_kafka_queue_poll(), etc, in order to cause retrieval
    // of an initial token to occur.
    // https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a988395722598f63396d7a1bedb22adaf
    if properties.connection.is_aws_msk_iam() {
        #[cfg(not(madsim))]
        client.poll(Duration::from_secs(10)); // note: this is a blocking call
        #[cfg(madsim)]
        client.poll(Duration::from_secs(10)).await;
    }
    Ok(Arc::new(client))
}
async fn build_kafka_admin(
    config: &ClientConfig,
    properties: &KafkaProperties,
) -> ConnectorResult<KafkaAdmin> {
    let ctx_common = KafkaContextCommon::new(
        properties.privatelink_common.broker_rewrite_map.clone(),
        None,
        None,
        properties.aws_auth_props.clone(),
        properties.connection.is_aws_msk_iam(),
    )
    .await?;
    let client_ctx = RwConsumerContext::new(ctx_common);
    let client: KafkaAdmin = config.create_with_context(client_ctx).await?;
    // AdminClient calls start_poll_thread on creation, so the additional poll seems not needed. (And currently no API for this.)
    Ok(client)
}

impl KafkaSplitEnumerator {
    async fn get_watermarks(
        &mut self,
        splits: &[KafkaSplit],
    ) -> KafkaResult<HashMap<Arc<str>, (i64, i64)>> {
        let mut map = HashMap::new();
        for split in splits {
            let (low, high) = self
                .client
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    self.sync_call_timeout,
                )
                .await?;
            self.report_high_watermark(split.id(), high);
            map.insert(split.id(), (low, high));
        }
        tracing::debug!("fetch kafka watermarks: {map:?}");
        Ok(map)
    }

    pub async fn list_splits_batch(
        &mut self,
        expect_start_timestamp_millis: Option<i64>,
        expect_stop_timestamp_millis: Option<i64>,
    ) -> ConnectorResult<Vec<KafkaSplit>> {
        let topic_partitions = self.fetch_topic_partitions().await.with_context(|| {
            format!(
                "failed to fetch metadata from kafka ({})",
                self.broker_address
            )
        })?;

        // Watermark here has nothing to do with watermark in streaming processing. Watermark
        // here means smallest/largest offset available for reading.
        let mut watermarks = self.get_watermarks(topic_partitions.as_ref()).await?;

        // here we are getting the start offset and end offset for each partition with the given
        // timestamp if the timestamp is None, we will use the low watermark and high
        // watermark as the start and end offset if the timestamp is provided, we will use
        // the watermark to narrow down the range
        let mut expect_start_offset = if let Some(ts) = expect_start_timestamp_millis {
            Some(
                self.fetch_offset_for_time(topic_partitions.as_ref(), ts, &watermarks)
                    .await?,
            )
        } else {
            None
        };

        let mut expect_stop_offset = if let Some(ts) = expect_stop_timestamp_millis {
            Some(
                self.fetch_offset_for_time(topic_partitions.as_ref(), ts, &watermarks)
                    .await?,
            )
        } else {
            None
        };

        Ok(topic_partitions
            .into_iter()
            .map(|mut split| {
                let (low, high) = watermarks.remove(&split.id()).unwrap();
                let start_offset = {
                    let earliest_offset = low - 1;
                    let start = expect_start_offset
                        .as_mut()
                        .map(|m| m.remove(&split.id()).flatten().unwrap_or(earliest_offset))
                        .unwrap_or(earliest_offset);
                    i64::max(start, earliest_offset)
                };
                let stop_offset = {
                    let stop = expect_stop_offset
                        .as_mut()
                        .map(|m| m.remove(&split.id()).unwrap_or(Some(high)))
                        .unwrap_or(Some(high))
                        .unwrap_or(high);
                    i64::min(stop, high)
                };

                if start_offset > stop_offset {
                    tracing::warn!(
                        "Skipping topic {} partition {}: requested start offset {} is greater than stop offset {}",
                        split.topic.as_str(),
                        split.partition,
                        start_offset,
                        stop_offset
                    );
                }
                split.start_offset = Some(start_offset);
                split.stop_offset = Some(stop_offset);
                split
            })
            .collect::<Vec<KafkaSplit>>())
    }

    async fn fetch_stop_offset(
        &self,
        splits: &[KafkaSplit],
        watermarks: &HashMap<Arc<str>, (i64, i64)>,
    ) -> KafkaResult<HashMap<Arc<str>, Option<i64>>> {
        match self.stop_offset {
            KafkaEnumeratorOffset::Earliest => unreachable!(),
            KafkaEnumeratorOffset::Latest => {
                let mut map = HashMap::new();
                for split in splits {
                    let (_, high_watermark) = watermarks.get(&split.id()).unwrap();
                    map.insert(split.id(), Some(*high_watermark));
                }
                Ok(map)
            }
            KafkaEnumeratorOffset::Timestamp(time) => {
                self.fetch_offset_for_time(splits, time, watermarks).await
            }
            KafkaEnumeratorOffset::None => {
                splits.iter().map(|split| Ok((split.id(), None))).collect()
            }
        }
    }

    async fn fetch_start_offset(
        &self,
        splits: &[KafkaSplit],
        watermarks: &HashMap<Arc<str>, (i64, i64)>,
    ) -> KafkaResult<HashMap<Arc<str>, Option<i64>>> {
        match self.start_offset {
            KafkaEnumeratorOffset::Earliest | KafkaEnumeratorOffset::Latest => {
                let mut map = HashMap::new();
                for split in splits {
                    let (low_watermark, high_watermark) = watermarks.get(&split.id()).unwrap();
                    let offset = match self.start_offset {
                        KafkaEnumeratorOffset::Earliest => low_watermark - 1,
                        KafkaEnumeratorOffset::Latest => high_watermark - 1,
                        _ => unreachable!(),
                    };
                    map.insert(split.id(), Some(offset));
                }
                Ok(map)
            }
            KafkaEnumeratorOffset::Timestamp(time) => {
                self.fetch_offset_for_time(splits, time, watermarks).await
            }
            KafkaEnumeratorOffset::None => {
                splits.iter().map(|split| Ok((split.id(), None))).collect()
            }
        }
    }

    async fn fetch_offset_for_time(
        &self,
        splits: &[KafkaSplit],
        time: i64,
        watermarks: &HashMap<Arc<str>, (i64, i64)>,
    ) -> KafkaResult<HashMap<Arc<str>, Option<i64>>> {
        let mut tpl = TopicPartitionList::new();

        for split in splits {
            tpl.add_partition_offset(split.topic.as_str(), split.partition, Offset::Offset(time))?;
        }

        let offsets = self
            .client
            .offsets_for_times(tpl, self.sync_call_timeout)
            .await?;

        let mut result = HashMap::with_capacity(splits.len());

        for split in splits {
            let Some(elem) = offsets.find_partition(split.topic.as_str(), split.partition) else {
                continue;
            };
            match elem.offset() {
                Offset::Offset(offset) => {
                    // XXX(rc): currently in RW source, `offset` means the last consumed offset, so we need to subtract 1
                    result.insert(split.id(), Some(offset - 1));
                }
                Offset::End => {
                    let (_, high_watermark) = watermarks.get(&split.id()).unwrap();
                    tracing::info!(
                        source_id = %self.context.info.source_id,
                        topic = %split.topic,
                        "no message found before timestamp {} (ms) for partition {}, start from latest",
                        time,
                        split.partition
                    );
                    result.insert(split.id(), Some(high_watermark - 1)); // align to Latest
                }
                Offset::Invalid => {
                    // special case for madsim test
                    // For a read Kafka, it returns `Offset::Latest` when the timestamp is later than the latest message in the partition
                    // But in madsim, it returns `Offset::Invalid`
                    // So we align to Latest here
                    tracing::info!(
                        source_id = %self.context.info.source_id,
                        topic = %split.topic,
                        "got invalid offset for partition  {} at timestamp {}, align to latest",
                        split.partition,
                        time
                    );
                    let (_, high_watermark) = watermarks.get(&split.id()).unwrap();
                    result.insert(split.id(), Some(high_watermark - 1)); // align to Latest
                }
                Offset::Beginning => {
                    let (low, _) = watermarks.get(&split.id()).unwrap();
                    tracing::info!(
                        source_id = %self.context.info.source_id,
                        topic = %split.topic,
                        "all message in partition {} is after timestamp {} (ms), start from earliest",
                        split.partition,
                        time,
                    );
                    result.insert(split.id(), Some(low - 1)); // align to Earliest
                }
                err_offset @ Offset::Stored | err_offset @ Offset::OffsetTail(_) => {
                    tracing::error!(
                        source_id = %self.context.info.source_id,
                        topic = %split.topic,
                        "got invalid offset for partition {}: {err_offset:?}",
                        split.partition,
                        err_offset = err_offset,
                    );
                    return Err(KafkaError::OffsetFetch(RDKafkaErrorCode::NoOffset));
                }
            }
        }

        Ok(result)
    }

    #[inline]
    fn report_high_watermark(&mut self, split_id: Arc<str>, offset: i64) {
        let source_id = self.context.info.source_id.to_string();
        let split_id_label = split_id.to_string();
        let high_watermark_metrics = self
            .high_watermark_metrics
            .entry(split_id_label.clone())
            .or_insert_with(|| {
                self.context
                    .metrics
                    .high_watermark
                    .with_guarded_label_values(&[&source_id, &split_id_label])
            });
        high_watermark_metrics.set(offset);
    }

    pub async fn check_reachability(&self) -> ConnectorResult<()> {
        let topics = self.fetch_topics().await?;
        if self.topic.is_some() && topics.is_empty() {
            bail!("topic {} not found", self.topic.as_deref().unwrap());
        }
        Ok(())
    }

    async fn fetch_topics(&self) -> ConnectorResult<Vec<String>> {
        let metadata = self
            .client
            .fetch_metadata(self.topic.as_deref(), self.sync_call_timeout)
            .await?;
        let mut topics = metadata
            .topics()
            .iter()
            .filter(|topic_meta| !topic_meta.name().starts_with("__"))
            .filter(|topic_meta| topic_meta.error().is_none())
            .filter(|topic_meta| !topic_meta.partitions().is_empty())
            .filter(|topic_meta| {
                self.topic
                    .as_deref()
                    .is_none_or(|topic| topic_meta.name() == topic)
            })
            .filter(|topic_meta| {
                self.topic_regex
                    .as_ref()
                    .is_none_or(|regex| regex.is_match(topic_meta.name()))
            })
            .map(|topic_meta| topic_meta.name().to_owned())
            .collect::<Vec<_>>();
        topics.sort();
        Ok(topics)
    }

    fn split_id_for_topic_partition(&self, topic: &str, partition: i32) -> Arc<str> {
        if self.topic.is_some() {
            partition.to_string().into()
        } else {
            format!("{topic}-{partition}").into()
        }
    }

    async fn fetch_topic_partitions(&self) -> ConnectorResult<Vec<KafkaSplit>> {
        let topics = self.fetch_topics().await?;
        if self.topic.is_some() && topics.is_empty() {
            bail!("topic {} not found", self.topic.as_deref().unwrap());
        }

        let metadata = self
            .client
            .fetch_metadata(self.topic.as_deref(), self.sync_call_timeout)
            .await?;

        let mut splits = vec![];
        for topic in &topics {
            let Some(topic_meta) = metadata.topics().iter().find(|meta| meta.name() == topic)
            else {
                continue;
            };
            for partition in topic_meta
                .partitions()
                .iter()
                .map(|partition| partition.id())
            {
                let split = KafkaSplit::new(partition, None, None, topic.clone())
                    .with_split_id(self.split_id_for_topic_partition(topic, partition));
                splits.push(split);
            }
        }
        splits.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
        Ok(splits)
    }
}
