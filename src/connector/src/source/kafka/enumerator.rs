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
use thiserror_ext::AsReport;

use crate::connector_common::read_kafka_log_level;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::SourceEnumeratorContextRef;
use crate::source::base::SplitEnumerator;
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

/// Selects which Kafka topics this source consumes from.
pub enum KafkaTopicSelector {
    /// A single fixed topic name.
    Fixed(String),
    /// A regex pattern matching multiple topics.
    Regex { pattern: String, compiled: Regex },
}

pub struct KafkaSplitEnumerator {
    context: SourceEnumeratorContextRef,
    broker_address: String,
    selector: KafkaTopicSelector,
    client: Arc<KafkaConsumer>,
    start_offset: KafkaEnumeratorOffset,

    // maybe used in the future for batch processing
    stop_offset: KafkaEnumeratorOffset,

    sync_call_timeout: Duration,
    high_watermark_metrics: HashMap<String, LabelGuardedIntGauge>,

    /// Cached splits from the last successful discovery (regex mode only).
    /// Used for disappearance protection: if a previously assigned topic becomes
    /// unavailable, we return a retriable error instead of dropping the splits.
    last_discovered_splits: Option<Vec<KafkaSplit>>,

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
        tracing::debug!(?fragment_ids, "delete groups result: {res:?}");
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
        properties.validate_topic_config()?;

        let mut config = rdkafka::ClientConfig::new();
        let common_props = &properties.common;

        let broker_address = properties.connection.brokers.clone();
        let selector = match properties.topic_regex.as_deref().filter(|p| !p.is_empty()) {
            Some(pattern) => {
                let compiled = Regex::new(pattern)
                    .with_context(|| format!("invalid topic.regex pattern '{}'", pattern))?;
                KafkaTopicSelector::Regex {
                    pattern: pattern.to_owned(),
                    compiled,
                }
            }
            None => KafkaTopicSelector::Fixed(common_props.topic.clone()),
        };
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
            selector,
            client: client.unwrap(),
            start_offset: scan_start_offset,
            stop_offset: KafkaEnumeratorOffset::None,
            sync_call_timeout: properties.common.sync_call_timeout,
            high_watermark_metrics: HashMap::new(),
            last_discovered_splits: None,
            properties,
            config,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<KafkaSplit>> {
        match &self.selector {
            KafkaTopicSelector::Regex { .. } => {
                return self.list_splits_regex().await;
            }
            KafkaTopicSelector::Fixed(_) => {}
        }

        let topic = match &self.selector {
            KafkaTopicSelector::Fixed(t) => t.clone(),
            KafkaTopicSelector::Regex { .. } => unreachable!(),
        };
        let topic_partitions = self.fetch_topic_partition(&topic).await.with_context(|| {
            format!(
                "failed to fetch metadata from kafka ({})",
                self.broker_address
            )
        })?;

        let watermarks = self
            .get_watermarks(&topic, topic_partitions.as_ref())
            .await?;
        let mut start_offsets = self
            .fetch_start_offset(&topic, topic_partitions.as_ref(), &watermarks)
            .await?;

        let mut stop_offsets = self
            .fetch_stop_offset(&topic, topic_partitions.as_ref(), &watermarks)
            .await?;

        let ret: Vec<_> = topic_partitions
            .into_iter()
            .map(|partition| KafkaSplit {
                topic: topic.clone(),
                partition,
                start_offset: start_offsets.remove(&partition).unwrap(),
                stop_offset: stop_offsets.remove(&partition).unwrap(),
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
        topic: &str,
        partitions: &[i32],
    ) -> KafkaResult<HashMap<i32, (i64, i64)>> {
        let mut map = HashMap::new();
        for partition in partitions {
            let (low, high) = self
                .client
                .fetch_watermarks(topic, *partition, self.sync_call_timeout)
                .await?;
            self.report_high_watermark(topic, *partition, high);
            map.insert(*partition, (low, high));
        }
        tracing::debug!(topic, "fetch kafka watermarks: {map:?}");
        Ok(map)
    }

    pub async fn list_splits_batch(
        &mut self,
        expect_start_timestamp_millis: Option<i64>,
        expect_stop_timestamp_millis: Option<i64>,
    ) -> ConnectorResult<Vec<KafkaSplit>> {
        let topic = match &self.selector {
            KafkaTopicSelector::Fixed(t) => t.clone(),
            KafkaTopicSelector::Regex { .. } => {
                bail!("list_splits_batch is not supported for regex topic sources")
            }
        };
        let topic_partitions = self.fetch_topic_partition(&topic).await.with_context(|| {
            format!(
                "failed to fetch metadata from kafka ({})",
                self.broker_address
            )
        })?;

        // Watermark here has nothing to do with watermark in streaming processing. Watermark
        // here means smallest/largest offset available for reading.
        let mut watermarks = self
            .get_watermarks(&topic, topic_partitions.as_ref())
            .await?;

        // here we are getting the start offset and end offset for each partition with the given
        // timestamp if the timestamp is None, we will use the low watermark and high
        // watermark as the start and end offset if the timestamp is provided, we will use
        // the watermark to narrow down the range
        let mut expect_start_offset = if let Some(ts) = expect_start_timestamp_millis {
            Some(
                self.fetch_offset_for_time(&topic, topic_partitions.as_ref(), ts, &watermarks)
                    .await?,
            )
        } else {
            None
        };

        let mut expect_stop_offset = if let Some(ts) = expect_stop_timestamp_millis {
            Some(
                self.fetch_offset_for_time(&topic, topic_partitions.as_ref(), ts, &watermarks)
                    .await?,
            )
        } else {
            None
        };

        Ok(topic_partitions
            .iter()
            .map(|partition| {
                let (low, high) = watermarks.remove(partition).unwrap();
                let start_offset = {
                    let earliest_offset = low - 1;
                    let start = expect_start_offset
                        .as_mut()
                        .map(|m| m.remove(partition).flatten().unwrap_or(earliest_offset))
                        .unwrap_or(earliest_offset);
                    i64::max(start, earliest_offset)
                };
                let stop_offset = {
                    let stop = expect_stop_offset
                        .as_mut()
                        .map(|m| m.remove(partition).unwrap_or(Some(high)))
                        .unwrap_or(Some(high))
                        .unwrap_or(high);
                    i64::min(stop, high)
                };

                if start_offset > stop_offset {
                    tracing::warn!(
                        "Skipping topic {} partition {}: requested start offset {} is greater than stop offset {}",
                        topic,
                        partition,
                        start_offset,
                        stop_offset
                    );
                }
                KafkaSplit {
                    topic: topic.clone(),
                    partition: *partition,
                    start_offset: Some(start_offset),
                    stop_offset: Some(stop_offset),
                    }
            })
            .collect::<Vec<KafkaSplit>>())
    }

    async fn fetch_stop_offset(
        &self,
        topic: &str,
        partitions: &[i32],
        watermarks: &HashMap<i32, (i64, i64)>,
    ) -> KafkaResult<HashMap<i32, Option<i64>>> {
        match self.stop_offset {
            KafkaEnumeratorOffset::Earliest => unreachable!(),
            KafkaEnumeratorOffset::Latest => {
                let mut map = HashMap::new();
                for partition in partitions {
                    let (_, high_watermark) = watermarks.get(partition).unwrap();
                    map.insert(*partition, Some(*high_watermark));
                }
                Ok(map)
            }
            KafkaEnumeratorOffset::Timestamp(time) => {
                self.fetch_offset_for_time(topic, partitions, time, watermarks)
                    .await
            }
            KafkaEnumeratorOffset::None => partitions
                .iter()
                .map(|partition| Ok((*partition, None)))
                .collect(),
        }
    }

    async fn fetch_start_offset(
        &self,
        topic: &str,
        partitions: &[i32],
        watermarks: &HashMap<i32, (i64, i64)>,
    ) -> KafkaResult<HashMap<i32, Option<i64>>> {
        match self.start_offset {
            KafkaEnumeratorOffset::Earliest | KafkaEnumeratorOffset::Latest => {
                let mut map = HashMap::new();
                for partition in partitions {
                    let (low_watermark, high_watermark) = watermarks.get(partition).unwrap();
                    let offset = match self.start_offset {
                        KafkaEnumeratorOffset::Earliest => low_watermark - 1,
                        KafkaEnumeratorOffset::Latest => high_watermark - 1,
                        _ => unreachable!(),
                    };
                    map.insert(*partition, Some(offset));
                }
                Ok(map)
            }
            KafkaEnumeratorOffset::Timestamp(time) => {
                self.fetch_offset_for_time(topic, partitions, time, watermarks)
                    .await
            }
            KafkaEnumeratorOffset::None => partitions
                .iter()
                .map(|partition| Ok((*partition, None)))
                .collect(),
        }
    }

    async fn fetch_offset_for_time(
        &self,
        topic: &str,
        partitions: &[i32],
        time: i64,
        watermarks: &HashMap<i32, (i64, i64)>,
    ) -> KafkaResult<HashMap<i32, Option<i64>>> {
        let mut tpl = TopicPartitionList::new();

        for partition in partitions {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(time))?;
        }

        let offsets = self
            .client
            .offsets_for_times(tpl, self.sync_call_timeout)
            .await?;

        let mut result = HashMap::with_capacity(partitions.len());

        for elem in offsets.elements_for_topic(topic) {
            match elem.offset() {
                Offset::Offset(offset) => {
                    // XXX(rc): currently in RW source, `offset` means the last consumed offset, so we need to subtract 1
                    result.insert(elem.partition(), Some(offset - 1));
                }
                Offset::End => {
                    let (_, high_watermark) = watermarks.get(&elem.partition()).unwrap();
                    tracing::info!(
                        source_id = %self.context.info.source_id,
                        "no message found before timestamp {} (ms) for partition {}, start from latest",
                        time,
                        elem.partition()
                    );
                    result.insert(elem.partition(), Some(high_watermark - 1)); // align to Latest
                }
                Offset::Invalid => {
                    // special case for madsim test
                    // For a read Kafka, it returns `Offset::Latest` when the timestamp is later than the latest message in the partition
                    // But in madsim, it returns `Offset::Invalid`
                    // So we align to Latest here
                    tracing::info!(
                        source_id = %self.context.info.source_id,
                        "got invalid offset for partition  {} at timestamp {}, align to latest",
                        elem.partition(),
                        time
                    );
                    let (_, high_watermark) = watermarks.get(&elem.partition()).unwrap();
                    result.insert(elem.partition(), Some(high_watermark - 1)); // align to Latest
                }
                Offset::Beginning => {
                    let (low, _) = watermarks.get(&elem.partition()).unwrap();
                    tracing::info!(
                        source_id = %self.context.info.source_id,
                        "all message in partition {} is after timestamp {} (ms), start from earliest",
                        elem.partition(),
                        time,
                    );
                    result.insert(elem.partition(), Some(low - 1)); // align to Earliest
                }
                err_offset @ Offset::Stored | err_offset @ Offset::OffsetTail(_) => {
                    tracing::error!(
                        source_id = %self.context.info.source_id,
                        "got invalid offset for partition {}: {err_offset:?}",
                        elem.partition(),
                        err_offset = err_offset,
                    );
                    return Err(KafkaError::OffsetFetch(RDKafkaErrorCode::NoOffset));
                }
            }
        }

        Ok(result)
    }

    #[inline]
    fn report_high_watermark(&mut self, topic: &str, partition: i32, offset: i64) {
        // Always use "topic:partition" format, consistent with the new split ID format.
        let label = format!("{}:{}", topic, partition);
        let high_watermark_metrics = self
            .high_watermark_metrics
            .entry(label.clone())
            .or_insert_with(|| {
                self.context
                    .metrics
                    .high_watermark
                    .with_guarded_label_values(&[&self.context.info.source_id.to_string(), &label])
            });
        high_watermark_metrics.set(offset);
    }

    pub async fn check_reachability(&self) -> ConnectorResult<()> {
        match &self.selector {
            KafkaTopicSelector::Fixed(topic) => {
                let _ = self
                    .client
                    .fetch_metadata(Some(topic.as_str()), self.sync_call_timeout)
                    .await?;
            }
            KafkaTopicSelector::Regex { .. } => {
                // For regex mode, just check that we can reach the broker.
                let _ = self
                    .client
                    .fetch_metadata(None, self.sync_call_timeout)
                    .await?;
            }
        }
        Ok(())
    }

    async fn fetch_topic_partition(&self, topic: &str) -> ConnectorResult<Vec<i32>> {
        let metadata = self
            .client
            .fetch_metadata(Some(topic), self.sync_call_timeout)
            .await?;

        let topic_meta = match metadata.topics() {
            [meta] => meta,
            _ => bail!("topic {} not found", topic),
        };

        if topic_meta.partitions().is_empty() {
            bail!("topic {} not found", topic);
        }

        Ok(topic_meta
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect())
    }

    /// Discover all topics matching the configured regex pattern.
    async fn fetch_topics_by_regex(&self) -> ConnectorResult<Vec<String>> {
        let re = match &self.selector {
            KafkaTopicSelector::Regex { compiled, .. } => compiled,
            KafkaTopicSelector::Fixed(_) => {
                unreachable!("fetch_topics_by_regex called in Fixed mode")
            }
        };

        let metadata = self
            .client
            .fetch_metadata(None, self.sync_call_timeout)
            .await?;

        let mut topics: Vec<String> = metadata
            .topics()
            .iter()
            .filter(|t| {
                let name = t.name();
                // Skip internal topics (starting with '__')
                !name.starts_with("__") && re.is_match(name)
            })
            .map(|t| t.name().to_owned())
            .collect();

        topics.sort();

        if topics.is_empty() {
            tracing::warn!(
                source_id = %self.context.info.source_id,
                pattern = %re,
                "no topics found matching topic.regex"
            );
        } else {
            tracing::info!(
                source_id = %self.context.info.source_id,
                pattern = %re,
                topics = ?topics,
                "discovered {} topics matching topic.regex",
                topics.len()
            );
        }

        Ok(topics)
    }

    /// List splits for all topics matching the regex pattern.
    ///
    /// Implements two key behaviors from the design:
    /// - **D6 (disappearance protection)**: If a previously discovered topic becomes
    ///   unavailable, returns a retriable error instead of dropping the splits.
    /// - **D7 (new topic unreadable)**: Skips newly discovered topics that can't be
    ///   read, logs a warning.
    async fn list_splits_regex(&mut self) -> ConnectorResult<Vec<KafkaSplit>> {
        let topics = self.fetch_topics_by_regex().await?;
        let mut all_splits = Vec::new();

        // Build the set of previously discovered topics for D6/D7 distinction.
        // Collected as owned Strings to avoid borrowing self.last_discovered_splits.
        let prev_topics: std::collections::HashSet<String> = self
            .last_discovered_splits
            .as_ref()
            .into_iter()
            .flat_map(|splits| splits.iter().map(|s| s.topic.clone()))
            .collect();

        // D6: Check if any previously discovered topic has disappeared from metadata.
        if !prev_topics.is_empty() {
            let new_topic_set: std::collections::HashSet<&str> =
                topics.iter().map(|t| t.as_str()).collect();
            for prev_topic in &prev_topics {
                if !new_topic_set.contains(prev_topic.as_str()) {
                    bail!(
                        "previously discovered topic '{}' is no longer available; \
                         will retry on next tick",
                        prev_topic
                    );
                }
            }
        }

        for topic in &topics {
            let is_previously_known = prev_topics.contains(topic);

            // Wrap per-topic discovery so we can distinguish D6 vs D7 on failure.
            let topic_splits: ConnectorResult<Vec<KafkaSplit>> = async {
                let partitions = self.fetch_topic_partition(topic).await?;
                let watermarks = self.get_watermarks(topic, &partitions).await?;
                let mut start_offsets = self
                    .fetch_start_offset(topic, &partitions, &watermarks)
                    .await?;
                let mut stop_offsets = self
                    .fetch_stop_offset(topic, &partitions, &watermarks)
                    .await?;

                Ok(partitions
                    .into_iter()
                    .map(|partition| KafkaSplit {
                        topic: topic.clone(),
                        partition,
                        start_offset: start_offsets.remove(&partition).unwrap(),
                        stop_offset: stop_offsets.remove(&partition).unwrap(),
                    })
                    .collect())
            }
            .await;

            match topic_splits {
                Ok(splits) => all_splits.extend(splits),
                Err(e) if is_previously_known => {
                    // D6: Previously assigned topic became unreadable — retriable error.
                    bail!(
                        "previously discovered topic '{}' became unavailable: {}",
                        topic,
                        e.as_report()
                    );
                }
                Err(e) => {
                    // D7: New topic is unreadable — skip with warning.
                    tracing::warn!(
                        source_id = %self.context.info.source_id,
                        topic = topic,
                        error = %e.as_report(),
                        "failed to enumerate new topic, skipping"
                    );
                    continue;
                }
            }
        }

        // Update the cache for next tick's disappearance check.
        self.last_discovered_splits = Some(all_splits.clone());

        Ok(all_splits)
    }
}
