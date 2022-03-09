use std::str::from_utf8;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use crate::base::{SourceMessage, SourceReader};
use crate::kafka::source::message::KafkaMessage;
use crate::kafka::split::{KafkaOffset, KafkaSplit};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct KafkaSplitReader {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    partition_queue: StreamPartitionQueue<DefaultConsumerContext>,
    topic: String,
    assigned_split: KafkaSplit,
}

#[async_trait]
impl SourceReader for KafkaSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<Vec<u8>>>> {
        let mut stream = self
            .partition_queue
            .stream()
            .ready_chunks(KAFKA_MAX_FETCH_MESSAGES);

        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        let mut ret = Vec::with_capacity(chunk.len());

        for msg in chunk {
            let msg = msg.map_err(|e| anyhow!(e))?;

            let offset = msg.offset();

            if let KafkaOffset::Offset(stopping_offset) = self.assigned_split.stop_offset {
                if offset >= stopping_offset {
                    // `self.partition_queue` will expire when it's done
                    // FIXME(chen): error handling
                    self.consumer
                        .assign(&TopicPartitionList::new())
                        .map_err(|e| anyhow!(e))?;

                    break;
                }
            }

            ret.push(KafkaMessage::from(msg).serialize()?.into_bytes());
        }

        Ok(Some(ret))
    }

    async fn assign_split<'a>(&mut self, split: &'a [u8]) -> Result<()> {
        let split: KafkaSplit = serde_json::from_str(from_utf8(split)?)?;
        let mut tpl = TopicPartitionList::new();

        let offset = match split.start_offset {
            KafkaOffset::None | KafkaOffset::Earliest => Offset::Beginning,
            KafkaOffset::Latest => Offset::End,
            KafkaOffset::Offset(offset) => Offset::Offset(offset),
            KafkaOffset::Timestamp(_) => unimplemented!(),
        };

        tpl.add_partition_offset(self.topic.as_str(), split.partition, offset)
            .map_err(|e| anyhow!(e))?;

        self.consumer.assign(&tpl).map_err(|e| anyhow!(e))?;

        let partition_queue = self
            .consumer
            .split_partition_queue(self.topic.as_str(), split.partition)
            .ok_or(anyhow!("Failed to split partition queue"))?;

        self.partition_queue = partition_queue;
        self.assigned_split = split;

        Ok(())
    }
}

impl KafkaSplitReader {
    fn create_consumer(&self) -> Result<StreamConsumer<DefaultConsumerContext>> {
        let mut config = ClientConfig::new();

        config.set("topic.metadata.refresh.interval.ms", "30000");
        config.set("fetch.message.max.bytes", "134217728");
        config.set("auto.offset.reset", "earliest");

        if config.get("group.id").is_none() {
            config.set(
                "group.id",
                format!(
                    "consumer-{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                ),
            );
        }

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("enable.auto.commit", "false");
        // config.set("bootstrap.servers", self.bootstrap_servers.join(","));

        config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(DefaultConsumerContext)
            .map_err(|e| anyhow!(e))
    }
}
