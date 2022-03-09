use std::borrow::BorrowMut;
use std::str::from_utf8;
use std::time::SystemTime;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};

use crate::base::{SourceMessage, SourceReader};
use crate::pulsar::source::message::PulsarMessage;
use crate::pulsar::split::{PulsarOffset, PulsarSplit};

pub struct PulsarSplitReader {
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    split: PulsarSplit,
}

const PULSAR_MAX_FETCH_MESSAGES: u32 = 1024;

#[async_trait]
impl SourceReader for PulsarSplitReader {
    async fn next(&mut self) -> anyhow::Result<Option<Vec<Vec<u8>>>> {
        let mut stream = self
            .consumer
            .borrow_mut()
            .ready_chunks(PULSAR_MAX_FETCH_MESSAGES as usize);

        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        let mut ret = Vec::with_capacity(chunk.len());

        for msg in chunk {
            let msg = msg.map_err(|e| anyhow!(e))?;

            let entry_id = msg.message_id.id.entry_id;

            let should_stop = match self.split.stop_offset {
                PulsarOffset::MessageID(id) => entry_id >= id,
                PulsarOffset::Timestamp(timestamp) => {
                    msg.payload.metadata.event_time() >= timestamp
                }
                PulsarOffset::None => false,
            };

            if should_stop {
                self.consumer
                    .borrow_mut()
                    .unsubscribe()
                    .await
                    .map_err(|e| anyhow!(e))?;
                break;
            }

            ret.push(PulsarMessage::new(msg).serialize()?.into_bytes());
        }

        Ok(Some(ret))
    }

    async fn assign_split<'a>(&mut self, split: &'a [u8]) -> anyhow::Result<()> {
        let split: PulsarSplit = serde_json::from_str(from_utf8(split)?)?;
        let consumer: Consumer<Vec<u8>, TokioExecutor> = self
            .pulsar
            .consumer()
            .with_topic(split.sub_topic.as_str())
            .with_batch_size(PULSAR_MAX_FETCH_MESSAGES)
            //.with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(format!(
                "consumer-{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ))
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        self.consumer = consumer;
        self.split = split;

        todo!("seek offset here")
    }
}
