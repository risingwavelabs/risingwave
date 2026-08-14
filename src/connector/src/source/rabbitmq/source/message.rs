// Copyright 2026 RisingWave Labs
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

use std::sync::atomic::{AtomicU64, Ordering};

use lapin::message::Delivery;

use crate::source::{SourceMessage, SourceMeta, SplitId};

static RABBITMQ_ACK_CONSUMER_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_ack_consumer_id() -> u64 {
    RABBITMQ_ACK_CONSUMER_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Debug)]
pub struct RabbitmqMeta {
    pub ack_data: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RabbitmqAckData {
    pub(crate) ack_consumer_id: u64,
    pub(crate) delivery_tag: u64,
}

impl RabbitmqAckData {
    pub const ENCODED_LEN: usize = 16;

    pub fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);
        buf.extend_from_slice(&self.ack_consumer_id.to_be_bytes());
        buf.extend_from_slice(&self.delivery_tag.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> crate::error::ConnectorResult<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(anyhow::anyhow!(
                "invalid RabbitMQ ack data length: expected {}, got {}",
                Self::ENCODED_LEN,
                bytes.len()
            )
            .into());
        }
        let mut ack_consumer_id = [0; 8];
        ack_consumer_id.copy_from_slice(&bytes[..8]);
        let mut delivery_tag = [0; 8];
        delivery_tag.copy_from_slice(&bytes[8..]);
        Ok(Self {
            ack_consumer_id: u64::from_be_bytes(ack_consumer_id),
            delivery_tag: u64::from_be_bytes(delivery_tag),
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct RabbitmqMessage {
    split_id: SplitId,
    queue: String,
    delivery_tag: u64,
    ack_consumer_id: u64,
    ack_data: Vec<u8>,
    payload: Vec<u8>,
}

impl RabbitmqMessage {
    pub fn new(split_id: SplitId, queue: String, ack_consumer_id: u64, delivery: Delivery) -> Self {
        let Delivery {
            delivery_tag, data, ..
        } = delivery;
        let ack_data = RabbitmqAckData {
            ack_consumer_id,
            delivery_tag,
        }
        .encode();
        Self {
            split_id,
            queue,
            delivery_tag,
            ack_consumer_id,
            ack_data,
            payload: data,
        }
    }

    pub fn offset(&self) -> String {
        format!(
            "{}:{}:{}",
            self.queue, self.ack_consumer_id, self.delivery_tag
        )
    }
}

impl From<RabbitmqMessage> for SourceMessage {
    fn from(message: RabbitmqMessage) -> Self {
        let offset = message.offset();
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            offset,
            split_id: message.split_id,
            meta: SourceMeta::Rabbitmq(RabbitmqMeta {
                ack_data: Some(message.ack_data),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RabbitmqAckData;

    #[test]
    fn rabbitmq_ack_data_roundtrip() {
        let ack_data = RabbitmqAckData {
            ack_consumer_id: 42,
            delivery_tag: u64::MAX - 1,
        };

        let encoded = ack_data.encode();

        assert_eq!(encoded.len(), RabbitmqAckData::ENCODED_LEN);
        assert_eq!(RabbitmqAckData::decode(&encoded).unwrap(), ack_data);
    }

    #[test]
    fn rabbitmq_ack_data_rejects_invalid_bytes() {
        for bytes in [
            vec![],
            vec![0; RabbitmqAckData::ENCODED_LEN - 1],
            vec![0; RabbitmqAckData::ENCODED_LEN + 1],
        ] {
            assert!(RabbitmqAckData::decode(&bytes).is_err());
        }
    }
}
