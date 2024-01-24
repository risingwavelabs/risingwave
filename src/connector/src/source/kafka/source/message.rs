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

use itertools::Itertools;
use rdkafka::message::{BorrowedMessage, Headers, OwnedHeaders};
use rdkafka::Message;
use risingwave_common::types::{Datum, ListValue, Scalar, ScalarImpl, StructValue};

use crate::parser::additional_columns::get_kafka_header_item_datatype;
use crate::source::base::SourceMessage;
use crate::source::SourceMeta;

#[derive(Debug, Clone)]
pub struct KafkaMeta {
    // timestamp(milliseconds) of message append in mq
    pub timestamp: Option<i64>,
    pub headers: Option<OwnedHeaders>,
}

impl KafkaMeta {
    pub fn extract_timestamp(&self) -> Option<Datum> {
        self.timestamp
            .map(|ts| {
                risingwave_common::cast::i64_to_timestamptz(ts)
                    .unwrap()
                    .to_scalar_value()
            })
            .into()
    }

    pub fn extract_headers(&self) -> Option<Datum> {
        self.headers.as_ref().map(|headers| {
            let header_item: Vec<Datum> = headers
                .iter()
                .map(|header| {
                    Some(ScalarImpl::Struct(StructValue::new(vec![
                        Some(ScalarImpl::Utf8(header.key.to_string().into())),
                        header.value.map(|byte| ScalarImpl::Bytea(byte.into())),
                    ])))
                })
                .collect_vec();
            Some(ScalarImpl::List(ListValue::from_datum_iter(
                &get_kafka_header_item_datatype(),
                header_item,
            )))
        })
    }
}

impl SourceMessage {
    pub fn from_kafka_message(message: &BorrowedMessage<'_>, require_header: bool) -> Self {
        SourceMessage {
            // TODO(TaoWu): Possible performance improvement: avoid memory copying here.
            key: message.key().map(|p| p.to_vec()),
            payload: message.payload().map(|p| p.to_vec()),
            offset: message.offset().to_string(),
            split_id: message.partition().to_string().into(),
            meta: SourceMeta::Kafka(KafkaMeta {
                timestamp: message.timestamp().to_millis(),
                headers: if require_header {
                    message.headers().map(|headers| headers.detach())
                } else {
                    None
                },
            }),
        }
    }
}
