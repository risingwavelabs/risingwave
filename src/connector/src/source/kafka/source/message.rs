// Copyright 2022 RisingWave Labs
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

use std::borrow::Cow;

use itertools::Itertools;
use rdkafka::message::{BorrowedMessage, Headers, OwnedHeaders};
use rdkafka::{Message, Timestamp};
use risingwave_common::types::{
    DataType, Datum, DatumCow, DatumRef, ListValue, MapValue, ScalarImpl, ScalarRefImpl,
    StructValue,
};
use risingwave_pb::data::DataType as PbDataType;
use risingwave_pb::data::data_type::TypeName as PbTypeName;

use crate::parser::additional_columns::get_kafka_header_item_datatype;
use crate::source::SourceMeta;
use crate::source::base::SourceMessage;

#[derive(Debug, Clone)]
pub struct KafkaMeta {
    pub timestamp: Timestamp,
    pub headers: Option<OwnedHeaders>,
}

impl KafkaMeta {
    pub fn extract_timestamp(&self) -> DatumRef<'_> {
        Some(
            risingwave_common::types::Timestamptz::from_millis(self.timestamp.to_millis()?)?.into(),
        )
    }

    pub fn extract_header_inner<'a>(
        &'a self,
        inner_field: &str,
        data_type: Option<&PbDataType>,
    ) -> Option<DatumCow<'a>> {
        let target_value = self.headers.as_ref().iter().find_map(|headers| {
            headers
                .iter()
                .find(|header| header.key == inner_field)
                .map(|header| header.value)
        })?; // if not found the specified column, return None

        let Some(target_value) = target_value else {
            return Some(Datum::None.into());
        };

        let datum = if let Some(data_type) = data_type
            && data_type.type_name == PbTypeName::Varchar as i32
        {
            match String::from_utf8_lossy(target_value) {
                Cow::Borrowed(str) => Some(ScalarRefImpl::Utf8(str)).into(),
                Cow::Owned(string) => Some(ScalarImpl::Utf8(string.into())).into(),
            }
        } else {
            Some(ScalarRefImpl::Bytea(target_value)).into()
        };

        Some(datum)
    }

    pub fn extract_headers(&self) -> Option<Datum> {
        self.headers.as_ref().map(|headers| {
            let header_item: Vec<Datum> = headers
                .iter()
                .map(|header| {
                    Some(ScalarImpl::Struct(StructValue::new(vec![
                        Some(ScalarImpl::Utf8(header.key.to_owned().into())),
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

    pub fn extract_headers_as_map(&self) -> Option<Datum> {
        self.headers.as_ref().map(|headers| {
            let mut entries: Vec<(&str, Option<&[u8]>)> = Vec::with_capacity(headers.count());

            for header in headers.iter() {
                if let Some((_, value)) = entries.iter_mut().find(|(key, _)| *key == header.key) {
                    tracing::warn!(
                        header_key = header.key,
                        "duplicate Kafka header key found when building header map; overwriting previous value"
                    );
                    *value = header.value;
                } else {
                    entries.push((header.key, header.value));
                }
            }

            let keys = entries.iter().map(|(key, _)| Some(*key)).collect::<ListValue>();
            let values = ListValue::from_datum_iter(
                &DataType::Bytea,
                entries
                    .into_iter()
                    .map(|(_, value)| value.map(|value| ScalarRefImpl::Bytea(value))),
            );
            Some(ScalarImpl::Map(
                MapValue::try_from_kv(keys, values)
                    .expect("Kafka header map keys are de-duplicated and non-null"),
            ))
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
                timestamp: message.timestamp(),
                headers: if require_header {
                    message.headers().map(|headers| headers.detach())
                } else {
                    None
                },
            }),
        }
    }
}
