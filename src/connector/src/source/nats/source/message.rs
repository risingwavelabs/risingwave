// Copyright 2023 RisingWave Labs
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

use async_nats::HeaderMap;
use async_nats::jetstream::Message;
use itertools::Itertools;
use risingwave_common::types::{
    Datum, DatumCow, DatumRef, ListValue, ScalarImpl, ScalarRefImpl, StructValue,
};
use risingwave_pb::data::DataType as PbDataType;
use risingwave_pb::data::data_type::TypeName as PbTypeName;

use crate::parser::additional_columns::get_kafka_header_item_datatype;
use crate::source::base::SourceMessage;
use crate::source::{SourceMeta, SplitId};

#[derive(Debug, Clone)]
pub struct NatsMeta {
    pub subject: String,
    pub headers: Option<HeaderMap>,
}

impl NatsMeta {
    pub fn extract_subject(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(self.subject.as_str()))
    }

    pub fn extract_header_inner<'a>(
        &'a self,
        inner_field: &str,
        data_type: Option<&PbDataType>,
    ) -> Option<DatumCow<'a>> {
        let target_value = self.headers.as_ref().and_then(|headers| {
            headers
                .iter()
                .find(|(name, _)| AsRef::<str>::as_ref(*name) == inner_field)
                .and_then(|(_, values)| values.first())
        })?;

        Some(header_value_to_datum(target_value.as_str(), data_type))
    }

    pub fn extract_headers(&self) -> Option<Datum> {
        self.headers.as_ref().map(|headers| {
            let header_item: Vec<Datum> = headers
                .iter()
                .flat_map(|(name, values)| {
                    let name_str: String = AsRef::<str>::as_ref(name).to_owned();
                    values.iter().map(move |value| {
                        let bytes: &[u8] = value.as_ref();
                        Some(ScalarImpl::Struct(StructValue::new(vec![
                            Some(ScalarImpl::Utf8(name_str.clone().into())),
                            Some(ScalarImpl::Bytea(bytes.to_vec().into())),
                        ])))
                    })
                })
                .collect_vec();
            Some(ScalarImpl::List(ListValue::from_datum_iter(
                &get_kafka_header_item_datatype(),
                header_item,
            )))
        })
    }
}

fn header_value_to_datum<'a>(value: &'a str, data_type: Option<&PbDataType>) -> DatumCow<'a> {
    if let Some(data_type) = data_type
        && data_type.type_name == PbTypeName::Varchar as i32
    {
        Some(ScalarRefImpl::Utf8(value)).into()
    } else {
        Some(ScalarRefImpl::Bytea(value.as_bytes())).into()
    }
}

#[derive(Clone, Debug)]
pub struct NatsMessage {
    pub split_id: SplitId,
    pub sequence_number: String,
    pub payload: Vec<u8>,
    pub reply_subject: Option<String>,
    pub subject: String,
    pub headers: Option<HeaderMap>,
}

impl From<NatsMessage> for SourceMessage {
    fn from(message: NatsMessage) -> Self {
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            // For nats jetstream, use sequence id as offset
            //
            // DEPRECATED: no longer use sequence id as offset, let nats broker handle failover
            // use reply_subject as offset for ack use, we just check the persisted state for whether this is the first run
            offset: message.reply_subject.unwrap_or_default(),
            split_id: message.split_id,
            meta: SourceMeta::Nats(NatsMeta {
                subject: message.subject,
                headers: message.headers,
            }),
        }
    }
}

impl NatsMessage {
    pub fn new(split_id: SplitId, message: Message) -> Self {
        let sequence_number = message.info().unwrap().stream_sequence.to_string();
        NatsMessage {
            split_id,
            sequence_number,
            payload: message.message.payload.to_vec(),
            reply_subject: message.message.reply.map(|s| s.as_str().to_owned()),
            subject: message.message.subject.as_str().to_owned(),
            headers: message.message.headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use async_nats::HeaderMap;
    use risingwave_common::types::{DataType, ScalarImpl, ScalarRefImpl, ToOwnedDatum};
    use risingwave_pb::data::DataType as PbDataType;

    use super::*;

    fn nats_meta(subject: &str, headers: Option<HeaderMap>) -> NatsMeta {
        NatsMeta {
            subject: subject.to_owned(),
            headers,
        }
    }

    fn varchar_pb() -> PbDataType {
        DataType::Varchar.to_protobuf()
    }

    #[test]
    fn extract_header_inner_varchar_borrowed_when_ascii() {
        let mut headers = HeaderMap::new();
        headers.insert("trace-id", "abc-123");
        let meta = nats_meta("subj", Some(headers));

        let result = meta
            .extract_header_inner("trace-id", Some(&varchar_pb()))
            .unwrap()
            .to_owned_datum();
        assert_eq!(result, Some(ScalarImpl::Utf8("abc-123".into())));
    }

    #[test]
    fn extract_header_inner_bytea_when_no_data_type() {
        let mut headers = HeaderMap::new();
        headers.insert("bin", "raw");
        let meta = nats_meta("subj", Some(headers));

        let result = meta
            .extract_header_inner("bin", None)
            .unwrap()
            .to_owned_datum();
        assert_eq!(result, Some(ScalarImpl::Bytea(b"raw".to_vec().into())));
    }

    #[test]
    fn extract_header_inner_returns_none_when_field_missing() {
        let mut headers = HeaderMap::new();
        headers.insert("present", "yes");
        let meta = nats_meta("subj", Some(headers));

        assert!(
            meta.extract_header_inner("absent", Some(&varchar_pb()))
                .is_none()
        );
    }

    #[test]
    fn extract_header_inner_returns_none_when_headers_none() {
        let meta = nats_meta("subj", None);
        assert!(meta.extract_header_inner("anything", None).is_none());
    }

    #[test]
    fn extract_headers_flattens_repeated_names() {
        let mut headers = HeaderMap::new();
        headers.insert("k1", "v1");
        headers.append("k2", "v2a");
        headers.append("k2", "v2b");
        let meta = nats_meta("subj", Some(headers));

        let datum = meta.extract_headers().expect("outer Option");
        let list = match datum {
            Some(ScalarImpl::List(list)) => list,
            other => panic!("expected list, got {other:?}"),
        };

        // Inter-key order comes from a `HashMap`, so sort before comparing.
        let mut entries = list
            .iter()
            .map(|datum| {
                let Some(ScalarRefImpl::Struct(item)) = datum else {
                    panic!("expected struct, got {datum:?}");
                };
                let mut fields = item.iter_fields_ref();
                let name = match fields.next().flatten() {
                    Some(ScalarRefImpl::Utf8(name)) => name.to_owned(),
                    other => panic!("expected varchar name, got {other:?}"),
                };
                let value = match fields.next().flatten() {
                    Some(ScalarRefImpl::Bytea(value)) => value.to_vec(),
                    other => panic!("expected bytea value, got {other:?}"),
                };
                (name, value)
            })
            .collect_vec();
        entries.sort();

        assert_eq!(
            entries,
            vec![
                ("k1".to_owned(), b"v1".to_vec()),
                ("k2".to_owned(), b"v2a".to_vec()),
                ("k2".to_owned(), b"v2b".to_vec()),
            ]
        );
    }

    #[test]
    fn extract_headers_returns_none_when_headers_none() {
        let meta = nats_meta("subj", None);
        assert!(meta.extract_headers().is_none());
    }

    #[test]
    fn source_message_from_nats_message_preserves_fields() {
        let mut headers = HeaderMap::new();
        headers.insert("h", "v");
        let msg = NatsMessage {
            split_id: "split-0".into(),
            sequence_number: "42".to_owned(),
            payload: b"body".to_vec(),
            reply_subject: Some("reply.subject".to_owned()),
            subject: "orders.new".to_owned(),
            headers: Some(headers),
        };

        let source: SourceMessage = msg.into();
        assert!(source.key.is_none());
        assert_eq!(source.payload.as_deref(), Some(b"body".as_ref()));
        assert_eq!(source.offset, "reply.subject");
        assert_eq!(&*source.split_id, "split-0");
        let SourceMeta::Nats(nats) = source.meta else {
            panic!("expected SourceMeta::Nats");
        };
        assert_eq!(nats.subject, "orders.new");
        assert!(nats.headers.is_some());
    }
}
