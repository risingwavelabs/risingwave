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

use itertools::Itertools;
use prost_013::Message as _;
use pulsar::consumer::Message;
use risingwave_common::types::{
    Datum, DatumCow, ListValue, ScalarImpl, ScalarRefImpl, StructValue,
};
use risingwave_pb::data::DataType as PbDataType;
use risingwave_pb::data::data_type::TypeName as PbTypeName;

use crate::parser::additional_columns::get_kafka_header_item_datatype;
use crate::source::{SourceMessage, SourceMeta};

#[derive(Debug, Clone)]
pub struct PulsarMeta {
    pub schema_version: Option<Vec<u8>>,
    pub ack_message_id: Option<Vec<u8>>,
    pub properties: Option<Vec<(String, String)>>,
}

impl PulsarMeta {
    pub fn extract_header_inner<'a>(
        &'a self,
        inner_field: &str,
        data_type: Option<&PbDataType>,
    ) -> Option<DatumCow<'a>> {
        let (_, target_value) = self
            .properties
            .as_ref()?
            .iter()
            .find(|(key, _)| key == inner_field)?;

        if let Some(data_type) = data_type
            && data_type.type_name == PbTypeName::Varchar as i32
        {
            Some(Some(ScalarRefImpl::Utf8(target_value.as_str())).into())
        } else {
            Some(Some(ScalarRefImpl::Bytea(target_value.as_bytes())).into())
        }
    }

    pub fn extract_headers(&self) -> Option<Datum> {
        self.properties.as_ref().map(|properties| {
            let header_item: Vec<Datum> = properties
                .iter()
                .map(|(key, value)| {
                    Some(ScalarImpl::Struct(StructValue::new(vec![
                        Some(ScalarImpl::Utf8(key.clone().into())),
                        Some(ScalarImpl::Bytea(value.as_bytes().into())),
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
    pub fn from_pulsar_message(msg: Message<Vec<u8>>, require_header: bool) -> Self {
        let Message {
            topic,
            payload,
            message_id,
            ..
        } = msg;
        let message_id = message_id.id;
        let ack_data_bytes = message_id.encode_to_vec();
        let metadata = payload.metadata;

        SourceMessage {
            key: metadata.partition_key.map(|k| k.into()),
            payload: Some(payload.data),
            offset: format!(
                "{}:{}:{}:{}",
                message_id.ledger_id,
                message_id.entry_id,
                message_id.partition.unwrap_or(-1),
                message_id.batch_index.unwrap_or(-1)
            ),
            split_id: topic.into(),
            meta: SourceMeta::Pulsar(PulsarMeta {
                schema_version: metadata.schema_version,
                ack_message_id: Some(ack_data_bytes),
                properties: require_header.then(|| {
                    metadata
                        .properties
                        .into_iter()
                        .map(|kv| (kv.key, kv.value))
                        .collect()
                }),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::data::DataType;

    use super::*;

    #[test]
    fn test_extract_pulsar_header_inner() {
        let meta = PulsarMeta {
            schema_version: None,
            ack_message_id: None,
            properties: Some(vec![("tenant".to_owned(), "acme".to_owned())]),
        };

        assert_eq!(
            meta.extract_header_inner("tenant", None),
            Some(Some(ScalarRefImpl::Bytea("acme".as_bytes())).into())
        );
        assert_eq!(
            meta.extract_header_inner(
                "tenant",
                Some(&DataType {
                    type_name: PbTypeName::Varchar as i32,
                    ..Default::default()
                })
            ),
            Some(Some(ScalarRefImpl::Utf8("acme")).into())
        );
    }

    #[test]
    fn test_extract_pulsar_headers() {
        let meta = PulsarMeta {
            schema_version: None,
            ack_message_id: None,
            properties: Some(vec![("tenant".to_owned(), "acme".to_owned())]),
        };

        assert_eq!(
            meta.extract_headers(),
            Some(Some(ScalarImpl::List(ListValue::from_datum_iter(
                &get_kafka_header_item_datatype(),
                vec![Some(ScalarImpl::Struct(StructValue::new(vec![
                    Some(ScalarImpl::Utf8("tenant".into())),
                    Some(ScalarImpl::Bytea("acme".as_bytes().into())),
                ])))],
            ))))
        );
    }
}
