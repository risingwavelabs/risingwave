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

use std::fmt::Debug;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::catalog::PbSchemaRegistryNameStrategy;
use risingwave_pb::plan_common::ColumnDesc;

use crate::parser::avro::schema_resolver::ConfluentSchemaResolver;
use crate::parser::avro::util::avro_schema_to_column_descs;
use crate::parser::unified::avro::{
    avro_extract_field_schema, avro_schema_skip_union, AvroAccess, AvroParseOptions,
};
use crate::parser::unified::AccessImpl;
use crate::parser::{AccessBuilder, EncodingProperties, EncodingType};
use crate::schema::schema_registry::{
    extract_schema_id, get_subject_by_strategy, handle_sr_list, Client,
};

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";
const PAYLOAD: &str = "payload";

#[derive(Debug)]
pub struct DebeziumAvroAccessBuilder {
    schema: Schema,
    schema_resolver: Arc<ConfluentSchemaResolver>,
    key_schema: Option<Arc<Schema>>,
    value: Option<Value>,
    encoding_type: EncodingType,
}

// TODO: reduce encodingtype match
impl AccessBuilder for DebeziumAvroAccessBuilder {
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> Result<AccessImpl<'_, '_>> {
        let (schema_id, mut raw_payload) = extract_schema_id(&payload)?;
        let schema = self.schema_resolver.get(schema_id).await?;
        self.value = Some(
            from_avro_datum(schema.as_ref(), &mut raw_payload, None)
                .map_err(|e| RwError::from(ProtocolError(e.to_string())))?,
        );
        self.key_schema = match self.encoding_type {
            EncodingType::Key => Some(schema),
            EncodingType::Value => None,
        };
        Ok(AccessImpl::Avro(AvroAccess::new(
            self.value.as_mut().unwrap(),
            AvroParseOptions::default().with_schema(match self.encoding_type {
                EncodingType::Key => self.key_schema.as_mut().unwrap(),
                EncodingType::Value => &self.schema,
            }),
        )))
    }
}

impl DebeziumAvroAccessBuilder {
    pub fn new(config: DebeziumAvroParserConfig, encoding_type: EncodingType) -> Result<Self> {
        let DebeziumAvroParserConfig {
            outer_schema,
            schema_resolver,
            ..
        } = config;

        let resolver = apache_avro::schema::ResolvedSchema::try_from(&*outer_schema)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
        // todo: to_resolved may cause stackoverflow if there's a loop in the schema
        let schema = resolver
            .to_resolved(&outer_schema)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
        Ok(Self {
            schema,
            schema_resolver,
            key_schema: None,
            value: None,
            encoding_type,
        })
    }
}

// TODO: avoid duplicated codes with `AvroParser`
#[derive(Debug, Clone)]
pub struct DebeziumAvroParserConfig {
    pub key_schema: Arc<Schema>,
    pub outer_schema: Arc<Schema>,
    pub schema_resolver: Arc<ConfluentSchemaResolver>,
}

impl DebeziumAvroParserConfig {
    pub async fn new(encoding_config: EncodingProperties) -> Result<Self> {
        let avro_config = try_match_expand!(encoding_config, EncodingProperties::Avro)?;
        let schema_location = &avro_config.row_schema_location;
        let client_config = &avro_config.client_config;
        let kafka_topic = &avro_config.topic;
        let url = handle_sr_list(schema_location)?;
        let client = Client::new(url, client_config)?;
        let resolver = ConfluentSchemaResolver::new(client);

        let name_strategy = &PbSchemaRegistryNameStrategy::Unspecified;
        let key_subject = get_subject_by_strategy(name_strategy, kafka_topic, None, true)?;
        let val_subject = get_subject_by_strategy(name_strategy, kafka_topic, None, false)?;
        let key_schema = resolver.get_by_subject_name(&key_subject).await?;
        let outer_schema = resolver.get_by_subject_name(&val_subject).await?;

        Ok(Self {
            key_schema,
            outer_schema,
            schema_resolver: Arc::new(resolver),
        })
    }

    pub fn extract_pks(&self) -> anyhow::Result<Vec<ColumnDesc>> {
        avro_schema_to_column_descs(&self.key_schema)
    }

    pub fn map_to_columns(&self) -> anyhow::Result<Vec<ColumnDesc>> {
        avro_schema_to_column_descs(avro_schema_skip_union(avro_extract_field_schema(
            &self.outer_schema,
            Some("before"),
        )?)?)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::PathBuf;

    use apache_avro::Schema;
    use itertools::Itertools;
    use maplit::{convert_args, hashmap};
    use risingwave_common::array::Op;
    use risingwave_common::catalog::ColumnDesc as CatColumnDesc;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};

    use super::*;
    use crate::parser::{
        DebeziumAvroParserConfig, DebeziumParser, SourceStreamChunkBuilder, SpecificParserConfig,
    };
    use crate::source::SourceColumnDesc;

    const DEBEZIUM_AVRO_DATA: &[u8] = b"\x00\x00\x00\x00\x06\x00\x02\xd2\x0f\x0a\x53\x61\x6c\x6c\x79\x0c\x54\x68\x6f\x6d\x61\x73\x2a\x73\x61\x6c\x6c\x79\x2e\x74\x68\x6f\x6d\x61\x73\x40\x61\x63\x6d\x65\x2e\x63\x6f\x6d\x16\x32\x2e\x31\x2e\x32\x2e\x46\x69\x6e\x61\x6c\x0a\x6d\x79\x73\x71\x6c\x12\x64\x62\x73\x65\x72\x76\x65\x72\x31\xc0\xb4\xe8\xb7\xc9\x61\x00\x30\x66\x69\x72\x73\x74\x5f\x69\x6e\x5f\x64\x61\x74\x61\x5f\x63\x6f\x6c\x6c\x65\x63\x74\x69\x6f\x6e\x12\x69\x6e\x76\x65\x6e\x74\x6f\x72\x79\x00\x02\x12\x63\x75\x73\x74\x6f\x6d\x65\x72\x73\x00\x00\x20\x6d\x79\x73\x71\x6c\x2d\x62\x69\x6e\x2e\x30\x30\x30\x30\x30\x33\x8c\x06\x00\x00\x00\x02\x72\x02\x92\xc3\xe8\xb7\xc9\x61\x00";

    fn schema_dir() -> String {
        let dir = PathBuf::from("src/test_data");
        std::fs::canonicalize(dir)
            .unwrap()
            .to_string_lossy()
            .to_string()
    }

    async fn parse_one(
        mut parser: DebeziumParser,
        columns: Vec<SourceColumnDesc>,
        payload: Vec<u8>,
    ) -> Vec<(Op, OwnedRow)> {
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        {
            let writer = builder.row_writer();
            parser
                .parse_inner(None, Some(payload), writer)
                .await
                .unwrap();
        }
        let chunk = builder.finish();
        chunk
            .rows()
            .map(|(op, row_ref)| (op, row_ref.into_owned_row()))
            .collect::<Vec<_>>()
    }

    fn get_outer_schema() -> Schema {
        let mut outer_schema_str = String::new();
        let location = schema_dir() + "/debezium_avro_msg_schema.avsc";
        std::fs::File::open(location)
            .unwrap()
            .read_to_string(&mut outer_schema_str)
            .unwrap();
        Schema::parse_str(&outer_schema_str).unwrap()
    }

    #[test]
    fn test_extract_inner_schema() {
        let inner_shema_str = r#"{
    "type": "record",
    "name": "Value",
    "namespace": "dbserver1.inventory.customers",
    "fields": [
        {
            "name": "id",
            "type": "int"
        },
        {
            "name": "first_name",
            "type": "string"
        },
        {
            "name": "last_name",
            "type": "string"
        },
        {
            "name": "email",
            "type": "string"
        }
    ]
}"#;

        let outer_schema = get_outer_schema();
        let expected_inner_schema = Schema::parse_str(inner_shema_str).unwrap();
        let extracted_inner_schema = avro_schema_skip_union(
            avro_extract_field_schema(&outer_schema, Some("before")).unwrap(),
        )
        .unwrap();
        assert_eq!(&expected_inner_schema, extracted_inner_schema);
    }

    #[test]
    fn test_get_pk_column_names() {
        let key_schema_str = r#"{
    "type": "record",
    "name": "Key",
    "namespace": "dbserver1.inventory.customers",
    "fields": [{
        "name": "id",
        "type": "int"
    }],
    "connect.name": "dbserver1.inventory.customers.Key"
}
"#;
        let key_schema = Schema::parse_str(key_schema_str).unwrap();
        let names: Vec<String> = avro_schema_to_column_descs(&key_schema)
            .unwrap()
            .drain(..)
            .map(|d| d.name)
            .collect();
        assert_eq!(names, vec!["id".to_owned()])
    }

    #[test]
    fn test_map_to_columns() {
        let outer_schema = get_outer_schema();
        let columns = avro_schema_to_column_descs(
            avro_schema_skip_union(
                avro_extract_field_schema(&outer_schema, Some("before")).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
        .into_iter()
        .map(CatColumnDesc::from)
        .collect_vec();

        assert_eq!(columns.len(), 4);
        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Int32, "id", 1),
            columns[0]
        );

        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Varchar, "first_name", 2),
            columns[1]
        );

        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Varchar, "last_name", 3),
            columns[2]
        );

        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Varchar, "email", 4),
            columns[3]
        );
    }

    #[ignore]
    #[tokio::test]
    async fn test_debezium_avro_parser() -> Result<()> {
        let props = convert_args!(hashmap!(
            "kafka.topic" => "dbserver1.inventory.customers"
        ));
        let info = StreamSourceInfo {
            row_schema_location: "http://127.0.0.1:8081".into(),
            format: PbFormatType::Debezium.into(),
            row_encode: PbEncodeType::Avro.into(),
            ..Default::default()
        };
        let parser_config = SpecificParserConfig::new(&info, &props)?;
        let config = DebeziumAvroParserConfig::new(parser_config.clone().encoding_config).await?;
        let columns = config
            .map_to_columns()?
            .into_iter()
            .map(CatColumnDesc::from)
            .map(|c| SourceColumnDesc::from(&c))
            .collect_vec();
        let parser =
            DebeziumParser::new(parser_config, columns.clone(), Arc::new(Default::default()))
                .await?;
        let [(op, row)]: [_; 1] = parse_one(parser, columns, DEBEZIUM_AVRO_DATA.to_vec())
            .await
            .try_into()
            .unwrap();
        assert_eq!(op, Op::Insert);
        assert_eq!(row[0], Some(ScalarImpl::Int32(1001)));
        assert_eq!(row[1], Some(ScalarImpl::Utf8("Sally".into())));
        assert_eq!(row[2], Some(ScalarImpl::Utf8("Thomas".into())));
        assert_eq!(
            row[3],
            Some(ScalarImpl::Utf8("sally.thomas@acme.com".into()))
        );
        Ok(())
    }
}
