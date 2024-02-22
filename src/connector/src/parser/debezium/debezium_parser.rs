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

use risingwave_common::bail;

use super::simd_json_parser::DebeziumJsonAccessBuilder;
use super::{DebeziumAvroAccessBuilder, DebeziumAvroParserConfig};
use crate::extract_key_config;
use crate::parser::unified::debezium::DebeziumChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType, JsonProperties,
    ParseResult, ParserFormat, ProtocolProperties, SourceStreamChunkRowWriter,
    SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

async fn build_accessor_builder(
    config: EncodingProperties,
    encoding_type: EncodingType,
) -> anyhow::Result<AccessBuilderImpl> {
    match config {
        EncodingProperties::Avro(_) => {
            let config = DebeziumAvroParserConfig::new(config).await?;
            Ok(AccessBuilderImpl::DebeziumAvro(
                DebeziumAvroAccessBuilder::new(config, encoding_type)?,
            ))
        }
        EncodingProperties::Json(_) => Ok(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new()?,
        )),
        EncodingProperties::Protobuf(_) => {
            Ok(AccessBuilderImpl::new_default(config, encoding_type).await?)
        }
        _ => bail!("unsupported encoding for Debezium"),
    }
}

impl DebeziumParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> anyhow::Result<Self> {
        let (key_config, key_type) = extract_key_config!(props);
        let key_builder = build_accessor_builder(key_config, key_type).await?;
        let payload_builder =
            build_accessor_builder(props.encoding_config, EncodingType::Value).await?;
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
        })
    }

    pub async fn new_for_test(rw_columns: Vec<SourceColumnDesc>) -> anyhow::Result<Self> {
        let props = SpecificParserConfig {
            key_encoding_config: None,
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
            }),
            protocol_config: ProtocolProperties::Debezium,
        };
        Self::new(props, rw_columns, Default::default()).await
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> anyhow::Result<ParseResult> {
        // tombetone messages are handled implicitly by these accessors
        let key_accessor = match key {
            None => None,
            Some(data) => Some(self.key_builder.generate_accessor(data).await?),
        };
        let payload_accessor = match payload {
            None => None,
            Some(data) => Some(self.payload_builder.generate_accessor(data).await?),
        };
        let row_op = DebeziumChangeEvent::new(key_accessor, payload_accessor);

        match apply_row_operation_on_stream_chunk_writer(&row_op, &mut writer) {
            Ok(_) => Ok(ParseResult::Rows),
            Err(err) => {
                // Only try to access transaction control message if the row operation access failed
                // to make it a fast path.
                if let Some(transaction_control) =
                    row_op.transaction_control(&self.source_ctx.connector_props)
                {
                    Ok(ParseResult::TransactionControl(transaction_control))
                } else {
                    Err(err)?
                }
            }
        }
    }
}

impl ByteStreamSourceParser for DebeziumParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Debezium
    }

    #[allow(clippy::unused_async)] // false positive for `async_trait`
    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        _payload: Option<Vec<u8>>,
        _writer: SourceStreamChunkRowWriter<'a>,
    ) -> anyhow::Result<()> {
        unreachable!("should call `parse_one_with_txn` instead")
    }

    async fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> anyhow::Result<ParseResult> {
        self.parse_inner(key, payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};

    use super::*;
    use crate::parser::{SourceStreamChunkBuilder, TransactionControl};
    use crate::source::{ConnectorProperties, DataType};

    #[tokio::test]
    async fn test_parse_transaction_metadata() {
        let schema = vec![
            ColumnCatalog {
                column_desc: ColumnDesc::named("payload", ColumnId::placeholder(), DataType::Jsonb),
                is_hidden: false,
            },
            ColumnCatalog::offset_column(),
            ColumnCatalog::cdc_table_name_column(),
        ];

        let columns = schema
            .iter()
            .map(|c| SourceColumnDesc::from(&c.column_desc))
            .collect::<Vec<_>>();

        let props = SpecificParserConfig {
            key_encoding_config: None,
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
            }),
            protocol_config: ProtocolProperties::Debezium,
        };
        let mut source_ctx = SourceContext::default();
        source_ctx.connector_props = ConnectorProperties::PostgresCdc(Box::default());
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 0);

        // "id":"35352:3962948040" Postgres transaction ID itself and LSN of given operation separated by colon, i.e. the format is txID:LSN
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"35352:3962948040","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"35352:3962950064","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;
        let res = parser
            .parse_one_with_txn(
                None,
                Some(begin_msg.as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Begin { id })) => {
                assert_eq!(id.deref(), "35352");
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
        let res = parser
            .parse_one_with_txn(
                None,
                Some(commit_msg.as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Commit { id })) => {
                assert_eq!(id.deref(), "35352");
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
    }
}
