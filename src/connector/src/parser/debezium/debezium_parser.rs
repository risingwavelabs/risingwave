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

use std::collections::BTreeMap;

use risingwave_common::bail;

use super::simd_json_parser::DebeziumJsonAccessBuilder;
use super::{DebeziumAvroAccessBuilder, DebeziumAvroParserConfig};
use crate::error::ConnectorResult;
use crate::parser::unified::debezium::DebeziumChangeEvent;
use crate::parser::unified::json::TimestamptzHandling;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType, ParseResult,
    ParserFormat, ProtocolProperties, SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,

    props: DebeziumProps,
}

pub const DEBEZIUM_IGNORE_KEY: &str = "ignore_key";

#[derive(Debug, Clone, Default)]
pub struct DebeziumProps {
    // Ignore the key part of the message.
    // If enabled, we don't take the key part into message accessor.
    pub ignore_key: bool,
    // If enabled, we will treat the update event as a delete and an insert.
    pub handle_update_as_delete_insert: bool,
}

impl DebeziumProps {
    pub fn from(props: &BTreeMap<String, String>) -> Self {
        let ignore_key = props
            .get(DEBEZIUM_IGNORE_KEY)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        Self {
            ignore_key,
            handle_update_as_delete_insert: false,
        }
    }

    pub fn cdc_etl() -> Self {
        // cdc-etl job will always handle update as delete-insert
        // and `ignore_key` will not take effect.
        Self {
            ignore_key: false,
            handle_update_as_delete_insert: true,
        }
    }
}

async fn build_accessor_builder(
    config: EncodingProperties,
    encoding_type: EncodingType,
) -> ConnectorResult<AccessBuilderImpl> {
    match config {
        EncodingProperties::Avro(_) => {
            let config = DebeziumAvroParserConfig::new(config).await?;
            Ok(AccessBuilderImpl::DebeziumAvro(
                DebeziumAvroAccessBuilder::new(config, encoding_type)?,
            ))
        }
        EncodingProperties::Json(json_config) => Ok(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new(
                json_config
                    .timestamptz_handling
                    .unwrap_or(TimestamptzHandling::GuessNumberUnit),
            )?,
        )),
        _ => bail!("unsupported encoding for Debezium"),
    }
}

impl DebeziumParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let key_builder =
            build_accessor_builder(props.encoding_config.clone(), EncodingType::Key).await?;
        let payload_builder =
            build_accessor_builder(props.encoding_config, EncodingType::Value).await?;
        let debezium_props = if let ProtocolProperties::Debezium(props) = props.protocol_config {
            props
        } else {
            unreachable!(
                "expecting Debezium protocol properties but got {:?}",
                props.protocol_config
            )
        };
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
            props: debezium_props,
        })
    }

    pub async fn new_for_test(rw_columns: Vec<SourceColumnDesc>) -> ConnectorResult<Self> {
        use crate::parser::JsonProperties;

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        Self::new(props, rw_columns, SourceContext::dummy().into()).await
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<ParseResult> {
        let meta = writer.source_meta();
        // tombetone messages are handled implicitly by these accessors
        let key_accessor = match (key, self.props.ignore_key) {
            (None, false) => None,
            (Some(data), false) => Some(self.key_builder.generate_accessor(data, meta).await?),
            (_, true) => None,
        };
        let payload_accessor = match payload {
            None => None,
            Some(data) => Some(self.payload_builder.generate_accessor(data, meta).await?),
        };
        let row_op = DebeziumChangeEvent::new(key_accessor, payload_accessor);

        let res: Result<_, crate::parser::AccessError> = (|| {
            if self.props.handle_update_as_delete_insert && row_op.is_update()? {
                writer.do_delete(|column| row_op.access_before_field(column))?;
                writer.do_insert(|column| row_op.access_after_field(column))?;
            } else {
                apply_row_operation_on_stream_chunk_writer(&row_op, &mut writer)?;
            }
            Ok(ParseResult::DeleteInsertRows)
        })();

        match res {
            Ok(r) => Ok(r),
            Err(err) => {
                // Only try to access transaction control message if the row operation access failed
                // to make it a fast path.
                if let Some(transaction_control) =
                    row_op.transaction_control(&self.source_ctx.connector_props)
                {
                    Ok(ParseResult::TransactionControl(transaction_control))
                } else {
                    Err(err.into())
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
    ) -> ConnectorResult<()> {
        unreachable!("should call `parse_one_with_txn` instead")
    }

    async fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<ParseResult> {
        self.parse_inner(key, payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use risingwave_common::catalog::{CDC_SOURCE_COLUMN_NUM, ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, Timestamptz};
    use risingwave_pb::plan_common::{
        AdditionalColumn, AdditionalColumnTimestamp, additional_column,
    };

    use super::*;
    use crate::parser::{JsonProperties, SourceStreamChunkBuilder, TransactionControl};
    use crate::source::{ConnectorProperties, SourceCtrlOpts};

    #[tokio::test]
    async fn test_parse_transaction_metadata() {
        let schema = ColumnCatalog::debezium_cdc_source_cols();

        let columns = schema
            .iter()
            .map(|c| SourceColumnDesc::from(&c.column_desc))
            .collect::<Vec<_>>();

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::PostgresCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut dummy_builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        // "id":"35352:3962948040" Postgres transaction ID itself and LSN of given operation separated by colon, i.e. the format is txID:LSN
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"35352:3962948040","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"35352:3962950064","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;
        let res = parser
            .parse_one_with_txn(
                None,
                Some(begin_msg.as_bytes().to_vec()),
                dummy_builder.row_writer(),
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
                dummy_builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Commit { id })) => {
                assert_eq!(id.deref(), "35352");
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_parse_additional_columns() {
        let columns = vec![
            ColumnDesc::named("O_ORDERKEY", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("O_CUSTKEY", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("O_ORDERSTATUS", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("O_TOTALPRICE", ColumnId::new(4), DataType::Decimal),
            ColumnDesc::named("O_ORDERDATE", ColumnId::new(5), DataType::Date),
            ColumnDesc::named_with_additional_column(
                "commit_ts",
                ColumnId::new(6),
                DataType::Timestamptz,
                AdditionalColumn {
                    column_type: Some(additional_column::ColumnType::Timestamp(
                        AdditionalColumnTimestamp {},
                    )),
                },
            ),
        ];

        let columns = columns
            .iter()
            .map(SourceColumnDesc::from)
            .collect::<Vec<_>>();

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps::default()),
        };
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::PostgresCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        let payload = r#"{ "payload": { "before": null, "after": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" }, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null }, "op": "c", "ts_ms": 1695277757017, "transaction": null } }"#;

        let res = parser
            .parse_one_with_txn(
                None,
                Some(payload.as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await;
        match res {
            Ok(ParseResult::Rows) => {
                builder.finish_current_chunk();
                let chunk = builder.consume_ready_chunks().next().unwrap();
                for (_, row) in chunk.rows() {
                    let commit_ts = row.datum_at(5).unwrap().into_timestamptz();
                    assert_eq!(commit_ts, Timestamptz::from_millis(1695277757000).unwrap());
                }
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cdc_source_job_schema() {
        let columns = ColumnCatalog::debezium_cdc_source_cols();
        // make sure it doesn't broken by future PRs
        assert_eq!(CDC_SOURCE_COLUMN_NUM, columns.len() as u32);
    }

    #[tokio::test]
    async fn test_debezium_update_as_delete_insert() {
        let columns = vec![
            ColumnDesc::named("O_ORDERKEY", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("O_CUSTKEY", ColumnId::new(2), DataType::Int64),
            ColumnDesc::named("O_ORDERSTATUS", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("O_TOTALPRICE", ColumnId::new(4), DataType::Decimal),
            ColumnDesc::named("O_ORDERDATE", ColumnId::new(5), DataType::Date),
        ];

        let columns: Vec<SourceColumnDesc> = columns.iter().map(SourceColumnDesc::from).collect();

        let props = SpecificParserConfig {
            encoding_config: EncodingProperties::Json(JsonProperties {
                use_schema_registry: false,
                timestamptz_handling: None,
            }),
            protocol_config: ProtocolProperties::Debezium(DebeziumProps {
                ignore_key: false,
                handle_update_as_delete_insert: true,
            }),
        };
        let source_ctx = SourceContext {
            connector_props: ConnectorProperties::PostgresCdc(Box::default()),
            ..SourceContext::dummy()
        };
        let mut parser = DebeziumParser::new(props, columns.clone(), Arc::new(source_ctx))
            .await
            .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(columns, SourceCtrlOpts::for_test());

        let payload = r#"{ "payload": { "before": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "F", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" }, "after": { "O_ORDERKEY": 5, "O_CUSTKEY": 44485, "O_ORDERSTATUS": "O", "O_TOTALPRICE": "144659.20", "O_ORDERDATE": "1994-07-30" }, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002", "ts_ms": 1695277757000, "snapshot": "last", "db": "mydb", "sequence": null, "table": "orders_new", "server_id": 0, "gtid": null, "file": "binlog.000008", "pos": 3693, "row": 0, "thread": null, "query": null }, "op": "u", "ts_ms": 1695277757017, "transaction": null } }"#;

        let res = parser
            .parse_one_with_txn(
                None,
                Some(payload.as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await;

        assert!(matches!(res, Ok(ParseResult::Rows)));

        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
        let mut rows = chunk.rows();

        let (op, row) = rows.next().unwrap();
        assert_eq!(op, risingwave_common::array::Op::Delete);
        assert_eq!(row.datum_at(2).unwrap().into_utf8(), "F");

        let (op, row) = rows.next().unwrap();
        assert_eq!(op, risingwave_common::array::Op::Insert);
        assert_eq!(row.datum_at(2).unwrap().into_utf8(), "O");
    }
}
