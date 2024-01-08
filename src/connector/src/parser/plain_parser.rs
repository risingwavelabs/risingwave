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

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

use super::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::parser::bytes_parser::BytesAccessBuilder;
use crate::parser::simd_json_parser::DebeziumJsonAccessBuilder;
use crate::parser::unified::debezium::parse_transaction_meta;
use crate::parser::unified::upsert::UpsertChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer_with_op;
use crate::parser::unified::{AccessImpl, ChangeEventOperation};
use crate::parser::upsert_parser::get_key_column_name;
use crate::parser::{BytesProperties, ParseResult, ParserFormat};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef, SourceMeta};

#[derive(Debug)]
pub struct PlainParser {
    pub key_builder: Option<AccessBuilderImpl>,
    pub payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    pub source_ctx: SourceContextRef,
    // parsing transaction metadata for shared cdc source
    pub transaction_meta_builder: Option<AccessBuilderImpl>,
}

impl PlainParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let key_builder = if let Some(key_column_name) = get_key_column_name(&rw_columns) {
            Some(AccessBuilderImpl::Bytes(BytesAccessBuilder::new(
                EncodingProperties::Bytes(BytesProperties {
                    column_name: Some(key_column_name),
                }),
            )?))
        } else {
            None
        };

        let payload_builder = match props.encoding_config {
            EncodingProperties::Json(_)
            | EncodingProperties::Protobuf(_)
            | EncodingProperties::Avro(_)
            | EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::new_default(props.encoding_config, EncodingType::Value).await?
            }
            _ => {
                return Err(RwError::from(ProtocolError(
                    "unsupported encoding for Plain".to_string(),
                )));
            }
        };

        let transaction_meta_builder = Some(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new()?,
        ));
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
            transaction_meta_builder,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<ParseResult> {
        // if the message is transaction metadata, parse it and return
        if let Some(msg_meta) = writer.row_meta
            && let SourceMeta::DebeziumCdc(cdc_meta) = msg_meta.meta
            && cdc_meta.is_transaction_meta
            && let Some(data) = payload
        {
            let accessor = self
                .transaction_meta_builder
                .as_mut()
                .expect("expect transaction metadata access builder")
                .generate_accessor(data)
                .await?;
            return match parse_transaction_meta(&accessor, &self.source_ctx.connector_props) {
                Ok(transaction_control) => Ok(ParseResult::TransactionControl(transaction_control)),
                Err(err) => Err(err)?,
            };
        }

        // reuse upsert component but always insert
        let mut row_op: UpsertChangeEvent<AccessImpl<'_, '_>, AccessImpl<'_, '_>> =
            UpsertChangeEvent::default();
        let change_event_op = ChangeEventOperation::Upsert;

        if let Some(data) = key
            && let Some(key_builder) = self.key_builder.as_mut()
        {
            // key is optional in format plain
            row_op = row_op.with_key(key_builder.generate_accessor(data).await?);
        }
        if let Some(data) = payload {
            // the data part also can be an empty vec
            row_op = row_op.with_value(self.payload_builder.generate_accessor(data).await?);
        }

        Ok(
            apply_row_operation_on_stream_chunk_writer_with_op(
                row_op,
                &mut writer,
                change_event_op,
            )
            .map(|_| ParseResult::Rows)?,
        )
    }
}

impl ByteStreamSourceParser for PlainParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Plain
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        _payload: Option<Vec<u8>>,
        _writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<()> {
        unreachable!("should call `parse_one_with_txn` instead")
    }

    async fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<ParseResult> {
        tracing::info!("parse_one_with_txn");
        self.parse_inner(key, payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use futures::executor::block_on;
    use futures::StreamExt;
    use futures_async_stream::try_stream;
    use itertools::Itertools;
    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};

    use super::*;
    use crate::parser::{MessageMeta, SourceStreamChunkBuilder, TransactionControl};
    use crate::source::cdc::DebeziumCdcMeta;
    use crate::source::{ConnectorProperties, DataType, SourceMessage, SplitId};

    #[tokio::test]
    async fn test_emit_transactional_chunk() {
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

        let mut source_ctx = SourceContext::default();
        source_ctx.connector_props = ConnectorProperties::PostgresCdc(Box::default());
        let source_ctx = Arc::new(source_ctx);
        // format plain encode json parser
        let parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            source_ctx.clone(),
        )
        .await
        .unwrap();

        let mut transactional = false;
        // for untransactional source, we expect emit a chunk for each message batch
        let message_stream = source_message_stream(transactional);
        let chunk_stream = crate::parser::into_chunk_stream(parser, message_stream.boxed());
        let output: std::result::Result<Vec<_>, _> = block_on(chunk_stream.collect::<Vec<_>>())
            .into_iter()
            .collect();
        let output = output
            .unwrap()
            .into_iter()
            .filter(|c| c.chunk.cardinality() > 0)
            .enumerate()
            .map(|(i, c)| {
                if i == 0 {
                    // begin + 3 data messages
                    assert_eq!(4, c.chunk.cardinality());
                }
                if i == 1 {
                    // 2 data messages + 1 end
                    assert_eq!(3, c.chunk.cardinality());
                }
                c.chunk
            })
            .collect_vec();

        // 2 chunks for 2 message batches
        assert_eq!(2, output.len());

        // format plain encode json parser
        let parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            source_ctx,
        )
        .await
        .unwrap();

        // for transactional source, we expect emit a single chunk for the transaction
        transactional = true;
        let message_stream = source_message_stream(transactional);
        let chunk_stream = crate::parser::into_chunk_stream(parser, message_stream.boxed());
        let output: std::result::Result<Vec<_>, _> = block_on(chunk_stream.collect::<Vec<_>>())
            .into_iter()
            .collect();
        let output = output
            .unwrap()
            .into_iter()
            .filter(|c| c.chunk.cardinality() > 0)
            .map(|c| {
                // 5 data messages in a single chunk
                assert_eq!(5, c.chunk.cardinality());
                c.chunk
            })
            .collect_vec();

        // a single transactional chunk
        assert_eq!(1, output.len());
    }

    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn source_message_stream(transactional: bool) {
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"35352:3962948040","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"35352:3962950064","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;
        let data_batches = vec![
            vec![
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a1", "order_date": "2020-01-30", "order_id": 10021, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a2", "order_date": "2020-02-30", "order_id": 10022, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a3", "order_date": "2020-03-30", "order_id": 10023, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
            ],
            vec![
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a4", "order_date": "2020-04-30", "order_id": 10024, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
                r#"{ "schema": null, "payload": {"after": {"customer_name": "a5", "order_date": "2020-05-30", "order_id": 10025, "order_status": false, "price": "50.50", "product_id": 102}, "before": null, "op": "c", "source": {"connector": "postgresql", "db": "mydb", "lsn": 3963199336, "name": "RW_CDC_1001", "schema": "public", "sequence": "[\"3963198512\",\"3963199336\"]", "snapshot": "false", "table": "orders_tx", "ts_ms": 1704355505506, "txId": 35352, "version": "2.4.2.Final", "xmin": null}, "transaction": {"data_collection_order": 1, "id": "35392:3963199336", "total_order": 1}, "ts_ms": 1704355839905} }"#,
            ],
        ];
        for (i, batch) in data_batches.iter().enumerate() {
            let mut source_msg_batch = vec![];
            if i == 0 {
                // put begin message at first
                source_msg_batch.push(SourceMessage {
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta {
                        full_table_name: "orders".to_string(),
                        source_ts_ms: 0,
                        is_transaction_meta: transactional,
                    }),
                    split_id: SplitId::from("1001"),
                    offset: "0".into(),
                    key: None,
                    payload: Some(begin_msg.as_bytes().to_vec()),
                });
            }
            // put data messages
            for data_msg in batch {
                source_msg_batch.push(SourceMessage {
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta {
                        full_table_name: "orders".to_string(),
                        source_ts_ms: 0,
                        is_transaction_meta: false,
                    }),
                    split_id: SplitId::from("1001"),
                    offset: "0".into(),
                    key: None,
                    payload: Some(data_msg.as_bytes().to_vec()),
                });
            }
            if i == data_batches.len() - 1 {
                // put commit message at last
                source_msg_batch.push(SourceMessage {
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta {
                        full_table_name: "orders".to_string(),
                        source_ts_ms: 0,
                        is_transaction_meta: transactional,
                    }),
                    split_id: SplitId::from("1001"),
                    offset: "0".into(),
                    key: None,
                    payload: Some(commit_msg.as_bytes().to_vec()),
                });
            }
            yield source_msg_batch;
        }
    }

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

        // format plain encode json parser
        let mut source_ctx = SourceContext::default();
        source_ctx.connector_props = ConnectorProperties::MysqlCdc(Box::default());
        let mut parser = PlainParser::new(
            SpecificParserConfig::DEFAULT_PLAIN_JSON,
            columns.clone(),
            Arc::new(source_ctx),
        )
        .await
        .unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 0);

        // "id":"35352:3962948040" Postgres transaction ID itself and LSN of given operation separated by colon, i.e. the format is txID:LSN
        let begin_msg = r#"{"schema":null,"payload":{"status":"BEGIN","id":"3E11FA47-71CA-11E1-9E33-C80AA9429562:23","event_count":null,"data_collections":null,"ts_ms":1704269323180}}"#;
        let commit_msg = r#"{"schema":null,"payload":{"status":"END","id":"3E11FA47-71CA-11E1-9E33-C80AA9429562:23","event_count":11,"data_collections":[{"data_collection":"public.orders_tx","event_count":5},{"data_collection":"public.person","event_count":6}],"ts_ms":1704269323180}}"#;

        let cdc_meta = SourceMeta::DebeziumCdc(DebeziumCdcMeta {
            full_table_name: "orders".to_string(),
            source_ts_ms: 0,
            is_transaction_meta: true,
        });
        let msg_meta = MessageMeta {
            meta: &cdc_meta,
            split_id: "1001",
            offset: "",
        };

        let expect_tx_id = "3E11FA47-71CA-11E1-9E33-C80AA9429562:23";
        let res = parser
            .parse_one_with_txn(
                None,
                Some(begin_msg.as_bytes().to_vec()),
                builder.row_writer().with_meta(msg_meta),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Begin { id })) => {
                assert_eq!(id.deref(), expect_tx_id);
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }
        let res = parser
            .parse_one_with_txn(
                None,
                Some(commit_msg.as_bytes().to_vec()),
                builder.row_writer().with_meta(msg_meta),
            )
            .await;
        match res {
            Ok(ParseResult::TransactionControl(TransactionControl::Commit { id })) => {
                assert_eq!(id.deref(), expect_tx_id);
            }
            _ => panic!("unexpected parse result: {:?}", res),
        }

        let output = builder.take(10);
        assert_eq!(0, output.cardinality());
    }
}
