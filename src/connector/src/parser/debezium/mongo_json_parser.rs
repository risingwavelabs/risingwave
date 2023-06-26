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

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use simd_json::{BorrowedValue, Mutable};

use crate::parser::unified::debezium::{DebeziumChangeEvent, MongoProjeciton};
use crate::parser::unified::json::{JsonAccess, JsonParseOptions};
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{ByteStreamSourceParser, SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumMongoJsonParser {
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    id_column: SourceColumnDesc,
    payload_column: SourceColumnDesc,
    source_ctx: SourceContextRef,
}

impl DebeziumMongoJsonParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Result<Self> {
        let id_column = rw_columns
            .iter()
            .find(|desc| {
                desc.name == "_id"
                    && matches!(
                        desc.data_type,
                        DataType::Jsonb
                            | DataType::Varchar
                            | DataType::Int32
                            | DataType::Int64
                    )
            })
            .ok_or_else(|| RwError::from(ProtocolError(
                "Debezuim Mongo needs a `_id` column with supported types (Varchar Jsonb int32 int64) in table".into(),
            )))?.clone();
        let payload_column = rw_columns
            .iter()
            .find(|desc| desc.name == "payload" && matches!(desc.data_type, DataType::Jsonb))
            .ok_or_else(|| {
                RwError::from(ProtocolError(
                    "Debezuim Mongo needs a `payload` column with supported types Jsonb in table"
                        .into(),
                ))
            })?
            .clone();

        if rw_columns.len() != 2 {
            return Err(RwError::from(ProtocolError(
                "Debezuim Mongo needs no more columns except `_id` and `payload` in table".into(),
            )));
        }

        Ok(Self {
            rw_columns,
            id_column,
            payload_column,

            source_ctx,
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        mut payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let mut event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        // Event can be configured with and without the "payload" field present.
        // See https://github.com/risingwavelabs/risingwave/issues/10178

        let payload = if let Some(payload) = event.get_mut("payload") {
            std::mem::take(payload)
        } else {
            event
        };

        let accessor = JsonAccess::new_with_options(payload, &JsonParseOptions::DEBEZIUM);

        let row_op = DebeziumChangeEvent::with_value(MongoProjeciton::new(accessor));

        apply_row_operation_on_stream_chunk_writer(row_op, &mut writer)
    }
}

impl ByteStreamSourceParser for DebeziumMongoJsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        payload: Vec<u8>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Op;
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::row::Row;
    use risingwave_common::types::{ScalarImpl, ToOwnedDatum};

    use super::*;
    use crate::parser::unified::debezium::extract_bson_id;
    use crate::parser::SourceStreamChunkBuilder;
    #[test]
    fn test_parse_bson_value_id_int() {
        let data = r#"{"_id":{"$numberInt":"2345"}}"#;
        let pld: serde_json::Value = serde_json::from_str(data).unwrap();
        let a = extract_bson_id(&DataType::Int32, &pld).unwrap();
        assert_eq!(a, Some(ScalarImpl::Int32(2345)));
    }
    #[test]
    fn test_parse_bson_value_id_long() {
        let data = r#"{"_id":{"$numberLong":"22423434544"}}"#;
        let pld: serde_json::Value = serde_json::from_str(data).unwrap();

        let a = extract_bson_id(&DataType::Int64, &pld).unwrap();
        assert_eq!(a, Some(ScalarImpl::Int64(22423434544)));
    }

    #[test]
    fn test_parse_bson_value_id_oid() {
        let data = r#"{"_id":{"$oid":"5d505646cf6d4fe581014ab2"}}"#;
        let pld: serde_json::Value = serde_json::from_str(data).unwrap();
        let a = extract_bson_id(&DataType::Varchar, &pld).unwrap();
        assert_eq!(a, Some(ScalarImpl::Utf8("5d505646cf6d4fe581014ab2".into())));
    }
    fn get_columns() -> Vec<SourceColumnDesc> {
        let descs = vec![
            SourceColumnDesc::simple("_id", DataType::Int64, ColumnId::from(0)),
            SourceColumnDesc::simple("payload", DataType::Jsonb, ColumnId::from(1)),
        ];

        descs
    }

    #[tokio::test]
    async fn test_long_id() {
        let input = vec![
            // data with payload and schema field
            br#"{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"before"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"removedFields"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"updatedFields"},{"type":"array","items":{"type":"struct","fields":[{"type":"string","optional":false,"field":"field"},{"type":"int32","optional":false,"field":"size"}],"optional":false,"name":"io.debezium.connector.mongodb.changestream.truncatedarray","version":1},"optional":true,"field":"truncatedArrays"}],"optional":true,"name":"io.debezium.connector.mongodb.changestream.updatedescription","version":1,"field":"updateDescription"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"string","optional":true,"field":"lsid"},{"type":"int64","optional":true,"field":"txnNumber"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"name":"event.block","version":1,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.customers.Envelope"},"payload":{"before":null,"after":"{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}","patch":null,"filter":null,"updateDescription":null,"source":{"version":"2.1.4.Final","connector":"mongodb","name":"dbserver1","ts_ms":1681879044000,"snapshot":"last","db":"inventory","sequence":null,"rs":"rs0","collection":"customers","ord":1,"lsid":null,"txnNumber":null},"op":"r","ts_ms":1681879054736,"transaction":null}}"#.to_vec(),
            // data without payload and schema field
            br#"{"before":null,"after":"{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}","patch":null,"filter":null,"updateDescription":null,"source":{"version":"2.1.4.Final","connector":"mongodb","name":"dbserver1","ts_ms":1681879044000,"snapshot":"last","db":"inventory","sequence":null,"rs":"rs0","collection":"customers","ord":1,"lsid":null,"txnNumber":null},"op":"r","ts_ms":1681879054736,"transaction":null}"#.to_vec()];

        let columns = get_columns();
        for data in input {
            let parser = DebeziumMongoJsonParser::new(columns.clone(), Default::default()).unwrap();

            let mut builder = SourceStreamChunkBuilder::with_capacity(columns.clone(), 3);

            let writer = builder.row_writer();
            parser.parse_inner(data, writer).await.unwrap();
            let chunk = builder.finish();
            let mut rows = chunk.rows();
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);

            assert_eq!(
            row.datum_at(1).to_owned_datum(),
            (Some(ScalarImpl::Jsonb(
                serde_json::json!({"_id": {"$numberLong": "1004"},"first_name": "Anne","last_name": "Kretchmar","email": "annek@noanswer.org"}).into()
            )))
        );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int64(1004)))
            );
        }
    }
}
