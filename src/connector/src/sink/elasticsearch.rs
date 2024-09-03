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

use std::collections::BTreeMap;

use anyhow::anyhow;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, JsonbArrayBuilder, RowRef, StreamChunk, Utf8ArrayBuilder,
};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{JsonbVal, Scalar, ToText};
use serde_json::Value;

use super::encoder::{JsonEncoder, RowEncoder};
use super::remote::{ElasticSearchSink, OpenSearchSink};
use crate::sink::{Result, Sink};
pub const ES_OPTION_DELIMITER: &str = "delimiter";
pub const ES_OPTION_INDEX_COLUMN: &str = "index_column";

pub enum StreamChunkConverter {
    Es(EsStreamChunkConverter),
    Other,
}
impl StreamChunkConverter {
    pub fn new(
        sink_name: &str,
        schema: Schema,
        pk_indices: &Vec<usize>,
        properties: &BTreeMap<String, String>,
    ) -> Result<Self> {
        if is_es_sink(sink_name) {
            let index_column = properties
                .get(ES_OPTION_INDEX_COLUMN)
                .cloned()
                .map(|n| {
                    schema
                        .fields()
                        .iter()
                        .position(|s| s.name == n)
                        .ok_or_else(|| anyhow!("Cannot find {}", ES_OPTION_INDEX_COLUMN))
                })
                .transpose()?;
            Ok(StreamChunkConverter::Es(EsStreamChunkConverter::new(
                schema,
                pk_indices.clone(),
                properties.get(ES_OPTION_DELIMITER).cloned(),
                index_column,
            )?))
        } else {
            Ok(StreamChunkConverter::Other)
        }
    }

    pub fn convert_chunk(&self, chunk: StreamChunk) -> Result<StreamChunk> {
        match self {
            StreamChunkConverter::Es(es) => es.convert_chunk(chunk),
            StreamChunkConverter::Other => Ok(chunk),
        }
    }
}
pub struct EsStreamChunkConverter {
    json_encoder: JsonEncoder,
    fn_build_id: Box<dyn Fn(RowRef<'_>) -> Result<String> + Send>,
    index_column: Option<usize>,
}
impl EsStreamChunkConverter {
    fn new(
        schema: Schema,
        pk_indices: Vec<usize>,
        delimiter: Option<String>,
        index_column: Option<usize>,
    ) -> Result<Self> {
        let fn_build_id: Box<dyn Fn(RowRef<'_>) -> Result<String> + Send> = if pk_indices.is_empty()
        {
            Box::new(|row: RowRef<'_>| {
                Ok(row
                    .datum_at(0)
                    .ok_or_else(|| anyhow!("No value found in row, index is 0"))?
                    .to_text())
            })
        } else if pk_indices.len() == 1 {
            let index = *pk_indices.get(0).unwrap();
            Box::new(move |row: RowRef<'_>| {
                Ok(row
                    .datum_at(index)
                    .ok_or_else(|| anyhow!("No value found in row, index is 0"))?
                    .to_text())
            })
        } else {
            let delimiter = delimiter
                .as_ref()
                .ok_or_else(|| anyhow!("Please set delimiter in with option"))?
                .clone();
            Box::new(move |row: RowRef<'_>| {
                let mut keys = vec![];
                for index in &pk_indices {
                    keys.push(
                        row.datum_at(*index)
                            .ok_or_else(|| anyhow!("No value found in row, index is {}", index))?
                            .to_text(),
                    );
                }
                Ok(keys.join(&delimiter))
            })
        };
        let col_indices = if let Some(index) = index_column {
            let mut col_indices: Vec<usize> = (0..schema.len()).collect();
            col_indices.remove(index);
            Some(col_indices)
        } else {
            None
        };
        let json_encoder = JsonEncoder::new_with_es(schema, col_indices);
        Ok(Self {
            json_encoder,
            fn_build_id,
            index_column,
        })
    }

    fn convert_chunk(&self, chunk: StreamChunk) -> Result<StreamChunk> {
        let mut ops = vec![];
        let mut id_string_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut json_builder =
            <JsonbArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut index_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        for (op, row) in chunk.rows() {
            ops.push(op);
            id_string_builder.append(Some(&self.build_id(row)?));
            if let Some(index) = self.index_column {
                index_builder.append(Some(
                    row.datum_at(index)
                        .ok_or_else(|| anyhow!("No value found in row, index is {}", index))?
                        .into_utf8(),
                ));
            } else {
                index_builder.append_null();
            }
            let json = JsonbVal::from(Value::Object(self.json_encoder.encode(row)?));
            json_builder.append(Some(json.as_scalar_ref()));
        }
        let json_array = risingwave_common::array::ArrayBuilder::finish(json_builder);
        let id_string_array = risingwave_common::array::ArrayBuilder::finish(id_string_builder);
        let index_string_array = risingwave_common::array::ArrayBuilder::finish(index_builder);
        Ok(StreamChunk::new(
            ops,
            vec![
                std::sync::Arc::new(ArrayImpl::Utf8(index_string_array)),
                std::sync::Arc::new(ArrayImpl::Utf8(id_string_array)),
                std::sync::Arc::new(ArrayImpl::Jsonb(json_array)),
            ],
        ))
    }

    fn build_id(&self, row: RowRef<'_>) -> Result<String> {
        (self.fn_build_id)(row)
    }
}

pub fn is_es_sink(sink_name: &str) -> bool {
    sink_name == ElasticSearchSink::SINK_NAME || sink_name == OpenSearchSink::SINK_NAME
}
