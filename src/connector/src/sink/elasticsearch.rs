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
    ArrayBuilder, ArrayImpl, JsonbArrayBuilder, StreamChunk, Utf8ArrayBuilder,
};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{JsonbVal, Scalar};
use serde_json::Value;

use super::elasticsearch_opensearch_common::{BuildBulkPara, ElasticSearchOpenSearchFormatter};
use super::remote::{ElasticSearchSink, OpenSearchSink};
use crate::sink::{Result, Sink};
pub const ES_OPTION_DELIMITER: &str = "delimiter";
pub const ES_OPTION_INDEX_COLUMN: &str = "index_column";
pub const ES_OPTION_INDEX: &str = "index";

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
            let index = properties.get(ES_OPTION_INDEX).cloned();
            Ok(StreamChunkConverter::Es(EsStreamChunkConverter::new(
                schema,
                pk_indices.clone(),
                properties.get(ES_OPTION_DELIMITER).cloned(),
                index_column,
                index,
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
    formatter: ElasticSearchOpenSearchFormatter,
}
impl EsStreamChunkConverter {
    pub fn new(
        schema: Schema,
        pk_indices: Vec<usize>,
        delimiter: Option<String>,
        index_column: Option<usize>,
        index: Option<String>,
    ) -> Result<Self> {
        let formatter = ElasticSearchOpenSearchFormatter::new(
            pk_indices,
            &schema,
            delimiter,
            index_column,
            index,
        )?;
        Ok(Self { formatter })
    }

    fn convert_chunk(&self, chunk: StreamChunk) -> Result<StreamChunk> {
        let mut ops = Vec::with_capacity(chunk.capacity());
        let mut id_string_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut json_builder =
            <JsonbArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut index_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        for build_bulk_para in self.formatter.covert_chunk(chunk)? {
            let BuildBulkPara {
                key, value, index, ..
            } = build_bulk_para;

            id_string_builder.append(Some(&key));
            index_builder.append(Some(&index));
            if value.is_some() {
                ops.push(risingwave_common::array::Op::Insert);
            } else {
                ops.push(risingwave_common::array::Op::Delete);
            }
            let value = value.map(|json| JsonbVal::from(Value::Object(json)));
            json_builder.append(value.as_ref().map(|json| json.as_scalar_ref()));
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
}

pub fn is_es_sink(sink_name: &str) -> bool {
    sink_name == ElasticSearchSink::SINK_NAME || sink_name == OpenSearchSink::SINK_NAME
}
