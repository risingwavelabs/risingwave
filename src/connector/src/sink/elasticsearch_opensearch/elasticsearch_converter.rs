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

use super::elasticsearch_opensearch_config::{
    ES_OPTION_DELIMITER, ES_OPTION_INDEX, ES_OPTION_INDEX_COLUMN, ES_OPTION_ROUTING_COLUMN,
};
use super::elasticsearch_opensearch_formatter::{BuildBulkPara, ElasticSearchOpenSearchFormatter};
use crate::sink::Result;

#[expect(clippy::large_enum_variant)]
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
        is_append_only: bool,
    ) -> Result<Self> {
        if is_remote_es_sink(sink_name) {
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
            let routing_column = properties
                .get(ES_OPTION_ROUTING_COLUMN)
                .cloned()
                .map(|n| {
                    schema
                        .fields()
                        .iter()
                        .position(|s| s.name == n)
                        .ok_or_else(|| anyhow!("Cannot find {}", ES_OPTION_ROUTING_COLUMN))
                })
                .transpose()?;
            Ok(StreamChunkConverter::Es(EsStreamChunkConverter::new(
                schema,
                pk_indices.clone(),
                properties.get(ES_OPTION_DELIMITER).cloned(),
                index_column,
                index,
                routing_column,
                is_append_only,
            )?))
        } else {
            Ok(StreamChunkConverter::Other)
        }
    }

    pub fn convert_chunk(&self, chunk: StreamChunk) -> Result<StreamChunk> {
        match self {
            StreamChunkConverter::Es(es) => es.convert_chunk(chunk, es.is_append_only),
            StreamChunkConverter::Other => Ok(chunk),
        }
    }
}
pub struct EsStreamChunkConverter {
    formatter: ElasticSearchOpenSearchFormatter,
    is_append_only: bool,
}
impl EsStreamChunkConverter {
    pub fn new(
        schema: Schema,
        pk_indices: Vec<usize>,
        delimiter: Option<String>,
        index_column: Option<usize>,
        index: Option<String>,
        routing_column: Option<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let formatter = ElasticSearchOpenSearchFormatter::new(
            pk_indices,
            &schema,
            delimiter,
            index_column,
            index,
            routing_column,
        )?;
        Ok(Self {
            formatter,
            is_append_only,
        })
    }

    fn convert_chunk(&self, chunk: StreamChunk, is_append_only: bool) -> Result<StreamChunk> {
        let mut ops = Vec::with_capacity(chunk.capacity());
        let mut id_string_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut json_builder =
            <JsonbArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut index_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        let mut routing_builder =
            <Utf8ArrayBuilder as risingwave_common::array::ArrayBuilder>::new(chunk.capacity());
        for build_bulk_para in self.formatter.convert_chunk(chunk, is_append_only)? {
            let BuildBulkPara {
                key,
                value,
                index,
                routing_column,
                ..
            } = build_bulk_para;

            id_string_builder.append(Some(&key));
            index_builder.append(Some(&index));
            routing_builder.append(routing_column.as_deref());
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
        let routing_string_array = risingwave_common::array::ArrayBuilder::finish(routing_builder);
        Ok(StreamChunk::new(
            ops,
            vec![
                std::sync::Arc::new(ArrayImpl::Utf8(index_string_array)),
                std::sync::Arc::new(ArrayImpl::Utf8(id_string_array)),
                std::sync::Arc::new(ArrayImpl::Jsonb(json_array)),
                std::sync::Arc::new(ArrayImpl::Utf8(routing_string_array)),
            ],
        ))
    }
}

pub fn is_remote_es_sink(_sink_name: &str) -> bool {
    // sink_name == ElasticSearchJavaSink::SINK_NAME || sink_name == OpenSearchJavaSink::SINK_NAME
    false
}
