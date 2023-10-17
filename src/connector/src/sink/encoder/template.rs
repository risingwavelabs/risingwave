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

use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ToText;

use super::{JsonEncoder, Result, RowEncoder};
use crate::sink::SinkError;

pub enum RedisFormatEncoder {
    Json(JsonEncoder),
    Template(TemplateEncoder),
}
impl RowEncoder for RedisFormatEncoder {
    type Output = String;

    fn encode_cols(
        &self,
        row: impl risingwave_common::row::Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        match self {
            RedisFormatEncoder::Json(json) => {
                Ok(serde_json::to_string(&json.encode_cols(row, col_indices)?)
                    .map_err(|err| SinkError::Encode(err.to_string()))?)
            }
            RedisFormatEncoder::Template(template) => template.encode_cols(row, col_indices),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            RedisFormatEncoder::Json(json) => json.schema(),
            RedisFormatEncoder::Template(template) => template.schema(),
        }
    }

    fn col_indices(&self) -> Option<&[usize]> {
        match self {
            RedisFormatEncoder::Json(json) => json.col_indices(),
            RedisFormatEncoder::Template(template) => template.col_indices(),
        }
    }
}

/// Encode a row according to a specified string template `user_id:{user_id}`
pub struct TemplateEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    template: String,
}

impl TemplateEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>, template: String) -> Self {
        Self {
            schema,
            col_indices,
            template,
        }
    }
}

impl RowEncoder for TemplateEncoder {
    type Output = String;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_ref().map(Vec::as_ref)
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        let mut s = self.template.to_string();

        for idx in col_indices {
            let field = &self.schema[idx];
            let name = &field.name;
            let data = row.datum_at(idx);
            // TODO: timestamptz ToText also depends on TimeZone
            s = s.replace(
                &format!("{{{}}}", name),
                &data.to_text_with_type(&field.data_type),
            );
        }
        Ok(s)
    }
}
