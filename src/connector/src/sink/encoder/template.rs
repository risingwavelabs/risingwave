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

use super::{Result, RowEncoder};

/// Encode a row according to a specified string template `user_id:{user_id}`
pub struct TemplateEncoder<'a> {
    schema: &'a Schema,
    col_indices: Option<&'a [usize]>,
    template: &'a str,
}

impl<'a> TemplateEncoder<'a> {
    pub fn new(schema: &'a Schema, col_indices: Option<&'a [usize]>, template: &'a str) -> Self {
        Self {
            schema,
            col_indices,
            template,
        }
    }
}

impl<'a> RowEncoder for TemplateEncoder<'a> {
    type Output = String;

    fn schema(&self) -> &Schema {
        self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices
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
