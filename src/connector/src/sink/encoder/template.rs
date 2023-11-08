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

use std::collections::HashSet;

use regex::Regex;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ToText;

use super::{Result, RowEncoder};
use crate::sink::SinkError;

/// Encode a row according to a specified string template `user_id:{user_id}`
pub struct TemplateEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    template: String,
}

/// todo! improve the performance.
impl TemplateEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>, template: String) -> Self {
        Self {
            schema,
            col_indices,
            template,
        }
    }

    pub fn check_string_format(format: &str, set: &HashSet<String>) -> Result<()> {
        // We will check if the string inside {} corresponds to a column name in rw.
        // In other words, the content within {} should exclusively consist of column names from rw,
        // which means '{{column_name}}' or '{{column_name1},{column_name2}}' would be incorrect.
        let re = Regex::new(r"\{([^}]*)\}").unwrap();
        if !re.is_match(format) {
            return Err(SinkError::Redis(
                "Can't find {} in key_format or value_format".to_string(),
            ));
        }
        for capture in re.captures_iter(format) {
            if let Some(inner_content) = capture.get(1)
                && !set.contains(inner_content.as_str())
            {
                return Err(SinkError::Redis(format!(
                    "Can't find field({:?}) in key_format or value_format",
                    inner_content.as_str()
                )));
            }
        }
        Ok(())
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
