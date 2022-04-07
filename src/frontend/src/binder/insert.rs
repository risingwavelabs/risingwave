// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SetExpr};

use super::{BoundQuery, BoundSetExpr};
use crate::binder::{Binder, BoundTableSource};

#[derive(Debug)]
pub struct BoundInsert {
    /// Used for injecting deletion chunks to the source.
    pub table_source: BoundTableSource,

    pub source: BoundQuery,
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        source_name: ObjectName,
        _columns: Vec<Ident>,
        source: Query,
    ) -> Result<BoundInsert> {
        let table_source = self.bind_table_source(source_name)?;

        let limit = source.get_limit_value();
        let offset = source.get_offset_value();
        let expected_types = table_source
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        let source = match source.body {
            SetExpr::Values(values) => {
                let values = self.bind_values(values, Some(expected_types))?;
                let body = BoundSetExpr::Values(values.into());
                BoundQuery {
                    body,
                    order: vec![],
                    limit,
                    offset,
                }
            }

            // TODO: insert type cast for select exprs
            SetExpr::Select(_) => self.bind_query(source)?,

            _ => {
                return Err(
                    ErrorCode::NotImplemented(format!("{:?}", source.body), None.into()).into(),
                )
            }
        };

        let insert = BoundInsert {
            table_source,
            source,
        };

        Ok(insert)
    }
}
