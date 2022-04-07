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
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::SetExpr;

use crate::binder::{Binder, BoundSelect, BoundValues};

/// Part of a validated query, without order or limit clause. It may be composed of smaller
/// `BoundSetExpr`s via set operators (e.g. union).
#[derive(Debug)]
pub enum BoundSetExpr {
    Select(Box<BoundSelect>),
    Values(Box<BoundValues>),
}

impl BoundSetExpr {
    /// The names returned by this [`BoundSetExpr`].
    pub fn names(&self) -> Vec<String> {
        match self {
            BoundSetExpr::Select(s) => s.names(),
            BoundSetExpr::Values(v) => v.schema.fields().iter().map(|f| f.name.clone()).collect(),
        }
    }

    /// The types returned by this [`BoundSetExpr`].
    pub fn data_types(&self) -> Vec<DataType> {
        match self {
            BoundSetExpr::Select(s) => s.data_types(),
            BoundSetExpr::Values(v) => v
                .schema
                .fields()
                .iter()
                .map(|f| f.data_type.clone())
                .collect(),
        }
    }

    pub fn is_correlated(&self) -> bool {
        match self {
            BoundSetExpr::Select(s) => s.is_correlated(),
            BoundSetExpr::Values(_) => false,
        }
    }
}

impl Binder {
    pub(super) fn bind_set_expr(&mut self, set_expr: SetExpr) -> Result<BoundSetExpr> {
        match set_expr {
            SetExpr::Select(s) => Ok(BoundSetExpr::Select(Box::new(self.bind_select(*s)?))),
            SetExpr::Values(v) => Ok(BoundSetExpr::Values(Box::new(self.bind_values(v, None)?))),
            _ => Err(ErrorCode::NotImplemented(format!("{:?}", set_expr), None.into()).into()),
        }
    }
}
