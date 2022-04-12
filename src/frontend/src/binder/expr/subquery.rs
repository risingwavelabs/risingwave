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
//
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Query;

use crate::binder::Binder;
use crate::expr::{ExprImpl, Subquery, SubqueryKind};

impl Binder {
    pub(super) fn bind_subquery_expr(
        &mut self,
        query: Query,
        kind: SubqueryKind,
    ) -> Result<ExprImpl> {
        let r = self.bind_query(query);
        if let Ok(query) = r {
            // uncorrelated subquery
            if kind == SubqueryKind::Scalar && query.data_types().len() != 1 {
                return Err(ErrorCode::BindError(
                    "subquery must return only one column".to_string(),
                )
                .into());
            }
            return Ok(Subquery::new(query, kind).into());
        }

        Err(ErrorCode::NotImplemented("correlated subquery".to_string(), 1343.into()).into())
    }
}
