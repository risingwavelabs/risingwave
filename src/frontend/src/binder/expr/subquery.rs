// Copyright 2025 RisingWave Labs
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

use risingwave_sqlparser::ast::Query;

use crate::binder::Binder;
use crate::error::{Result, bail_bind_error};
use crate::expr::{ExprImpl, Subquery, SubqueryKind};

impl Binder {
    pub fn bind_subquery_expr(&mut self, query: &Query, kind: SubqueryKind) -> Result<ExprImpl> {
        let query = self.bind_query(query)?;
        if !matches!(kind, SubqueryKind::Existential | SubqueryKind::UpdateSet)
            && query.data_types().len() != 1
        {
            bail_bind_error!("Subquery must return only one column");
        }
        Ok(Subquery::new(query, kind).into())
    }
}
