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

use std::fmt;
use std::hash::Hash;

use educe::Educe;
use risingwave_common::catalog::TableVersionId;

use crate::catalog::TableId;
use crate::expr::{ExprImpl, ExprRewriter};

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Update<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub exprs: Vec<ExprImpl>,
    pub returning: bool,
}

impl<PlanRef: Eq + Hash> Update<PlanRef> {
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        exprs: Vec<ExprImpl>,
        returning: bool,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_version_id,
            input,
            exprs,
            returning,
        }
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}, exprs: {:?}{} }}",
            name,
            self.table_name,
            self.exprs,
            if self.returning {
                ", returning: true"
            } else {
                ""
            }
        )
    }

    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.exprs = self
            .exprs
            .iter()
            .map(|e| r.rewrite_expr(e.clone()))
            .collect();
    }
}
