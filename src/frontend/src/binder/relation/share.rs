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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};

use crate::binder::statement::RewriteExprsRecursive;
use crate::binder::{BoundQuery, Relation, ShareId};
use crate::error::{ErrorCode, Result};
use crate::optimizer::plan_node::generic::{_CHANGELOG_ROW_ID, CHANGELOG_OP};

/// Share a relation during binding and planning.
/// It could be used to share a (recursive) CTE, a source, a view and so on.

#[derive(Debug, Clone)]
pub enum BoundShareInput {
    Query(BoundQuery),
    ChangeLog(Relation),
    /// A recursive CTE with anchor and recursive parts.
    RecursiveCte {
        anchor: BoundQuery,
        recursive: BoundQuery,
    },
}
impl BoundShareInput {
    pub fn fields(&self) -> Result<Vec<(bool, Field)>> {
        match self {
            BoundShareInput::Query(q) => Ok(q
                .schema()
                .fields()
                .iter()
                .cloned()
                .map(|f| (false, f))
                .collect_vec()),
            BoundShareInput::RecursiveCte { anchor, .. } => Ok(anchor
                .schema()
                .fields()
                .iter()
                .cloned()
                .map(|f| (false, f))
                .collect_vec()),
            BoundShareInput::ChangeLog(r) => {
                let (fields, _name) = if let Relation::BaseTable(bound_base_table) = r {
                    (
                        bound_base_table.table_catalog.columns().to_vec(),
                        bound_base_table.table_catalog.name().to_owned(),
                    )
                } else if let Relation::Source(bound_source) = r {
                    (
                        bound_source.catalog.columns.clone(),
                        bound_source.catalog.name.clone(),
                    )
                } else {
                    return Err(ErrorCode::BindError(
                        "Change log CTE must be a table or source".to_owned(),
                    )
                    .into());
                };
                let fields = fields
                    .into_iter()
                    .map(|x| {
                        (
                            x.is_hidden,
                            Field::with_name(x.data_type().clone(), x.name()),
                        )
                    })
                    .chain(vec![
                        (
                            false,
                            Field::with_name(
                                risingwave_common::types::DataType::Int16,
                                CHANGELOG_OP.to_owned(),
                            ),
                        ),
                        (
                            true,
                            Field::with_name(
                                risingwave_common::types::DataType::Serial,
                                _CHANGELOG_ROW_ID.to_owned(),
                            ),
                        ),
                    ])
                    .collect();
                Ok(fields)
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct BoundShare {
    pub(crate) share_id: ShareId,
    pub(crate) input: BoundShareInput,
}

impl RewriteExprsRecursive for BoundShare {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        match &mut self.input {
            BoundShareInput::Query(q) => q.rewrite_exprs_recursive(rewriter),
            BoundShareInput::ChangeLog(r) => r.rewrite_exprs_recursive(rewriter),
            BoundShareInput::RecursiveCte { anchor, recursive } => {
                anchor.rewrite_exprs_recursive(rewriter);
                recursive.rewrite_exprs_recursive(rewriter);
            }
        };
    }
}

/// A reference back to a recursive CTE that is currently being defined.
/// This is used inside the recursive branch of the CTE to refer to the CTE's working table.
#[derive(Debug, Clone)]
pub struct BoundBackCteRef {
    #[allow(dead_code)]
    pub(crate) share_id: ShareId,
    /// Schema of the CTE (from the anchor query).
    #[allow(dead_code)]
    pub(crate) schema: Schema,
}
