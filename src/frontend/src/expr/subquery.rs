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

use std::hash::Hash;

use risingwave_common::types::{DataType, StructType};

use super::{Expr, ExprImpl, ExprType};
use crate::binder::{BoundQuery, UNNAMED_COLUMN};
use crate::expr::{CorrelatedId, Depth};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubqueryKind {
    /// Returns a scalar value (single column single row).
    Scalar,
    /// Returns a scalar struct value composed of multiple columns.
    /// Used in `UPDATE SET (col1, col2) = (SELECT ...)`.
    UpdateSet,
    /// `EXISTS` | `NOT EXISTS` subquery (semi/anti-semi join). Returns a boolean.
    Existential,
    /// `IN` subquery.
    In(ExprImpl),
    /// Expression operator `SOME` subquery.
    Some(ExprImpl, ExprType),
    /// Expression operator `ALL` subquery.
    All(ExprImpl, ExprType),
    /// Expression operator `ARRAY` subquery.
    Array,
}

/// Subquery expression.
#[derive(Clone)]
pub struct Subquery {
    pub query: BoundQuery,
    pub kind: SubqueryKind,
}

impl Subquery {
    pub fn new(query: BoundQuery, kind: SubqueryKind) -> Self {
        Self { query, kind }
    }

    pub fn is_correlated(&self, depth: Depth) -> bool {
        self.query.is_correlated(depth)
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        let mut correlated_indices = self
            .query
            .collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id);
        correlated_indices.sort();
        correlated_indices.dedup();
        correlated_indices
    }
}

impl PartialEq for Subquery {
    fn eq(&self, _other: &Self) -> bool {
        unreachable!("Subquery {:?} has not been unnested", self)
    }
}

impl Hash for Subquery {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        unreachable!("Subquery {:?} has not been hashed", self)
    }
}

impl Eq for Subquery {}

impl Expr for Subquery {
    fn return_type(&self) -> DataType {
        match self.kind {
            SubqueryKind::Scalar => {
                let types = self.query.data_types();
                assert_eq!(types.len(), 1, "Subquery with more than one column");
                types[0].clone()
            }
            SubqueryKind::UpdateSet => {
                let schema = self.query.schema();
                let struct_type = if schema.fields().iter().any(|f| f.name == UNNAMED_COLUMN) {
                    StructType::unnamed(self.query.data_types())
                } else {
                    StructType::new(
                        (schema.fields().iter().cloned()).map(|f| (f.name, f.data_type)),
                    )
                };
                DataType::Struct(struct_type)
            }
            SubqueryKind::Array => {
                let types = self.query.data_types();
                assert_eq!(types.len(), 1, "Subquery with more than one column");
                DataType::List(types[0].clone().into())
            }
            _ => DataType::Boolean,
        }
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("Subquery {:?} has not been unnested", self)
    }
}

impl std::fmt::Debug for Subquery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subquery")
            .field("kind", &self.kind)
            .field("query", &self.query)
            .finish()
    }
}
