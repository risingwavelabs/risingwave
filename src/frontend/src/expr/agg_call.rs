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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::{Expr, ExprImpl, ExprRewriter};
use crate::optimizer::property::Direction;
use crate::utils::Condition;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AggOrderByExpr {
    pub expr: ExprImpl,
    pub direction: Direction,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AggOrderBy {
    pub sort_exprs: Vec<AggOrderByExpr>,
}

impl AggOrderBy {
    pub fn any() -> Self {
        Self {
            sort_exprs: Vec::new(),
        }
    }

    pub fn new(sort_exprs: Vec<AggOrderByExpr>) -> Self {
        Self { sort_exprs }
    }

    pub fn rewrite_expr(self, rewriter: &mut (impl ExprRewriter + ?Sized)) -> Self {
        Self {
            sort_exprs: self
                .sort_exprs
                .into_iter()
                .map(|e| AggOrderByExpr {
                    expr: rewriter.rewrite_expr(e.expr),
                    direction: e.direction,
                    nulls_first: e.nulls_first,
                })
                .collect(),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct AggCall {
    agg_kind: AggKind,
    return_type: DataType,
    inputs: Vec<ExprImpl>,
    distinct: bool,
    order_by: AggOrderBy,
    filter: Condition,
}

impl std::fmt::Debug for AggCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("AggCall")
                .field("agg_kind", &self.agg_kind)
                .field("return_type", &self.return_type)
                .field("inputs", &self.inputs)
                .field("filter", &self.filter)
                .finish()
        } else {
            let mut builder = f.debug_tuple(&format!("{}", self.agg_kind));
            self.inputs.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()
        }
    }
}

impl AggCall {
    /// Infer the return type for the given agg call.
    /// Returns error if not supported or the arguments are invalid.
    pub fn infer_return_type(agg_kind: &AggKind, inputs: &[DataType]) -> Result<DataType> {
        let invalid = || {
            let args = inputs.iter().map(|t| format!("{}", t)).join(", ");
            Err(RwError::from(ErrorCode::InvalidInputSyntax(format!(
                "Invalid aggregation: {}({})",
                agg_kind, args
            ))))
        };

        // The function signatures are aligned with postgres, see
        // https://www.postgresql.org/docs/current/functions-aggregate.html.
        let return_type = match (&agg_kind, inputs) {
            // Min, Max
            (AggKind::Min | AggKind::Max, [input]) => input.clone(),
            (AggKind::Min | AggKind::Max, _) => return invalid(),

            // Avg
            (AggKind::Avg, [input]) => match input {
                DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Decimal => {
                    DataType::Decimal
                }
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::Interval => DataType::Interval,
                _ => return invalid(),
            },
            (AggKind::Avg, _) => return invalid(),

            // Sum
            (AggKind::Sum, [input]) => match input {
                DataType::Int16 => DataType::Int64,
                DataType::Int32 => DataType::Int64,
                DataType::Int64 => DataType::Decimal,
                DataType::Decimal => DataType::Decimal,
                DataType::Float32 => DataType::Float32,
                DataType::Float64 => DataType::Float64,
                DataType::Interval => DataType::Interval,
                _ => return invalid(),
            },
            (AggKind::Sum, _) => return invalid(),

            // ApproxCountDistinct
            (AggKind::ApproxCountDistinct, [_]) => DataType::Int64,
            (AggKind::ApproxCountDistinct, _) => return invalid(),

            // Count
            (AggKind::Count, [] | [_]) => DataType::Int64,
            (AggKind::Count, _) => return invalid(),

            // StringAgg
            (AggKind::StringAgg, [DataType::Varchar, DataType::Varchar]) => DataType::Varchar,
            (AggKind::StringAgg, _) => return invalid(),

            (AggKind::ArrayAgg, [input]) => DataType::List {
                datatype: Box::new(input.clone()),
            },
            (AggKind::ArrayAgg, _) => return invalid(),

            // SingleValue
            (AggKind::SingleValue, [input]) => input.clone(),
            (AggKind::SingleValue, _) => return invalid(),
        };

        Ok(return_type)
    }

    /// Returns error if the function name matches with an existing function
    /// but with illegal arguments.
    pub fn new(
        agg_kind: AggKind,
        inputs: Vec<ExprImpl>,
        distinct: bool,
        order_by: AggOrderBy,
        filter: Condition,
    ) -> Result<Self> {
        let data_types = inputs.iter().map(ExprImpl::return_type).collect_vec();
        let return_type = Self::infer_return_type(&agg_kind, &data_types)?;
        Ok(AggCall {
            agg_kind,
            return_type,
            inputs,
            distinct,
            order_by,
            filter,
        })
    }

    pub fn decompose(self) -> (AggKind, Vec<ExprImpl>, bool, AggOrderBy, Condition) {
        (
            self.agg_kind,
            self.inputs,
            self.distinct,
            self.order_by,
            self.filter,
        )
    }

    pub fn agg_kind(&self) -> AggKind {
        self.agg_kind
    }

    /// Get a reference to the agg call's inputs.
    pub fn inputs(&self) -> &[ExprImpl] {
        self.inputs.as_ref()
    }

    pub fn inputs_mut(&mut self) -> &mut [ExprImpl] {
        self.inputs.as_mut()
    }
}

impl Expr for AggCall {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        // This function is always called on the physical planning step, where
        // `ExprImpl::AggCall` must have been rewritten to aggregate operators.

        unreachable!(
            "AggCall {:?} has not been rewritten to physical aggregate operators",
            self
        )
    }
}
