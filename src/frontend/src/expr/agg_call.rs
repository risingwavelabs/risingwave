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

use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::AggKind;

use super::{infer_type, Expr, ExprImpl, Literal, OrderBy};
use crate::utils::Condition;

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct AggCall {
    agg_kind: AggKind,
    return_type: DataType,
    args: Vec<ExprImpl>,
    distinct: bool,
    order_by: OrderBy,
    filter: Condition,
    direct_args: Vec<Literal>,
}

impl std::fmt::Debug for AggCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("AggCall")
                .field("agg_kind", &self.agg_kind)
                .field("return_type", &self.return_type)
                .field("args", &self.args)
                .field("filter", &self.filter)
                .finish()
        } else {
            let mut builder = f.debug_tuple(&format!("{}", self.agg_kind));
            self.args.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()
        }
    }
}

impl AggCall {
    /// Returns error if the function name matches with an existing function
    /// but with illegal arguments.
    pub fn new(
        agg_kind: AggKind,
        mut args: Vec<ExprImpl>,
        distinct: bool,
        order_by: OrderBy,
        filter: Condition,
        direct_args: Vec<Literal>,
    ) -> Result<Self> {
        let return_type = infer_type(agg_kind.into(), &mut args)?;
        Ok(AggCall {
            agg_kind,
            return_type,
            args,
            distinct,
            order_by,
            filter,
            direct_args,
        })
    }

    /// Constructs an `AggCall` without type inference.
    pub fn new_unchecked(
        agg_kind: AggKind,
        args: Vec<ExprImpl>,
        return_type: DataType,
    ) -> Result<Self> {
        Ok(AggCall {
            agg_kind,
            return_type,
            args,
            distinct: false,
            order_by: OrderBy::any(),
            filter: Condition::true_cond(),
            direct_args: vec![],
        })
    }

    pub fn decompose(
        self,
    ) -> (
        AggKind,
        Vec<ExprImpl>,
        bool,
        OrderBy,
        Condition,
        Vec<Literal>,
    ) {
        (
            self.agg_kind,
            self.args,
            self.distinct,
            self.order_by,
            self.filter,
            self.direct_args,
        )
    }

    pub fn agg_kind(&self) -> AggKind {
        self.agg_kind
    }

    /// Get a reference to the agg call's arguments.
    pub fn args(&self) -> &[ExprImpl] {
        self.args.as_ref()
    }

    pub fn args_mut(&mut self) -> &mut [ExprImpl] {
        self.args.as_mut()
    }

    pub fn order_by(&self) -> &OrderBy {
        &self.order_by
    }

    pub fn order_by_mut(&mut self) -> &mut OrderBy {
        &mut self.order_by
    }

    pub fn filter(&self) -> &Condition {
        &self.filter
    }

    pub fn filter_mut(&mut self) -> &mut Condition {
        &mut self.filter
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
