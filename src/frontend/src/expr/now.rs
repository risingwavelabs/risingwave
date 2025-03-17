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

use risingwave_common::types::DataType;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::{self, NowRexNode};

use super::{Expr, ExprImpl, ExprRewriter, FunctionCall, Literal};
use crate::expr::ExprVisitor;

/// The `NOW()` function.
/// - in streaming queries, it represents a retractable monotonic timestamp stream,
/// - in batch queries, it represents a constant timestamp.
///
/// `NOW()` should only appear during optimization, or in the table catalog for column default
/// values. Before execution, it should be rewritten to `Literal` in batch queries, or `NowNode` in
/// streaming queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Now;

impl Expr for Now {
    fn return_type(&self) -> DataType {
        DataType::Timestamptz
    }

    fn to_expr_proto(&self) -> ExprNode {
        ExprNode {
            function_type: expr_node::Type::Unspecified.into(),
            return_type: Some(self.return_type().into()),
            rex_node: Some(expr_node::RexNode::Now(NowRexNode {})),
        }
    }
}

/// Expression rewriter to inline `NOW()` and `PROCTIME()` to a literal extracted from the epoch.
///
/// This should only be applied for batch queries. See the documentation of [`Now`] for details.
pub struct InlineNowProcTime {
    /// The current epoch value.
    epoch: Epoch,
}

impl InlineNowProcTime {
    pub fn new(epoch: Epoch) -> Self {
        Self { epoch }
    }

    fn literal(&self) -> ExprImpl {
        Literal::new(Some(self.epoch.as_scalar()), Now.return_type()).into()
    }
}

impl ExprRewriter for InlineNowProcTime {
    fn rewrite_now(&mut self, _now: Now) -> ExprImpl {
        self.literal()
    }

    fn rewrite_function_call(&mut self, func_call: super::FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();

        if let expr_node::Type::Proctime = func_type {
            assert!(inputs.is_empty());
            return self.literal();
        }

        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new_unchecked(func_type, inputs, ret).into()
    }
}

/// Expression rewriter to rewrite `NOW()` to `PROCTIME()`
///
/// This is applied for the sink into table query for those column with default expression containing `now()` because streaming execution can not handle `now` expression
pub struct RewriteNowToProcTime;

impl ExprRewriter for RewriteNowToProcTime {
    fn rewrite_now(&mut self, _now: Now) -> ExprImpl {
        FunctionCall::new(expr_node::Type::Proctime, vec![])
            .unwrap()
            .into()
    }
}

#[derive(Default)]
pub struct NowProcTimeFinder {
    has: bool,
}

impl NowProcTimeFinder {
    pub fn has(&self) -> bool {
        self.has
    }
}

impl ExprVisitor for NowProcTimeFinder {
    fn visit_now(&mut self, _: &Now) {
        self.has = true;
    }

    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        if let expr_node::Type::Proctime = func_call.func_type {
            self.has = true;
            return;
        }

        func_call
            .inputs()
            .iter()
            .for_each(|expr| self.visit_expr(expr));
    }
}
