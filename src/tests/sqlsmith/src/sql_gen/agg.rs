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

use rand::Rng;
use rand::seq::IndexedRandom;
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::PbAggKind;
use risingwave_expr::sig::SigDataType;
use risingwave_sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgList, Ident, ObjectName, OrderByExpr,
};

use crate::config::Syntax;
use crate::sql_gen::types::AGG_FUNC_TABLE;
use crate::sql_gen::{SqlGenerator, SqlGeneratorContext};

impl<R: Rng> SqlGenerator<'_, R> {
    pub fn gen_agg(&mut self, ret: &DataType) -> Expr {
        let funcs = match AGG_FUNC_TABLE.get(ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        if matches!(func.name.as_aggregate(), PbAggKind::Min | PbAggKind::Max)
            && matches!(
                func.ret_type,
                SigDataType::Exact(DataType::Boolean | DataType::Jsonb)
            )
        {
            return self.gen_simple_scalar(ret);
        }

        let context = SqlGeneratorContext::new(self.should_generate(Syntax::Agg), true);
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(t.as_exact(), context))
            .collect();

        // DISTINCT now only works with agg kinds except `ApproxCountDistinct`, and with at least
        // one argument and only the first being non-constant. See `Binder::bind_normal_agg`
        // for more details.
        let distinct_allowed = func.name.as_aggregate() != PbAggKind::ApproxCountDistinct
            && !exprs.is_empty()
            && exprs.iter().skip(1).all(|e| matches!(e, Expr::Value(_)));
        let distinct = distinct_allowed && self.flip_coin();

        let filter = if self.flip_coin() {
            let context = SqlGeneratorContext::new(false, false);
            // ENABLE: https://github.com/risingwavelabs/risingwave/issues/4762
            // Prevent correlated query with `FILTER`
            let old_ctxt = self.new_local_context();
            let expr = Some(Box::new(self.gen_expr(&DataType::Boolean, context)));
            self.restore_context(old_ctxt);
            expr
        } else {
            None
        };

        let order_by = if self.flip_coin() {
            if distinct {
                // can only generate order by clause with exprs in argument list, see
                // `Binder::bind_normal_agg`
                self.gen_order_by_within(&exprs)
            } else {
                self.gen_order_by()
            }
        } else {
            vec![]
        };
        self.make_agg_expr(func.name.as_aggregate(), &exprs, distinct, filter, order_by)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generates aggregate expressions. For internal / unsupported aggregators, we return `None`.
    fn make_agg_expr(
        &mut self,
        func: PbAggKind,
        exprs: &[Expr],
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Vec<OrderByExpr>,
    ) -> Option<Expr> {
        use PbAggKind as A;
        match func {
            kind @ (A::FirstValue | A::LastValue) => {
                if order_by.is_empty() {
                    // `first/last_value` only works when ORDER BY is provided
                    None
                } else {
                    Some(Expr::Function(make_agg_func(
                        &kind.as_str_name().to_lowercase(),
                        exprs,
                        distinct,
                        filter,
                        order_by,
                    )))
                }
            }
            other => Some(Expr::Function(make_agg_func(
                &other.as_str_name().to_lowercase(),
                exprs,
                distinct,
                filter,
                order_by,
            ))),
        }
    }
}

/// This is the function that generate aggregate function.
/// DISTINCT, ORDER BY or FILTER is allowed in aggregation functions。
fn make_agg_func(
    func_name: &str,
    exprs: &[Expr],
    distinct: bool,
    filter: Option<Box<Expr>>,
    order_by: Vec<OrderByExpr>,
) -> Function {
    let args = if exprs.is_empty() {
        // The only agg without args is `count`.
        // `select proname from pg_proc where array_length(proargtypes, 1) = 0 and prokind = 'a';`
        vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard(None))]
    } else {
        exprs
            .iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
            .collect()
    };

    Function {
        scalar_as_agg: false,
        name: ObjectName(vec![Ident::new_unchecked(func_name)]),
        arg_list: FunctionArgList::for_agg(distinct, args, order_by),
        over: None,
        filter,
        within_group: None,
    }
}
