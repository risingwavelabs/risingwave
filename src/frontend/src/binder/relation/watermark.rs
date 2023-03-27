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
use risingwave_common::error::ErrorCode;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{FunctionArg, TableAlias};

use super::{Binder, Relation, Result};
use crate::binder::statement::RewriteExprsRecursive;
use crate::expr::{ExprImpl, InputRef};

const ERROR_1ST_ARG: &str = "The 1st arg of watermark function should be a table name (incl. source, CTE, view) but not complex structure (subquery, join, another table function). Consider using an intermediate CTE or view as workaround.";
const ERROR_2ND_ARG_EXPR: &str = "The 2st arg of watermark function should be a column name but not complex expression. Consider using an intermediate CTE or view as workaround.";
const ERROR_2ND_ARG_TYPE: &str = "The 2st arg of watermark function should be a column of type timestamp with time zone, timestamp or date.";

#[derive(Debug, Clone)]
#[expect(dead_code)]
pub struct BoundWatermark {
    pub(crate) input: Relation,
    pub(crate) time_col: InputRef,
    pub(crate) args: Vec<ExprImpl>,
}

impl RewriteExprsRecursive for BoundWatermark {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.input.rewrite_exprs_recursive(rewriter);
        let new_agrs = std::mem::take(&mut self.args)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.args = new_agrs;
    }
}

pub(super) fn is_watermark_func(func_name: &str) -> bool {
    func_name.eq_ignore_ascii_case("watermark")
}

impl Binder {
    pub(super) fn bind_watermark(
        &mut self,
        alias: Option<TableAlias>,
        args: Vec<FunctionArg>,
    ) -> Result<BoundWatermark> {
        let mut args = args.into_iter();

        self.push_context();

        let (base, table_name) = self.bind_relation_by_function_arg(args.next(), ERROR_1ST_ARG)?;

        let time_col = self.bind_column_by_function_args(args.next(), ERROR_2ND_ARG_EXPR)?;

        if DataType::window_of(&time_col.data_type).is_none() {
            return Err(ErrorCode::BindError(ERROR_2ND_ARG_TYPE.to_string()).into());
        };

        let base_columns = std::mem::take(&mut self.context.columns);

        self.pop_context()?;

        let columns = base_columns
            .into_iter()
            .map(|c| (c.is_hidden, c.field))
            .collect_vec();

        let (_, table_name) = Self::resolve_schema_qualified_name(&self.db_name, table_name)?;
        self.bind_table_to_context(columns, table_name, alias)?;

        // Other arguments are validated in `plan_watermark`
        let exprs: Vec<_> = args
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        Ok(BoundWatermark {
            input: base,
            time_col: *time_col,
            args: exprs,
        })
    }
}
