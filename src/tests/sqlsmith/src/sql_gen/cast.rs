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

use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_frontend::expr::CastContext;
use risingwave_sqlparser::ast::Expr;

use crate::sql_gen::types::{data_type_to_ast_data_type, EXPLICIT_CAST_TABLE};
use crate::sql_gen::{SqlGenerator, SqlGeneratorContext};

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_explicit_cast(
        &mut self,
        ret: &DataType,
        context: SqlGeneratorContext,
    ) -> Expr {
        self.gen_explicit_cast_inner(ret, context)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generate casts from a cast map.
    /// TODO: Assign casts have to be tested via `INSERT`.
    fn gen_explicit_cast_inner(
        &mut self,
        ret: &DataType,
        context: SqlGeneratorContext,
    ) -> Option<Expr> {
        let casts = EXPLICIT_CAST_TABLE.get(ret)?;
        let cast_sig = casts.choose(&mut self.rng).unwrap();

        match cast_sig.context {
            CastContext::Explicit => {
                let expr = self.gen_expr(&cast_sig.from_type, context).into();
                let data_type = data_type_to_ast_data_type(&cast_sig.to_type);
                Some(Expr::Cast { expr, data_type })
            }

            // TODO: Generate this when e2e inserts are generated.
            // T::Assign
            _ => unreachable!(),
        }
    }

    /// NOTE: This can result in ambiguous expressions.
    /// Should only be used in unambiguous context.
    pub(crate) fn gen_implicit_cast(
        &mut self,
        ret: &DataType,
        context: SqlGeneratorContext,
    ) -> Expr {
        self.gen_expr(ret, context)
    }
}
