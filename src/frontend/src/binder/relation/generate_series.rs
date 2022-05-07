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
use risingwave_common::error::ErrorCode;
use risingwave_sqlparser::ast::FunctionArg;

use super::{Binder, Result};
use crate::expr::ExprImpl;

#[derive(Debug)]
pub struct BoundGenerateSeriesFunction {
    pub(crate) args: Vec<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_generate_series_function(
        &mut self,
        args: Vec<FunctionArg>,
    ) -> Result<BoundGenerateSeriesFunction> {
        let mut args = args.into_iter();

        self.push_context();

        // generate_series ( start timestamp, stop timestamp, step interval )
        // step interval might be None
        if args.len() < 2 || args.len() > 3 {
            return Err(ErrorCode::BindError(
                "the args of generate series funciton should be around 2 to 3".to_string(),
            )
            .into());
        }
        // Todo(d2lark) check 2 or 3 args are same type

        self.pop_context();

        let exprs: Vec<_> = args
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;

        dbg!(&exprs);

        Ok(BoundGenerateSeriesFunction { args: exprs })
    }
}
