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
use risingwave_common::catalog::Field;
use risingwave_common::error::ErrorCode;
use risingwave_common::types::DataType;
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
        let args = args.into_iter();

        // generate_series ( start timestamp, stop timestamp, step interval )
        if args.len() != 3 {
            return Err(ErrorCode::BindError(
                "the length of args of generate series funciton should be 3".to_string(),
            )
            .into());
        }
        // Todo(d2lark) check 2 or 3 args are same type
        let columns = [(
            false,
            Field {
                data_type: DataType::Timestamp,
                name: "generate_series".to_string(),
                sub_fields: vec![],
                type_name: "".to_string(),
            },
        )]
        .into_iter();

        self.bind_context(columns, "generate_series".to_string(), None)?;

        let exprs: Vec<_> = args
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;

        Ok(BoundGenerateSeriesFunction { args: exprs })
    }
}
