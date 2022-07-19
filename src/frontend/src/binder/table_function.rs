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
use risingwave_common::types::DataType;
use risingwave_pb::expr::table_function::Type;
use risingwave_pb::expr::TableFunction;

use crate::expr::{Expr as _, ExprImpl, ExprRewriter};

/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
///
/// See also [`TableFunction`](risingwave_expr::table_function::TableFunction) trait in expr crate.
#[derive(Clone)]
pub struct BoundTableFunction {
    pub(crate) args: Vec<ExprImpl>,
    pub(crate) return_type: DataType,
    pub(crate) function_type: TableFunctionType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFunctionType {
    Generate,
    Unnest,
}

impl TableFunctionType {
    fn to_protobuf(&self) -> Type {
        match self {
            TableFunctionType::Generate => Type::Generate,
            TableFunctionType::Unnest => Type::Unnest,
        }
    }
}

impl TableFunctionType {
    pub fn name(&self) -> &str {
        match self {
            TableFunctionType::Generate => "generate_series",
            TableFunctionType::Unnest => "unnest",
        }
    }
}

impl BoundTableFunction {
    pub fn to_protobuf(&self) -> TableFunction {
        TableFunction {
            function_type: self.function_type.to_protobuf() as i32,
            args: self.args.iter().map(|c| c.to_expr_proto()).collect_vec(),
            return_type: Some(self.return_type.to_protobuf()),
        }
    }

    pub fn rewrite(self, rewriter: &mut impl ExprRewriter) -> Self {
        Self {
            args: self
                .args
                .into_iter()
                .map(|e| rewriter.rewrite_expr(e))
                .collect(),
            ..self
        }
    }
}

impl std::fmt::Debug for BoundTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("FunctionCall")
                .field("function_type", &self.function_type)
                .field("return_type", &self.return_type)
                .field("args", &self.args)
                .finish()
        } else {
            let func_name = format!("{:?}", self.function_type);
            let mut builder = f.debug_tuple(&func_name);
            self.args.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()
        }
    }
}
