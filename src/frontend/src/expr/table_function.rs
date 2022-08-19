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

use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::error::ErrorCode;
use risingwave_common::types::{unnested_list_type, DataType, ScalarImpl};
use risingwave_pb::expr::table_function::Type;
use risingwave_pb::expr::TableFunction as TableFunctionProst;

use super::{Expr, ExprImpl, ExprRewriter, Result};

/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
///
/// See also [`TableFunction`](risingwave_expr::table_function::TableFunction) trait in expr crate
/// and [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem).
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct TableFunction {
    pub args: Vec<ExprImpl>,
    pub return_type: DataType,
    pub function_type: TableFunctionType,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TableFunctionType {
    Generate,
    Unnest,
    RegexpMatches,
}

impl TableFunctionType {
    fn to_protobuf(self) -> Type {
        match self {
            TableFunctionType::Generate => Type::Generate,
            TableFunctionType::Unnest => Type::Unnest,
            TableFunctionType::RegexpMatches => Type::RegexpMatches,
        }
    }
}

impl TableFunctionType {
    pub fn name(&self) -> &str {
        match self {
            TableFunctionType::Generate => "generate_series",
            TableFunctionType::Unnest => "unnest",
            TableFunctionType::RegexpMatches => "regexp_matches",
        }
    }
}

impl FromStr for TableFunctionType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("generate_series") {
            Ok(TableFunctionType::Generate)
        } else if s.eq_ignore_ascii_case("unnest") {
            Ok(TableFunctionType::Unnest)
        } else if s.eq_ignore_ascii_case("regexp_matches") {
            Ok(TableFunctionType::RegexpMatches)
        } else {
            Err(())
        }
    }
}

impl TableFunction {
    /// Create a `TableFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(func_type: TableFunctionType, args: Vec<ExprImpl>) -> Result<Self> {
        // TODO: refactor into sth like FunctionCall::new.
        // Current implementation is copied from legacy code.

        match func_type {
            TableFunctionType::Generate => {
                // generate_series ( start timestamp, stop timestamp, step interval ) or
                // generate_series ( start i32, stop i32, step i32 )

                fn type_check(exprs: &[ExprImpl]) -> Result<DataType> {
                    let mut exprs = exprs.iter();
                    let (start, stop, step) = exprs.next_tuple().unwrap();
                    match (start.return_type(), stop.return_type(), step.return_type()) {
                        (DataType::Int32, DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
                        (DataType::Timestamp, DataType::Timestamp, DataType::Interval) => {
                            Ok(DataType::Timestamp)
                        }
                        _ => Err(ErrorCode::BindError(
                            "Invalid arguments for Generate series function".to_string(),
                        )
                        .into()),
                    }
                }

                if args.len() != 3 {
                    return Err(ErrorCode::BindError(
                        "the length of args of generate series function should be 3".to_string(),
                    )
                    .into());
                }

                let data_type = type_check(&args)?;

                Ok(TableFunction {
                    args,
                    return_type: data_type,
                    function_type: TableFunctionType::Generate,
                })
            }
            TableFunctionType::Unnest => {
                if args.len() != 1 {
                    return Err(ErrorCode::BindError(
                        "the length of args of unnest function should be 1".to_string(),
                    )
                    .into());
                }

                let expr = args.into_iter().next().unwrap();
                if matches!(expr.return_type(), DataType::List { datatype: _ }) {
                    let data_type = unnested_list_type(expr.return_type());

                    Ok(TableFunction {
                        args: vec![expr],
                        return_type: data_type,
                        function_type: TableFunctionType::Unnest,
                    })
                } else {
                    Err(ErrorCode::BindError(
                        "the expr function of unnest function should be array".to_string(),
                    )
                    .into())
                }
            }
            TableFunctionType::RegexpMatches => {
                if args.len() != 2 && args.len() != 3 {
                    return Err(ErrorCode::BindError(
                        "the length of args of generate series function should be 2 or 3"
                            .to_string(),
                    )
                    .into());
                }
                if let Some(flag) = args.get(2) {
                    if let ExprImpl::Literal(lit) = flag &&
                      let Some(ScalarImpl::Utf8(flag)) = lit.get_data() {
                        if flag != "g" {
                            return Err(ErrorCode::NotImplemented(
                                "flag in regexp_matches".to_string(),
                                4545.into()
                            ).into());
                        }
                        // Currently when 'g' is not present, regexp_matches will also return multiple rows.
                        // This is intuitive, but differs from PG's default behavior.
                    } else {
                        return Err(ErrorCode::BindError(
                            "flag in regexp_matches should be a constant string".to_string(),
                        )
                        .into())
                    };
                }
                Ok(TableFunction {
                    args,
                    return_type: DataType::List {
                        datatype: Box::new(DataType::Varchar),
                    },
                    function_type: TableFunctionType::RegexpMatches,
                })
            }
        }
    }

    pub fn to_protobuf(&self) -> TableFunctionProst {
        TableFunctionProst {
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

impl std::fmt::Debug for TableFunction {
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

impl Expr for TableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("Table function should not be converted to ExprNode")
    }
}
