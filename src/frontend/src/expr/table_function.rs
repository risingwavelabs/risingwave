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

use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::ErrorCode;
use risingwave_common::types::{unnested_list_type, DataType, ScalarImpl};
use risingwave_pb::expr::table_function::Type;
use risingwave_pb::expr::{
    TableFunction as TableFunctionPb, UserDefinedTableFunction as UserDefinedTableFunctionPb,
};

use super::{Expr, ExprImpl, ExprRewriter, RwResult};
use crate::catalog::function_catalog::{FunctionCatalog, FunctionKind};

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
    /// Catalog of user defined table function.
    pub udtf_catalog: Option<Arc<FunctionCatalog>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableFunctionType {
    Generate,
    Range,
    Unnest,
    RegexpMatches,
    Udtf,
}

impl TableFunctionType {
    fn to_protobuf(self) -> Type {
        match self {
            TableFunctionType::Generate => Type::Generate,
            TableFunctionType::Range => Type::Range,
            TableFunctionType::Unnest => Type::Unnest,
            TableFunctionType::RegexpMatches => Type::RegexpMatches,
            TableFunctionType::Udtf => Type::Udtf,
        }
    }
}

impl TableFunction {
    pub fn name(&self) -> &str {
        match self.function_type {
            TableFunctionType::Generate => "generate_series",
            TableFunctionType::Range => "range",
            TableFunctionType::Unnest => "unnest",
            TableFunctionType::RegexpMatches => "regexp_matches",
            TableFunctionType::Udtf => &self.udtf_catalog.as_ref().unwrap().name,
        }
    }
}

impl FromStr for TableFunctionType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("generate_series") {
            Ok(TableFunctionType::Generate)
        } else if s.eq_ignore_ascii_case("range") {
            Ok(TableFunctionType::Range)
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
    pub fn new(func_type: TableFunctionType, args: Vec<ExprImpl>) -> RwResult<Self> {
        // TODO: refactor into sth like FunctionCall::new.
        // Current implementation is copied from legacy code.

        match func_type {
            function_type @ (TableFunctionType::Generate | TableFunctionType::Range) => {
                // generate_series ( start timestamp, stop timestamp, step interval ) or
                // generate_series ( start i32, stop i32, step i32 )

                fn type_check(exprs: &[ExprImpl]) -> RwResult<DataType> {
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
                    function_type,
                    udtf_catalog: None,
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
                        udtf_catalog: None,
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
                    match flag {
                        ExprImpl::Literal(flag) => {
                            match flag.get_data() {
                                Some(flag) => {
                                    let ScalarImpl::Utf8(flag) = flag else {
                                        return Err(ErrorCode::BindError(
                                            "flag in regexp_matches must be a literal string"
                                                .to_string(),
                                        )
                                        .into());
                                    };
                                    for c in flag.chars() {
                                        if !"icg".contains(c) {
                                            return Err(ErrorCode::NotImplemented(
                                                format!(
                                                    "invalid regular expression option: \"{c}\""
                                                ),
                                                None.into(),
                                            )
                                            .into());
                                        }
                                    }
                                }
                                None => {
                                    // flag is NULL. Will return NULL.
                                }
                            }
                        }
                        _ => {
                            return Err(ErrorCode::BindError(
                                "flag in regexp_matches must be a literal string".to_string(),
                            )
                            .into())
                        }
                    }
                }
                Ok(TableFunction {
                    args,
                    return_type: DataType::List {
                        datatype: Box::new(DataType::Varchar),
                    },
                    function_type: TableFunctionType::RegexpMatches,
                    udtf_catalog: None,
                })
            }
            // not in this path
            TableFunctionType::Udtf => unreachable!(),
        }
    }

    /// Create a user-defined `TableFunction`.
    pub fn new_user_defined(catalog: Arc<FunctionCatalog>, args: Vec<ExprImpl>) -> Self {
        let FunctionKind::Table = &catalog.kind else {
            panic!("not a table function");
        };
        TableFunction {
            args,
            return_type: catalog.return_type.clone(),
            function_type: TableFunctionType::Udtf,
            udtf_catalog: Some(catalog),
        }
    }

    pub fn to_protobuf(&self) -> TableFunctionPb {
        TableFunctionPb {
            function_type: self.function_type.to_protobuf() as i32,
            args: self.args.iter().map(|c| c.to_expr_proto()).collect_vec(),
            return_type: Some(self.return_type.to_protobuf()),
            udtf: self
                .udtf_catalog
                .as_ref()
                .map(|c| UserDefinedTableFunctionPb {
                    arg_types: c.arg_types.iter().map(|t| t.to_protobuf()).collect(),
                    language: c.language.clone(),
                    link: c.link.clone(),
                    identifier: c.identifier.clone(),
                }),
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
