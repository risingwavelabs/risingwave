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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::ErrorCode;
use risingwave_common::types::DataType;
use risingwave_expr::sig::table_function::FUNC_SIG_MAP;
pub use risingwave_pb::expr::table_function::PbType as TableFunctionType;
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

impl TableFunction {
    /// Create a `TableFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(func_type: TableFunctionType, args: Vec<ExprImpl>) -> RwResult<Self> {
        let arg_types = args.iter().map(|c| c.return_type()).collect_vec();
        let signature = FUNC_SIG_MAP
            .get(
                func_type,
                &args.iter().map(|c| c.return_type().into()).collect_vec(),
            )
            .ok_or_else(|| {
                ErrorCode::BindError(format!(
                    "table function not found: {:?}({})",
                    func_type,
                    arg_types.iter().map(|t| format!("{:?}", t)).join(", "),
                ))
            })?;
        let return_type = (signature.type_infer)(&arg_types)?;
        Ok(TableFunction {
            args,
            return_type,
            function_type: func_type,
            udtf_catalog: None,
        })
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
            function_type: self.function_type as i32,
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

    /// Get the name of the table function.
    pub fn name(&self) -> String {
        match self.function_type {
            TableFunctionType::Udtf => self.udtf_catalog.as_ref().unwrap().name.clone(),
            t => t.as_str_name().to_lowercase(),
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
