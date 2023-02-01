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

use risingwave_common::types::DataType;

use super::{Expr, ExprImpl};
use crate::catalog::function_catalog::FunctionCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserDefinedFunction {
    pub args: Vec<ExprImpl>,
    pub catalog: Arc<FunctionCatalog>,
}

impl UserDefinedFunction {
    pub fn new(catalog: Arc<FunctionCatalog>, args: Vec<ExprImpl>) -> Self {
        Self { args, catalog }
    }
}

impl Expr for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.catalog.return_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: Type::Udf.into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::Udf(UserDefinedFunction {
                children: self.args.iter().map(Expr::to_expr_proto).collect(),
                name: self.catalog.name.clone(),
                arg_types: self
                    .catalog
                    .arg_types
                    .iter()
                    .map(|t| t.to_protobuf())
                    .collect(),
                language: self.catalog.language.clone(),
                path: self.catalog.path.clone(),
            })),
        }
    }
}
