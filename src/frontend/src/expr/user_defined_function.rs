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

use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::{FunctionId, Schema};
use risingwave_common::types::DataType;

use super::{Expr, ExprDisplay, ExprImpl};
use crate::catalog::function_catalog::{FunctionCatalog, FunctionKind};
use crate::error::ErrorCode;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserDefinedFunction {
    pub args: Vec<ExprImpl>,
    pub catalog: Arc<FunctionCatalog>,
}

impl UserDefinedFunction {
    pub fn new(catalog: Arc<FunctionCatalog>, args: Vec<ExprImpl>) -> Self {
        Self { args, catalog }
    }

    pub(super) fn from_expr_proto(
        udf: &risingwave_pb::expr::UserDefinedFunction,
        return_type: DataType,
    ) -> crate::error::Result<Self> {
        let mut args: Vec<_> = udf
            .get_children()
            .iter()
            .map(ExprImpl::from_expr_proto)
            .try_collect()?;
        let mut arg_names = udf.arg_names.clone();
        let mut arg_types = udf.get_arg_types().iter().map_into().collect_vec();
        let mut secret_refs = BTreeMap::new();

        while let Some(secret) = args.pop_if(|arg| matches!(arg, ExprImpl::SecretRefExpr(_))) {
            let secret_expr = secret.into_secret_ref_expr().unwrap();
            let secret_name = arg_names.pop().ok_or_else(|| {
                Err(ErrorCode::InternalError(format!(
                    "mismatched secret arg names and args for udf {}",
                    udf.name
                )).into())
            })?;
            let data_type = arg_types.pop().ok_or_else(|| {
                Err(ErrorCode::InternalError(format!(
                    "mismatched secret arg types and args for udf {}",
                    udf.name
                )).into())
            })?;
            if data_type != secret_expr.return_type() {
                return Err(ErrorCode::InternalError(format!(
                    "mismatched secret arg type for udf {}: expected {:?}, got {:?}",
                    udf.name,
                    data_type,
                    secret_expr.return_type()
                ))
                .into());
            }
            if secret_refs
                .insert(secret_name, secret_expr.to_pb_secret_ref())
                .is_some()
            {
                return Err(ErrorCode::InternalError(format!(
                    "duplicate secret arg name '{}' for udf {}",
                    secret_name, udf.name
                ))
                .into());
            }
        }

        debug_assert!(
            args.iter()
                .all(|arg| !matches!(arg, ExprImpl::SecretRefExpr(_)))
        );
        debug_assert_eq!(arg_names.len(), args.len());
        debug_assert_eq!(arg_types.len(), args.len());

        // function catalog
        let catalog = FunctionCatalog {
            // FIXME(yuhao): function id is not in udf proto.
            id: FunctionId::placeholder(),
            name: udf.name.clone(),
            // FIXME(yuhao): owner is not in udf proto.
            owner: u32::MAX - 1,
            kind: FunctionKind::Scalar,
            arg_names: udf.arg_names.clone(),
            arg_types,
            return_type,
            language: udf.language.clone(),
            runtime: udf.runtime.clone(),
            name_in_runtime: udf.name_in_runtime().map(|x| x.to_owned()),
            body: udf.body.clone(),
            link: udf.link.clone(),
            compressed_binary: udf.compressed_binary.clone(),
            always_retry_on_network_error: udf.always_retry_on_network_error,
            is_batched: udf.is_batched,
            is_async: udf.is_async,
            created_at_epoch: None,
            created_at_cluster_version: None,
            secret_refs,
        };

        Ok(Self {
            args,
            catalog: Arc::new(catalog),
        })
    }
}

impl Expr for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.catalog.return_type.clone()
    }

    fn try_to_expr_proto(&self) -> Result<risingwave_pb::expr::ExprNode, String> {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;

        let children: Vec<_> = self
            .args
            .iter()
            .map(|arg| arg.try_to_expr_proto())
            .try_collect()?;

        // Prepare arg_names and arg_types including secret refs
        let mut arg_names = self.catalog.arg_names.clone();
        let mut arg_types = self
            .catalog
            .arg_types
            .iter()
            .map(|t| t.to_protobuf())
            .collect_vec();
        for secret_name in self.catalog.secret_refs.keys() {
            arg_names.push(secret_name.clone());
            arg_types.push(DataType::Varchar.to_protobuf());
        }

        debug_assert_eq!(arg_names.len(), children.len());
        debug_assert_eq!(arg_types.len(), children.len());

        Ok(ExprNode {
            function_type: Type::Unspecified.into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::Udf(Box::new(UserDefinedFunction {
                children,
                name: self.catalog.name.clone(),
                arg_names,
                arg_types,
                language: self.catalog.language.clone(),
                runtime: self.catalog.runtime.clone(),
                identifier: self.catalog.name_in_runtime.clone(),
                link: self.catalog.link.clone(),
                body: self.catalog.body.clone(),
                compressed_binary: self.catalog.compressed_binary.clone(),
                always_retry_on_network_error: self.catalog.always_retry_on_network_error,
                is_async: self.catalog.is_async,
                is_batched: self.catalog.is_batched,
                version: PbUdfExprVersion::LATEST as _,
            }))),
        })
    }
}

pub struct UserDefinedFunctionDisplay<'a> {
    pub func_call: &'a UserDefinedFunction,
    pub input_schema: &'a Schema,
}

impl std::fmt::Debug for UserDefinedFunctionDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let that = self.func_call;
        let mut builder = f.debug_tuple(&that.catalog.name);
        that.args.iter().for_each(|arg| {
            builder.field(&ExprDisplay {
                expr: arg,
                input_schema: self.input_schema,
            });
        });
        builder.finish()
    }
}
