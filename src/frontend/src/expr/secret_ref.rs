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

use risingwave_common::types::DataType;
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_sqlparser::ast::SecretRefAsType;

use super::{Expr, ExprImpl};
use crate::catalog::SecretId;
use crate::expr::ExprRewriter;

/// Represents a secret reference that will be resolved to its actual value during planning.
/// This is used when a function argument is `secret <name>`.
///
/// Note: This expression type is created by the parser and should be resolved by the binder
/// into a fully-qualified secret reference with catalog ID before serialization.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRefExpr {
    pub secret_ref_type: SecretRefAsType,
    /// the resolved secret ID from the catalog
    pub secret_id: SecretId,
}

impl SecretRefExpr {
    pub fn new(secret_ref_type: SecretRefAsType, secret_id: SecretId) -> Self {
        Self {
            secret_ref_type,
            secret_id,
        }
    }

    pub fn from_expr_proto(secret_ref: &risingwave_pb::secret::PbSecretRef) -> Self {
        let ref_as = match secret_ref.ref_as() {
            PbRefAsType::Text => SecretRefAsType::Text,
            PbRefAsType::File => SecretRefAsType::File,
            PbRefAsType::Unspecified => unreachable!("Unspecified is not a valid RefAsType"),
        };

        Self {
            secret_ref_type: ref_as,
            secret_id: secret_ref.secret_id,
        }
    }

    pub fn to_pb_secret_ref(&self) -> risingwave_pb::secret::PbSecretRef {
        let ref_as = match self.secret_ref_type {
            SecretRefAsType::Text => PbRefAsType::Text,
            SecretRefAsType::File => PbRefAsType::File,
        };

        risingwave_pb::secret::PbSecretRef {
            secret_id: self.secret_id.into(),
            ref_as: ref_as.into(),
        }
    }
}

impl Expr for SecretRefExpr {
    fn return_type(&self) -> DataType {
        // Secrets are always returned as VARCHAR
        DataType::Varchar
    }

    fn try_to_expr_proto(&self) -> std::result::Result<risingwave_pb::expr::ExprNode, String> {
        let pb_secret_ref = self.to_pb_secret_ref();

        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;

        Ok(ExprNode {
            function_type: 0, // Not a function
            return_type: Some(DataType::Varchar.to_protobuf()),
            rex_node: Some(RexNode::Secret(pb_secret_ref)),
        })
    }
}

impl ExprRewriter for SecretRefExpr {
    fn rewrite_secret_ref(&mut self, new_secret_ref: SecretRefExpr) -> ExprImpl {
        new_secret_ref.into()
    }
}
