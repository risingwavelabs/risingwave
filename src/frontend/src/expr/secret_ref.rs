// Copyright 2026 RisingWave Labs
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
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::{ExprNode, SecretRefNode};
use risingwave_pb::secret::secret_ref::RefAsType;

use super::Expr;
use crate::catalog::SecretId;

/// A reference to a secret that is resolved at runtime on compute nodes.
///
/// The secret value is never stored in the plan — only the `secret_id` is serialized.
/// At execution time, the expression is resolved to a literal via `LocalSecretManager`.
#[derive(Clone)]
pub struct SecretRef {
    pub secret_id: SecretId,
    pub ref_as: RefAsType,
    /// Human-readable name for EXPLAIN output. Not serialized to proto.
    /// Excluded from `PartialEq`/`Hash` since it's display-only and lost during proto round-trip.
    pub secret_name: String,
}

impl std::fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretRef")
            .field("secret_id", &self.secret_id)
            .field("ref_as", &self.ref_as)
            .field("secret_name", &"[REDACTED]")
            .finish()
    }
}

impl PartialEq for SecretRef {
    fn eq(&self, other: &Self) -> bool {
        self.secret_id == other.secret_id && self.ref_as == other.ref_as
    }
}

impl Eq for SecretRef {}

impl std::hash::Hash for SecretRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.secret_id.hash(state);
        self.ref_as.hash(state);
    }
}

impl Expr for SecretRef {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn try_to_expr_proto(&self) -> Result<ExprNode, String> {
        Ok(ExprNode {
            function_type: Type::Unspecified.into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::SecretRef(SecretRefNode {
                secret_id: self.secret_id.as_raw_id(),
                ref_as: self.ref_as.into(),
            })),
        })
    }
}
