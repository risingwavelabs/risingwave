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

use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::expr::ExprNode;

use super::{Expr, ExprImpl, FunctionCall};

/// `NOW()` in streaming queries, representing a retractable monotonic timestamp stream and will be
/// rewritten to `NowNode` for execution.
///
/// Note that `NOW()` in batch queries have already been rewritten to `Literal` when binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Now;

impl Expr for Now {
    fn return_type(&self) -> DataType {
        DataType::Timestamptz
    }

    fn to_expr_proto(&self) -> ExprNode {
        FunctionCall::new(Type::Now, vec![])
            .unwrap()
            .to_expr_proto()
    }
}
