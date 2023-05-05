use risingwave_common::types::DataType;
use risingwave_pb::expr::ExprNode;

use super::Expr;

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
        unreachable!(
            "`Now` should be translated to `Literal` in batch mode or `NowNode` in stream mode"
        )
    }
}
