use risingwave_common::types::DataType;
use risingwave_pb::expr::ExprNode;

use super::Expr;

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
