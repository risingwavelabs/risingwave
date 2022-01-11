use risingwave_pb::data::DataType;
use risingwave_pb::expr::expr_node;

use super::BoundExpr;
#[derive(Clone)]
pub struct BoundInputRef {
    index: usize,
    data_type: DataType,
}
impl BoundInputRef {
    pub fn new(index: usize, data_type: DataType) -> Self {
        BoundInputRef { index, data_type }
    }
    pub fn get_expr_type(&self) -> expr_node::Type {
        expr_node::Type::InputRef
    }
}
impl BoundExpr for BoundInputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }
}
impl std::fmt::Debug for BoundInputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.index)
    }
}
