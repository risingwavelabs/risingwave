use risingwave_common::expr::AggKind;
use risingwave_common::types::DataTypeKind;

use super::{BoundExpr, BoundExprImpl};

#[derive(Clone, Debug)]
pub struct BoundAggCall {
    agg_kind: AggKind,
    return_type: DataTypeKind,
    inputs: Vec<BoundExprImpl>,
}
impl BoundAggCall {
    #![allow(unreachable_code)]
    #![allow(unused_variables)]
    #![allow(clippy::diverging_sub_expression)]
    pub fn new(agg_kind: AggKind, inputs: Vec<BoundExprImpl>) -> Option<Self> {
        let return_type = todo!(); // should be derived from inputs
        Some(BoundAggCall {
            agg_kind,
            return_type,
            inputs,
        })
    }
    pub fn decompose(self) -> (AggKind, Vec<BoundExprImpl>) {
        (self.agg_kind, self.inputs)
    }
    pub fn agg_kind(&self) -> AggKind {
        self.agg_kind.clone()
    }
}
impl BoundExpr for BoundAggCall {
    fn return_type(&self) -> DataTypeKind {
        self.return_type
    }
    fn bound_expr(self) -> BoundExprImpl {
        BoundExprImpl::AggCall(Box::new(self))
    }
}
