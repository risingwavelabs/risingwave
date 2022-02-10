use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;

use super::{Expr, ExprImpl};

#[derive(Clone, Debug, PartialEq)]
pub struct AggCall {
    agg_kind: AggKind,
    return_type: DataType,
    inputs: Vec<ExprImpl>,
}
impl AggCall {
    #![allow(unreachable_code)]
    #![allow(unused_variables)]
    #![allow(clippy::diverging_sub_expression)]
    pub fn new(agg_kind: AggKind, inputs: Vec<ExprImpl>) -> Option<Self> {
        let return_type = todo!(); // should be derived from inputs
        Some(AggCall {
            agg_kind,
            return_type,
            inputs,
        })
    }
    pub fn decompose(self) -> (AggKind, Vec<ExprImpl>) {
        (self.agg_kind, self.inputs)
    }
    pub fn agg_kind(&self) -> AggKind {
        self.agg_kind.clone()
    }

    /// Get a reference to the agg call's inputs.
    pub fn inputs(&self) -> &[ExprImpl] {
        self.inputs.as_ref()
    }
}
impl Expr for AggCall {
    fn return_type(&self) -> DataType {
        self.return_type
    }
    fn bound_expr(self) -> ExprImpl {
        ExprImpl::AggCall(Box::new(self))
    }
}
