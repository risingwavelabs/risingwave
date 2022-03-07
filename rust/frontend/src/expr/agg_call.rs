use risingwave_common::error::Result;
use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;

use super::{Expr, ExprImpl};

#[derive(Clone, Debug)]
pub struct AggCall {
    agg_kind: AggKind,
    return_type: DataType,
    inputs: Vec<ExprImpl>,
}
impl AggCall {
    #![allow(clippy::diverging_sub_expression)]
    /// Returns error if the function name matches with an existing function
    /// but with illegal arguments. `Ok(None)` is returned when there's no matching
    /// function.
    pub fn new(agg_kind: AggKind, inputs: Vec<ExprImpl>) -> Result<Self> {
        // TODO(TaoWu): Add arguments validator.
        let return_type = match agg_kind {
            AggKind::Min => inputs.get(0).unwrap().return_type(),
            AggKind::Max => inputs.get(0).unwrap().return_type(),
            AggKind::Sum => DataType::Int64,
            AggKind::Count => DataType::Int64,
            _ => todo!(),
        }; // should be derived from inputs
        Ok(AggCall {
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
        self.return_type.clone()
    }
}
