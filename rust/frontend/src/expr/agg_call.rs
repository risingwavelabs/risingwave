use risingwave_common::error::Result;
use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;

use super::{Expr, ExprImpl};

#[derive(Clone, PartialEq)]
pub struct AggCall {
    agg_kind: AggKind,
    return_type: DataType,
    inputs: Vec<ExprImpl>,
}

impl std::fmt::Debug for AggCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("AggCall")
                .field("agg_kind", &self.agg_kind)
                .field("return_type", &self.return_type)
                .field("inputs", &self.inputs)
                .finish()
        } else {
            let mut builder = f.debug_tuple(&format!("{:?}", self.agg_kind));
            self.inputs.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()
        }
    }
}

impl AggCall {
    #![allow(clippy::diverging_sub_expression)]
    /// Returns error if the function name matches with an existing function
    /// but with illegal arguments.
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
