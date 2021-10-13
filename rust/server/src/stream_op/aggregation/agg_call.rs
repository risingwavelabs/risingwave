use crate::expr::AggKind;
use crate::types::DataTypeRef;

/// An aggregation function may accept 0, 1 or 2 arguments.
pub enum AggArgs {
    None([DataTypeRef; 0], [usize; 0]),
    Unary([DataTypeRef; 1], [usize; 1]),
    Binary([DataTypeRef; 2], [usize; 2]),
}

impl AggArgs {
    /// return the types of arguments.
    pub fn arg_types(&self) -> &[DataTypeRef] {
        use AggArgs::*;
        match self {
            None(typs, _) => typs,
            Unary(typs, _) => typs,
            Binary(typs, _) => typs,
        }
    }

    /// return the indices of the arguments in [`StreamChunk`].
    pub fn val_indices(&self) -> &[usize] {
        use AggArgs::*;
        match self {
            None(_, val_indices) => val_indices,
            Unary(_, val_indices) => val_indices,
            Binary(_, val_indices) => val_indices,
        }
    }
}

/// Represents an aggregation function.
pub struct AggCall {
    /// Aggregation Kind for constructing [`StreamingAggStateImpl`]
    pub kind: AggKind,
    /// Arguments of aggregation function input.
    pub args: AggArgs,
    /// The return type of aggregation function.
    pub return_type: DataTypeRef,
}
