use risingwave_common::expr::AggKind;
use risingwave_common::types::DataTypeRef;
use std::slice;

/// An aggregation function may accept 0, 1 or 2 arguments.
#[derive(Clone, Debug)]
pub enum AggArgs {
    /// `None` is used for aggregation function accepts 0 arguments, such as [`AggKind::RowCount`].
    None,
    /// `Unary` is used for aggregation function accepts 1 argument, such as [`AggKind::Sum`].
    Unary(DataTypeRef, usize),
    /// `Binary` is used for aggregation function accepts 2 arguments.
    Binary([DataTypeRef; 2], [usize; 2]),
}

impl AggArgs {
    /// return the types of arguments.
    pub fn arg_types(&self) -> &[DataTypeRef] {
        use AggArgs::*;
        match self {
            None => Default::default(),
            Unary(typ, _) => slice::from_ref(typ),
            Binary(typs, _) => typs,
        }
    }

    /// return the indices of the arguments in [`StreamChunk`].
    pub fn val_indices(&self) -> &[usize] {
        use AggArgs::*;
        match self {
            None => Default::default(),
            Unary(_, val_idx) => slice::from_ref(val_idx),
            Binary(_, val_indices) => val_indices,
        }
    }
}

/// Represents an aggregation function.
#[derive(Clone, Debug)]
pub struct AggCall {
    /// Aggregation Kind for constructing [`StreamingAggStateImpl`]
    pub kind: AggKind,
    /// Arguments of aggregation function input.
    pub args: AggArgs,
    /// The return type of aggregation function.
    pub return_type: DataTypeRef,
}
