mod column_index_mapping;
pub use column_index_mapping::*;
mod condition;
pub use condition::*;

use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};

/// Substitute `InputRef` with corresponding `ExprImpl`.
pub struct Substitute {
    pub mapping: Vec<ExprImpl>,
}

impl ExprRewriter for Substitute {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        assert_eq!(
            self.mapping[input_ref.index()].return_type(),
            input_ref.return_type(),
            "Type mismatch when substituting {:?} with {:?}",
            input_ref,
            self.mapping[input_ref.index()],
        );
        self.mapping[input_ref.index()].clone()
    }
}
