use fixedbitset::FixedBitSet;
use paste::paste;

use super::*;
use crate::expr::{ExprImpl, ExprVisitor, InputRef};
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable {
    /// Transform the plan node to only output the required columns ordered by index number.
    ///
    /// `required_cols` must be a subset of the range `0..self.schema().len()`.
    ///
    /// After calling `prune_col` on the children, their output schema may change, so
    /// the caller may need to transform its [`InputRef`] using
    /// [`ColIndexMapping`](crate::utils::ColIndexMapping).
    ///
    /// When implementing this method for a node, it may require its children to produce additional
    /// columns besides `required_cols`. In this case, it may need to insert a
    /// [`LogicalProject`](super::LogicalProject) above to have a correct schema.
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef;
}

/// impl ColPrunable for batch and streaming node.
macro_rules! ban_prune_col {
  ([], $( { $convention:ident, $name:ident }),*) => {
    paste!{
      $(impl ColPrunable for [<$convention $name>] {
        fn prune_col(&self, _required_cols: &FixedBitSet) -> PlanRef {
          panic!("column pruning is only allowed on logical plan")
        }
     })*
    }
  }
}
for_batch_plan_nodes! { ban_prune_col }
for_stream_plan_nodes! { ban_prune_col }

/// Union all `InputRef`s' indexes in the expression with the initial `input_bits`.
pub struct CollectInputRef {
    pub input_bits: FixedBitSet,
}

impl ExprVisitor for CollectInputRef {
    fn visit_input_ref(&mut self, expr: &InputRef) {
        self.input_bits.insert(expr.index());
    }
}

impl CollectInputRef {
    pub fn collect(expr: &ExprImpl, capacity: usize) -> FixedBitSet {
        let mut visitor = Self {
            input_bits: FixedBitSet::with_capacity(capacity),
        };
        visitor.visit_expr(expr);
        visitor.input_bits
    }
}
