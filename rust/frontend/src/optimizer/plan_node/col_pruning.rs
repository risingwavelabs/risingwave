use bit_set::BitSet;
use paste::paste;

use super::*;
use crate::expr::{BoundExpr, BoundExprImpl, BoundInputRef};
use crate::optimizer::property::WithSchema;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable: WithSchema + IntoPlanRef {
    /// transform the plan node to only output the required columns ordered by index number.
    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let schema = self.schema();
        let exprs: Vec<BoundExprImpl> = required_cols
            .iter()
            .map(|i| BoundInputRef::new(i, schema.fields()[i].data_type()).bound_expr())
            .collect();
        let alias = vec![None; required_cols.len()];
        LogicalProject::create(self.clone_as_plan_ref(), exprs, alias)
    }
}

/// impl ColPrunable for batch and streaming node.
macro_rules! ban_prune_col {
  ([], $( { $convention:ident, $name:ident }),*) => {
    paste!{
      $(impl ColPrunable for [<$convention $name>] {
        fn prune_col(&self, _required_cols: BitSet) -> PlanRef {
          panic!("column pruning is only allowed on logical plan")
        }
     })*
    }
  }
}
for_batch_plan_nodes! { ban_prune_col }
for_stream_plan_nodes! { ban_prune_col }
