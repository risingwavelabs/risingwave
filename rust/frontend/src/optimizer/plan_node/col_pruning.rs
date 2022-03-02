use fixedbitset::FixedBitSet;
use paste::paste;

use super::*;
use crate::optimizer::property::WithSchema;
use crate::utils::ColIndexMapping;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable: WithSchema + IntoPlanRef {
    /// transform the plan node to only output the required columns ordered by index number.
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols);
        LogicalProject::with_mapping(self.clone_as_plan_ref(), mapping)
    }
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
