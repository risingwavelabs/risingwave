use itertools::Itertools;
use paste::paste;

use super::super::plan_node::*;
use super::super::property::Convention;
use crate::for_all_plan_nodes;

macro_rules! def_rewriter {
  ([], $({ $convention:ident, $name:ident }),*) => {

    /// it's kind of like a [`PlanVisitor<PlanRef>`](super::PlanVisitor), but with default behaviour of each rewrite method
    pub trait PlanRewriter {
    fn check_convention(&self, _convention: Convention) -> bool {
      return true;
    }
    paste! {
      fn rewrite(&mut self, plan: PlanRef) -> PlanRef{
        match plan.node_type() {
        $(
          PlanNodeType::[<$convention $name>] => self.[<rewrite_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
        )*
        }
      }

      $(
        #[doc = "Visit [`" [<$convention $name>] "`] , the function should rewrite the children."]
        fn [<rewrite_ $convention:snake _ $name:snake>](&mut self, plan: &[<$convention $name>]) -> PlanRef {
          let new_children = plan
          .children()
          .into_iter()
          .map(|child| self.rewrite(child.clone()))
          .collect_vec();
          plan.clone_with_children(&new_children)
        }
      )*
      }
    }
  }
}
for_all_plan_nodes! { def_rewriter }
