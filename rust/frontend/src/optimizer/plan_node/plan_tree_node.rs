use smallvec::SmallVec;

use super::PlanRef;
use crate::optimizer::property::{Distribution, Order};

/// the trait [`PlanNode`](super::PlanNode) really need about tree structure and used by optimizer
/// framework. every plan node should impl it.
///
/// the trait [`PlanTreeNodeLeaf`], [`PlanTreeNodeUnary`] and [`PlanTreeNodeBinary`], is just
/// special cases for [`PlanTreeNode`]. as long as you impl these trait for a plan node, we can
/// easily impl the [`PlanTreeNode`] which is really need by framework with helper macros
/// [`impl_plan_tree_node_for_leaf`], [`impl_plan_tree_node_for_unary`] and
/// [`impl_plan_tree_node_for_binary`].
///
/// and due to these three traits need not be used as dyn, it can return `Self` type, which is
/// useful when implement rules and visitors. So we highly recommend not impl the [`PlanTreeNode`]
/// trait directly, instead use these tree trait and impl [`PlanTreeNode`] use these helper
/// macros.
pub trait PlanTreeNode {
    /// Get input nodes of the plan.
    fn inputs(&self) -> SmallVec<[PlanRef; 2]>;

    /// Clone the node with a list of new inputs.
    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef;

    /// return the required [`Distribution`] of each input for the node to matain the
    /// [`Distribution`] property of the current node, Use the default impl will not affect
    /// correctness, but insert unnecessary Exchange in plan
    fn inputs_distribution_required(&self) -> Vec<Distribution> {
        self.inputs()
            .into_iter()
            .map(|plan| plan.distribution())
            .collect()
    }

    /// return the required [`Order`] of each input for the node to matain the [`Order`] property of
    /// the current node, Use the default impl will not affect correctness, but insert unnecessary
    /// Sort in plan
    fn inputs_order_required(&self) -> Vec<Order> {
        self.inputs().into_iter().map(|plan| plan.order()).collect()
    }

    /// return the required  [`Distribution`]  of each input for the node, it is just a hint for
    /// optimizer and it's ok to be wrong, which will not affect correctness, but insert unnecessary
    /// Exchange in plan.
    // Maybe: maybe the return type should be Vec<Vec<Distribution>>, return all possible
    // combination of inputs' distribution, when a cascades introduced
    fn dist_pass_through(&self, _required: &Distribution) -> Vec<Distribution> {
        std::vec::from_elem(Distribution::any(), self.inputs().len())
    }
}

/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeLeaf: Clone {}
/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeUnary {
    fn input(&self) -> PlanRef;
    #[must_use]
    fn clone_with_input(&self, input: PlanRef) -> Self;
    fn input_dist_required(&self) -> Distribution {
        self.input().distribution()
    }
    fn input_order_required(&self) -> Order {
        self.input().order()
    }

    fn dist_pass_through_input(&self, _required: &Distribution) -> Distribution {
        Distribution::any()
    }
}
/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeBinary {
    fn left(&self) -> PlanRef;
    fn right(&self) -> PlanRef;

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self;

    fn left_dist_required(&self) -> Distribution {
        self.left().distribution()
    }

    fn right_dist_required(&self) -> Distribution {
        self.right().distribution()
    }

    fn left_order_required(&self) -> Order {
        self.left().order()
    }

    fn right_order_required(&self) -> Order {
        self.right().order()
    }

    fn dist_pass_through_left_right(
        &self,
        _required: &Distribution,
    ) -> (Distribution, Distribution) {
        (Distribution::any(), Distribution::any())
    }
}

macro_rules! impl_plan_tree_node_for_leaf {
    ($leaf_node_type:ident) => {
        impl crate::optimizer::plan_node::PlanTreeNode for $leaf_node_type {
            fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
                smallvec::smallvec![]
            }

            /// Clone the node with a list of new inputs.
            fn clone_with_inputs(
                &self,
                inputs: &[crate::optimizer::PlanRef],
            ) -> crate::optimizer::PlanRef {
                assert_eq!(inputs.len(), 0);
                self.clone().into_plan_ref()
            }

            fn inputs_distribution_required(
                &self,
            ) -> Vec<crate::optimizer::property::Distribution> {
                vec![]
            }
            fn inputs_order_required(&self) -> Vec<crate::optimizer::property::Order> {
                vec![]
            }
            fn dist_pass_through(
                &self,
                _required: &crate::optimizer::property::Distribution,
            ) -> Vec<crate::optimizer::property::Distribution> {
                vec![]
            }
        }
    };
}

macro_rules! impl_plan_tree_node_for_unary {
    ($unary_node_type:ident) => {
        impl crate::optimizer::plan_node::PlanTreeNode for $unary_node_type {
            fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
                smallvec::smallvec![self.input()]
            }

            /// Clone the node with a list of new inputs.
            fn clone_with_inputs(
                &self,
                inputs: &[crate::optimizer::PlanRef],
            ) -> crate::optimizer::PlanRef {
                assert_eq!(inputs.len(), 1);
                self.clone_with_input(inputs[0].clone()).into_plan_ref()
            }

            fn inputs_distribution_required(
                &self,
            ) -> Vec<crate::optimizer::property::Distribution> {
                vec![self.input_dist_required()]
            }
            fn inputs_order_required(&self) -> Vec<crate::optimizer::property::Order> {
                vec![self.input_order_required()]
            }
            fn dist_pass_through(
                &self,
                required: &crate::optimizer::property::Distribution,
            ) -> Vec<crate::optimizer::property::Distribution> {
                vec![self.dist_pass_through_input(required)]
            }
        }
    };
}

macro_rules! impl_plan_tree_node_for_binary {
    ($binary_node_type:ident) => {
        impl crate::optimizer::plan_node::PlanTreeNode for $binary_node_type {
            fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
                smallvec::smallvec![self.left(), self.right()]
            }
            fn clone_with_inputs(
                &self,
                inputs: &[crate::optimizer::PlanRef],
            ) -> crate::optimizer::PlanRef {
                assert_eq!(inputs.len(), 2);
                self.clone_with_left_right(inputs[0].clone(), inputs[1].clone())
                    .into_plan_ref()
            }
            fn inputs_distribution_required(
                &self,
            ) -> Vec<crate::optimizer::property::Distribution> {
                vec![self.left_dist_required()]
            }
            fn inputs_order_required(&self) -> Vec<crate::optimizer::property::Order> {
                vec![self.right_order_required()]
            }
            fn dist_pass_through(
                &self,
                required: &crate::optimizer::property::Distribution,
            ) -> Vec<crate::optimizer::property::Distribution> {
                let (left_dist, right_dist) = self.dist_pass_through_left_right(required);
                vec![left_dist, right_dist]
            }
        }
    };
}
