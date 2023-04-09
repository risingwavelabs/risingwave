// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use smallvec::SmallVec;

use super::PlanRef;
use crate::utils::ColIndexMapping;

/// The trait [`PlanNode`](super::PlanNode) really need about tree structure and used by optimizer
/// framework. every plan node should impl it.
///
/// The trait [`PlanTreeNodeLeaf`], [`PlanTreeNodeUnary`] and [`PlanTreeNodeBinary`], is just
/// special cases for [`PlanTreeNode`]. as long as you impl these trait for a plan node, we can
/// easily impl the [`PlanTreeNode`] which is really need by framework with helper macros
/// `impl_plan_tree_node_for_leaf`, `impl_plan_tree_node_for_unary` and
/// `impl_plan_tree_node_for_binary`. We can't auto `impl PlanTreeNode for
/// PlanTreeNodeLeaf/Unary/Binary`, because compiler doesn't know they are disjoint and thinks there
/// are conflicting implementation.
///
/// And due to these three traits need not be used as dyn, it can return `Self` type, which is
/// useful when implement rules and visitors. So we highly recommend not impl the [`PlanTreeNode`]
/// trait directly, instead use these tree trait and impl [`PlanTreeNode`] use these helper
/// macros.
pub trait PlanTreeNode {
    /// Get input nodes of the plan.
    fn inputs(&self) -> SmallVec<[PlanRef; 2]>;

    /// Clone the node with a list of new inputs.
    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef;
}

/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeLeaf: Clone {}

/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeUnary {
    fn input(&self) -> PlanRef;
    #[must_use]
    fn clone_with_input(&self, input: PlanRef) -> Self;

    /// Rewrites the plan node according to the schema change of its input node during rewriting.
    ///
    /// This function can be used to implement [`prune_col`](super::ColPrunable::prune_col) or
    /// [`logical_rewrite_for_stream`](super::ToStream::logical_rewrite_for_stream)
    #[must_use]
    fn rewrite_with_input(
        &self,
        _input: PlanRef,
        _input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping)
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeBinary {
    fn left(&self) -> PlanRef;
    fn right(&self) -> PlanRef;

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self;

    /// Rewrites the plan node according to the schema change of its input nodes during rewriting.
    ///
    /// This function can be used to implement [`prune_col`](super::ColPrunable::prune_col) or
    /// [`logical_rewrite_for_stream`](super::ToStream::logical_rewrite_for_stream)
    #[must_use]
    fn rewrite_with_left_right(
        &self,
        _left: PlanRef,
        _left_col_change: ColIndexMapping,
        _right: PlanRef,
        _right_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping)
    where
        Self: Sized,
    {
        unimplemented!()
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
                self.clone().into()
            }
        }

        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $leaf_node_type {
            type PlanRef = crate::optimizer::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
                smallvec::smallvec![]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                assert!(inputs.next().is_none(), "expect exactly no input");
                self.clone()
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
                self.clone_with_input(inputs[0].clone()).into()
            }
        }

        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $unary_node_type {
            type PlanRef = crate::optimizer::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
                smallvec::smallvec![self.input()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let input = inputs.next().expect("expect exactly 1 input");
                assert!(inputs.next().is_none(), "expect exactly 1 input");
                self.clone_with_input(input).into()
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
                    .into()
            }
        }
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $binary_node_type {
            type PlanRef = crate::optimizer::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
                smallvec::smallvec![self.left(), self.right()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let left = inputs.next().expect("expect exactly 2 input");
                let right = inputs.next().expect("expect exactly 2 input");
                assert!(inputs.next().is_none(), "expect exactly 2 input");
                self.clone_with_left_right(left, right).into()
            }
        }
    };
}
