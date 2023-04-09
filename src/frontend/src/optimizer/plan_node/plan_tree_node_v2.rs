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

pub trait PlanTreeNodeV2 {
    type PlanRef;

    fn inputs(&self) -> SmallVec<[Self::PlanRef; 2]>;
    fn clone_with_inputs(&self, inputs: impl Iterator<Item = Self::PlanRef>) -> Self;
}

macro_rules! impl_plan_tree_node_v2_for_stream_leaf_node {
    ($node_type:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                assert!(inputs.next().is_none(), "expect exactly no input");
                self.clone()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_stream_unary_node {
    ($node_type:ident, $input_field:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![self.$input_field.clone()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$input_field = inputs.next().expect("expect exactly 1 input");
                assert!(inputs.next().is_none(), "expect exactly 1 input");
                new.clone()
            }
        }
    };
}

// macro_rules! impl_plan_tree_node_v2_for_stream_binary_node {
//     ($node_type:ident, $first_input_field:ident, $second_input_field:ident) => {
//         impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
//             type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

//             fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
//                 smallvec::smallvec![
//                     self.$first_input_field.clone(),
//                     self.$second_input_field.clone()
//                 ]
//             }

//             fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self
// {                 let mut new = self.clone();
//                 new.$first_input_field = inputs.next().expect("expect exactly 2 input");
//                 new.$second_input_field = inputs.next().expect("expect exactly 2 input");
//                 assert!(inputs.next().is_none(), "expect exactly 2 input");
//                 new.clone()
//             }
//         }
//     };
// }

macro_rules! impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating {
    ($node_type:ident, $core_field:ident, $input_field:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![self.$core_field.$input_field.clone()]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$core_field.$input_field = inputs.next().expect("expect exactly 1 input");
                assert!(inputs.next().is_none(), "expect exactly 1 input");
                new.clone()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_v2_for_stream_binary_node_with_core_delegating {
    ($node_type:ident, $core_field:ident, $first_input_field:ident, $second_input_field:ident) => {
        impl crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2 for $node_type {
            type PlanRef = crate::optimizer::plan_node::stream::PlanRef;

            fn inputs(&self) -> smallvec::SmallVec<[Self::PlanRef; 2]> {
                smallvec::smallvec![
                    self.$core_field.$first_input_field.clone(),
                    self.$core_field.$second_input_field.clone()
                ]
            }

            fn clone_with_inputs(&self, mut inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
                let mut new = self.clone();
                new.$core_field.$first_input_field = inputs.next().expect("expect exactly 2 input");
                new.$core_field.$second_input_field =
                    inputs.next().expect("expect exactly 2 input");
                assert!(inputs.next().is_none(), "expect exactly 2 input");
                new.clone()
            }
        }
    };
}
