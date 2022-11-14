// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use paste::paste;

use crate::for_all_plan_nodes;
use crate::optimizer::plan_node::*;

/// Define `PlanRewriter` trait.
macro_rules! def_rewriter {
    ($({ $convention:ident, $name:ident }),*) => {

        /// it's kind of like a [`PlanVisitor<PlanRef>`](super::plan_visitor::PlanVisitor), but with default behaviour of each rewrite method
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
                    #[doc = "Visit [`" [<$convention $name>] "`] , the function should rewrite the inputs."]
                    fn [<rewrite_ $convention:snake _ $name:snake>](&mut self, plan: &[<$convention $name>]) -> PlanRef {
                        let new_inputs = plan
                            .inputs()
                            .into_iter()
                            .map(|input| self.rewrite(input.clone()))
                            .collect_vec();
                        plan.clone_with_inputs(&new_inputs)
                    }
                )*
            }
        }
    }
}
for_all_plan_nodes! { def_rewriter }
