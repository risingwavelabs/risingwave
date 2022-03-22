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
//
use paste::paste;

use crate::for_all_plan_nodes;
use crate::optimizer::plan_node::*;
use crate::optimizer::property::Convention;

/// Define `PlanVisitor` trait.
macro_rules! def_visitor {
    ([], $({ $convention:ident, $name:ident }),*) => {
        /// The visitor for plan nodes. visit all inputs and return the ret value of the left most input,
        /// and leaf node returns R::default()
        pub trait PlanVisitor<R:Default> {
            fn check_convention(&self, _convention: Convention) -> bool {
                return true;
            }
            paste! {
                fn visit(&mut self, plan: PlanRef) -> R{
                    match plan.node_type() {
                        $(
                            PlanNodeType::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
                        )*
                    }
                }

                $(
                    #[doc = "Visit [`" [<$convention $name>] "`] , the function should visit the inputs."]
                    fn [<visit_ $convention:snake _ $name:snake>](&mut self, plan: &[<$convention $name>]) -> R {
                        let inputs = plan.inputs();
                        if inputs.is_empty() {
                            return R::default();
                        }
                        let mut iter = plan.inputs().into_iter();
                        let ret = self.visit(iter.next().unwrap());
                        iter.for_each(|input| {self.visit(input);});
                        ret
                    }
                )*
            }
        }
    }
}

for_all_plan_nodes! { def_visitor }
