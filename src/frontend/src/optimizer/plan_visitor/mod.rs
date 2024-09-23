// Copyright 2024 RisingWave Labs
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

use paste::paste;
mod apply_visitor;
pub use apply_visitor::*;
mod plan_correlated_id_finder;
pub use plan_correlated_id_finder::*;
mod share_parent_counter;
pub use share_parent_counter::*;

#[cfg(debug_assertions)]
mod input_ref_validator;
#[cfg(debug_assertions)]
pub use input_ref_validator::*;

mod execution_mode_decider;
pub use execution_mode_decider::*;
mod temporal_join_validator;
pub use temporal_join_validator::*;
mod relation_collector_visitor;
mod sys_table_visitor;
pub use relation_collector_visitor::*;
pub use sys_table_visitor::*;
mod side_effect_visitor;
pub use side_effect_visitor::*;
mod cardinality_visitor;
pub use cardinality_visitor::*;
mod jsonb_stream_key_checker;
pub use jsonb_stream_key_checker::*;
mod distributed_dml_visitor;
mod read_storage_table_visitor;
pub use distributed_dml_visitor::*;
pub use read_storage_table_visitor::*;

use crate::for_all_plan_nodes;
use crate::optimizer::plan_node::*;

/// The behavior for the default implementations of `visit_xxx`.
pub trait DefaultBehavior<R> {
    /// Apply this behavior to the plan node with the given results.
    fn apply(&self, results: impl IntoIterator<Item = R>) -> R;
}

/// Visit all input nodes, merge the results with a function.
/// - If there's no input node, return the default value of the result type.
/// - If there's only a single input node, directly return its result.
pub struct Merge<F>(F);

impl<F, R> DefaultBehavior<R> for Merge<F>
where
    F: Fn(R, R) -> R,
    R: Default,
{
    fn apply(&self, results: impl IntoIterator<Item = R>) -> R {
        results.into_iter().reduce(&self.0).unwrap_or_default()
    }
}

/// Visit all input nodes, return the default value of the result type.
pub struct DefaultValue;

impl<R> DefaultBehavior<R> for DefaultValue
where
    R: Default,
{
    fn apply(&self, results: impl IntoIterator<Item = R>) -> R {
        let _ = results.into_iter().count(); // consume the iterator
        R::default()
    }
}

/// Define `PlanVisitor` trait.
macro_rules! def_visitor {
    ($({ $convention:ident, $name:ident }),*) => {
        /// The visitor for plan nodes. visit all inputs and return the ret value of the left most input,
        /// and leaf node returns `R::default()`
        pub trait PlanVisitor {
            type Result: Default;
            type DefaultBehavior: DefaultBehavior<Self::Result>;

            /// The behavior for the default implementations of `visit_xxx`.
            fn default_behavior() -> Self::DefaultBehavior;

            paste! {
                fn visit(&mut self, plan: PlanRef) -> Self::Result {
                    use risingwave_common::util::recursive::{tracker, Recurse};
                    use crate::session::current::notice_to_user;

                    tracker!().recurse(|t| {
                        if t.depth_reaches(PLAN_DEPTH_THRESHOLD) {
                            notice_to_user(PLAN_TOO_DEEP_NOTICE);
                        }

                        match plan.node_type() {
                            $(
                                PlanNodeType::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
                            )*
                        }
                    })
                }

                $(
                    #[doc = "Visit [`" [<$convention $name>] "`] , the function should visit the inputs."]
                    fn [<visit_ $convention:snake _ $name:snake>](&mut self, plan: &[<$convention $name>]) -> Self::Result {
                        let results = plan.inputs().into_iter().map(|input| self.visit(input));
                        Self::default_behavior().apply(results)
                    }
                )*
            }
        }
    }
}

for_all_plan_nodes! { def_visitor }

macro_rules! impl_has_variant {
    ( $($variant:ty),* ) => {
        paste! {
            $(
                pub fn [<has_ $variant:snake _where>]<P>(plan: PlanRef, pred: P) -> bool
                where
                    P: FnMut(&$variant) -> bool,
                {
                    struct HasWhere<P> {
                        pred: P,
                    }

                    impl<P> PlanVisitor for HasWhere<P>
                    where
                        P: FnMut(&$variant) -> bool,
                    {
                        type Result = bool;
                        type DefaultBehavior = impl DefaultBehavior<Self::Result>;

                        fn default_behavior() -> Self::DefaultBehavior {
                            Merge(|a, b| a | b)
                        }

                        fn [<visit_ $variant:snake>](&mut self, node: &$variant) -> Self::Result {
                            (self.pred)(node)
                        }
                    }

                    let mut visitor = HasWhere { pred };
                    visitor.visit(plan)
                }

                #[allow(dead_code)]
                pub fn [<has_ $variant:snake>](plan: PlanRef) -> bool {
                    [<has_ $variant:snake _where>](plan, |_| true)
                }
            )*
        }
    };
}

impl_has_variant! {
    LogicalApply,
    LogicalMaxOneRow,
    LogicalOverWindow,
    LogicalScan,
    LogicalSource,
    BatchExchange,
    BatchSeqScan,
    BatchSource,
    BatchInsert,
    BatchDelete,
    BatchUpdate
}
