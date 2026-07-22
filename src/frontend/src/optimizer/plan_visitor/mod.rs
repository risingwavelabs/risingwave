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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

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
mod locality_provider_counter;
mod rw_timestamp_validator;
mod sole_sys_table_visitor;
pub use distributed_dml_visitor::*;
pub use locality_provider_counter::*;
pub use rw_timestamp_validator::*;
pub use sole_sys_table_visitor::*;

#[cfg(feature = "datafusion")]
mod datafusion_execute_checker;
#[cfg(feature = "datafusion")]
pub use datafusion_execute_checker::*;
#[cfg(feature = "datafusion")]
mod datafusion_plan_converter;
#[cfg(feature = "datafusion")]
pub use datafusion_plan_converter::*;

use crate::for_each_convention_all_plan_nodes;
use crate::optimizer::ShareId;
use crate::optimizer::plan_node::*;

type VisitorKey = (&'static str, usize);

#[derive(Default)]
struct VisitorTraversalState {
    depth: usize,
    visited_shares: HashSet<ShareId>,
}

thread_local! {
    static VISITOR_TRAVERSALS: RefCell<HashMap<VisitorKey, VisitorTraversalState>> =
        RefCell::new(HashMap::new());
}

struct VisitorTraversalGuard {
    key: VisitorKey,
}

impl VisitorTraversalGuard {
    fn enter<V: ?Sized>(convention: &'static str, visitor: &mut V) -> Self {
        let key = (convention, visitor as *mut V as *mut () as usize);
        VISITOR_TRAVERSALS.with_borrow_mut(|traversals| {
            traversals.entry(key).or_default().depth += 1;
        });
        Self { key }
    }
}

impl Drop for VisitorTraversalGuard {
    fn drop(&mut self) {
        VISITOR_TRAVERSALS.with_borrow_mut(|traversals| {
            let state = traversals
                .get_mut(&self.key)
                .expect("visitor traversal must exist");
            state.depth -= 1;
            if state.depth == 0 {
                traversals.remove(&self.key);
            }
        });
    }
}

fn visit_share_once<V: ?Sized>(
    convention: &'static str,
    visitor: &mut V,
    share_id: ShareId,
) -> bool {
    let key = (convention, visitor as *mut V as *mut () as usize);
    VISITOR_TRAVERSALS.with_borrow_mut(|traversals| {
        traversals
            .get_mut(&key)
            .is_none_or(|state| state.visited_shares.insert(share_id))
    })
}

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

pub trait PlanVisitor<C: ConventionMarker> {
    type Result;
    fn visit(&mut self, plan: PlanRef<C>) -> Self::Result;
}

macro_rules! def_visit_method {
    ($convention:ident, Share) => {
        paste! {
            #[doc = "Visit [`" [<$convention Share>] "`] once per stable share identity and merge its input. Repeated edges return the default behavior for an empty input set; visitors for which that is not a neutral value must override this method."]
            fn [<visit_ $convention:snake _share>](&mut self, plan: &[<$convention Share>]) -> Self::Result {
                if !visit_share_once(stringify!($convention), self, plan.share_id()) {
                    return Self::default_behavior().apply(std::iter::empty());
                }
                let results = plan.inputs().into_iter().map(|input| self.[<visit_ $convention:snake>](input));
                Self::default_behavior().apply(results)
            }
        }
    };
    ($convention:ident, $name:ident) => {
        paste! {
            #[doc = "Visit [`" [<$convention $name>] "`] and merge its inputs."]
            fn [<visit_ $convention:snake _ $name:snake>](&mut self, plan: &[<$convention $name>]) -> Self::Result {
                let results = plan.inputs().into_iter().map(|input| self.[<visit_ $convention:snake>](input));
                Self::default_behavior().apply(results)
            }
        }
    };
}

/// Define `PlanVisitor` trait.
macro_rules! def_visitor {
    ({
        $( $convention:ident, { $( $name:ident ),* }),*
    }) => {
        paste! {
            $(
                /// The visitor for plan nodes. visit all inputs and return the ret value of the left most input,
                /// and leaf node returns `R::default()`
                pub trait [<$convention  PlanVisitor>] {
                    type Result;
                    type DefaultBehavior: DefaultBehavior<Self::Result>;

                    /// The behavior for the default implementations of `visit_xxx`.
                    fn default_behavior() -> Self::DefaultBehavior;

                    fn [<visit_ $convention:snake>](&mut self, plan: PlanRef<$convention>) -> Self::Result {
                        use risingwave_common::util::recursive::{tracker, Recurse};
                        use crate::session::current::notice_to_user;

                        let _traversal_guard =
                            VisitorTraversalGuard::enter(stringify!($convention), self);
                        tracker!().recurse(|t| {
                            if t.depth_reaches(PLAN_DEPTH_THRESHOLD) {
                                notice_to_user(PLAN_TOO_DEEP_NOTICE);
                            }

                            match plan.node_type() {
                                $(
                                    [<$convention PlanNodeType>]::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
                                )*
                            }
                        })
                    }

                    $(
                        def_visit_method!($convention, $name);
                    )*

                }

                impl<V: [<$convention  PlanVisitor>]> PlanVisitor<$convention> for V {
                    type Result = V::Result;
                    fn visit(&mut self, plan: PlanRef<$convention>) -> Self::Result {
                        self.[<visit_ $convention:snake>](plan)
                    }
                }
            )*
        }
    }
}

for_each_convention_all_plan_nodes! { def_visitor }

macro_rules! impl_has_variant {
    ( $({$convention:ident $variant_name:ident}),* ) => {
        paste! {
            $(
                pub fn [<has_ $convention:snake _ $variant_name:snake _where>]<P>(plan: PlanRef<$convention>, pred: P) -> bool
                where
                    P: FnMut(&[<$convention $variant_name>]) -> bool,
                {
                    struct HasWhere<P> {
                        pred: P,
                    }

                    impl<P> [<$convention PlanVisitor>] for HasWhere<P>
                    where
                        P: FnMut(&[<$convention $variant_name>]) -> bool,
                    {
                        type Result = bool;
                        type DefaultBehavior = impl DefaultBehavior<Self::Result>;

                        fn default_behavior() -> Self::DefaultBehavior {
                            Merge(|a, b| a | b)
                        }

                        fn [<visit_ $convention:snake _ $variant_name:snake>](&mut self, node: &[<$convention $variant_name>]) -> Self::Result {
                            (self.pred)(node)
                        }
                    }

                    let mut visitor = HasWhere { pred };
                    visitor.visit(plan)
                }

                #[allow(dead_code)]
                pub fn [<has_ $convention:snake _ $variant_name:snake>](plan: PlanRef<$convention>) -> bool {
                    [<has_ $convention:snake _$variant_name:snake _where>](plan, |_| true)
                }
            )*
        }
    };
}

impl_has_variant! {
    {Logical Apply},
    {Logical MaxOneRow},
    {Logical OverWindow},
    {Logical Scan},
    {Logical Source},
    {Batch Exchange},
    {Batch SeqScan},
    {Batch Source},
    {Batch Insert},
    {Batch Delete},
    {Batch Update}
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;

    #[derive(Default)]
    struct ValuesVisitCounter {
        values_visits: usize,
    }

    impl LogicalPlanVisitor for ValuesVisitCounter {
        type Result = ();

        type DefaultBehavior = impl DefaultBehavior<Self::Result>;

        fn default_behavior() -> Self::DefaultBehavior {
            DefaultValue
        }

        fn visit_logical_values(&mut self, _plan: &LogicalValues) {
            self.values_visits += 1;
        }
    }

    #[tokio::test]
    async fn test_default_visitor_visits_shared_subplan_once() {
        let ctx = OptimizerContext::mock();
        let schema = Schema::new(vec![Field::with_name(DataType::Int32, "v")]);
        let values = LogicalValues::new(vec![], schema, ctx).into();
        let share = LogicalShare::create(values);
        let left: LogicalPlanRef = LogicalProject::with_out_col_idx(share.clone(), 0..1).into();
        let right: LogicalPlanRef = LogicalProject::with_out_col_idx(share, 0..1).into();
        let root = LogicalUnion::create(true, vec![left, right]);

        let mut visitor = ValuesVisitCounter::default();
        visitor.visit(root.clone());
        assert_eq!(visitor.values_visits, 1);

        // Visited state is scoped to one top-level traversal.
        visitor.visit(root);
        assert_eq!(visitor.values_visits, 2);
    }
}
