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

use risingwave_common::catalog::Schema;

use super::{generic, EqJoinPredicate, PlanNodeId};
use crate::optimizer::property::{Distribution, FunctionalDependencySet};
use crate::session::OptimizerContextRef;
use crate::utils::Condition;
use crate::{TableCatalog, WithOptions};

macro_rules! impl_node {
($base:ident, $($t:ident),*) => {
    #[derive(Debug, Clone)]
    pub enum Node {
        $($t(Box<$t>),)*
    }
    $(
    impl From<$t> for Node {
        fn from(o: $t) -> Node {
            Node::$t(Box::new(o))
        }
    }
    )*
    pub type PlanOwned = ($base, Node);
    pub type PlanRef = std::rc::Rc<PlanOwned>;
};
}

impl generic::GenericPlanRef for PlanRef {
    fn schema(&self) -> &Schema {
        &self.0.schema
    }

    fn distribution(&self) -> &Distribution {
        &self.0.dist
    }

    fn append_only(&self) -> bool {
        self.0.append_only
    }
}

impl generic::GenericBase for PlanBase {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn logical_pk(&self) -> &[usize] {
        &self.logical_pk
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}

/// Implements [`generic::Join`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone)]
pub struct DeltaJoin {
    pub logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,
}

#[derive(Clone, Debug)]
pub struct DynamicFilter {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    pub predicate: Condition,
    // dist_key_l: Distribution,
    pub left_index: usize,
    pub left: PlanRef,
    pub right: PlanRef,
}

#[derive(Debug, Clone)]
pub struct Exchange(pub PlanRef);

#[derive(Debug, Clone)]
pub struct Expand(pub generic::Expand<PlanRef>);

#[derive(Debug, Clone)]
pub struct Filter(pub generic::Filter<PlanRef>);

#[derive(Debug, Clone)]
pub struct GlobalSimpleAgg(pub generic::Agg<PlanRef>);

#[derive(Debug, Clone)]
pub struct GroupTopN {
    pub logical: generic::TopN<PlanRef>,
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct HashAgg {
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
    pub logical: generic::Agg<PlanRef>,
}

/// Implements [`generic::Join`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct HashJoin {
    pub logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    pub is_append_only: bool,
}

#[derive(Debug, Clone)]
pub struct HopWindow(pub generic::HopWindow<PlanRef>);

/// [`IndexScan`] is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request. Compared with [`TableScan`], it will reorder columns, and the chain node
/// doesn't allow rearrange.
#[derive(Debug, Clone)]
pub struct IndexScan {
    pub logical: generic::Scan,
    pub batch_plan_id: PlanNodeId,
}

/// Local simple agg.
///
/// Should only be used for stateless agg, including `sum`, `count` and *append-only* `min`/`max`.
///
/// The output of `LocalSimpleAgg` doesn't have pk columns, so the result can only
/// be used by `GlobalSimpleAgg` with `ManagedValueState`s.
#[derive(Debug, Clone)]
pub struct LocalSimpleAgg(pub generic::Agg<PlanRef>);

#[derive(Debug, Clone)]
pub struct Materialize {
    /// Child of Materialize plan
    pub input: PlanRef,
    pub table: TableCatalog,
}

#[derive(Debug, Clone)]
pub struct ProjectSet(pub generic::ProjectSet<PlanRef>);

/// `Project` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone)]
pub struct Project(pub generic::Project<PlanRef>);

/// [`Sink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone)]
pub struct Sink {
    pub input: PlanRef,
    pub properties: WithOptions,
}

/// [`Source`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct Source(pub generic::Source);

/// `TableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone)]
pub struct TableScan {
    pub logical: generic::Scan,
    pub batch_plan_id: PlanNodeId,
}

/// `TopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone)]
pub struct TopN(pub generic::TopN<PlanRef>);

#[derive(Clone, Debug)]
pub struct PlanBase {
    pub id: PlanNodeId,
    pub ctx: OptimizerContextRef,
    pub schema: Schema,
    pub logical_pk: Vec<usize>,
    pub dist: Distribution,
    pub append_only: bool,
    pub functional_dependency: FunctionalDependencySet,
}

impl_node!(
    PlanBase,
    Exchange,
    DynamicFilter,
    DeltaJoin,
    Expand,
    Filter,
    GlobalSimpleAgg,
    GroupTopN,
    HashAgg,
    HashJoin,
    HopWindow,
    IndexScan,
    LocalSimpleAgg,
    Materialize,
    ProjectSet,
    Project,
    Sink,
    Source,
    TableScan,
    TopN
);
