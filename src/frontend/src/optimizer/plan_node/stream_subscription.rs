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

use std::assert_matches::assert_matches;
use std::collections::HashSet;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, TableId, UserId};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::derive::{derive_columns, derive_pk};
use super::expr_visitable::ExprVisitable;
use super::stream::prelude::{GenericPlanRef, PhysicalPlanRef};
use super::utils::{childless_record, infer_kv_log_store_table_catalog_inner, Distill, IndicesDisplay};
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode};
use crate::catalog::subscription_catalog::{SubscriptionCatalog, SubscriptionId};
use crate::error::Result;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{PlanRef, TableCatalog, WithOptions};

/// [`StreamSubscription`] represents a subscription at the very end of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSubscription {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    subscription_catalog: SubscriptionCatalog,
}

impl StreamSubscription {
    #[must_use]
    pub fn new(input: PlanRef, subscription_catalog: SubscriptionCatalog) -> Self {
        let base = input
            .plan_base()
            .into_stream()
            .expect("input should be stream plan")
            .clone_with_new_plan_id();
        Self {
            base,
            input,
            subscription_catalog,
        }
    }

    pub fn subscription_catalog(&self) -> SubscriptionCatalog {
        self.subscription_catalog.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        database_id: u32,
        schema_id: u32,
        dependent_relations: HashSet<TableId>,
        input: PlanRef,
        name: String,
        subscription_from_name: String,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        user_cols: FixedBitSet,
        out_names: Vec<String>,
        definition: String,
        properties: WithOptions,
        user_id: UserId,
    ) -> Result<Self> {
        let columns = derive_columns(input.schema(), out_names, &user_cols)?;
        let (input, subscription) = Self::derive_subscription_catalog(
            database_id,
            schema_id,
            dependent_relations,
            input,
            user_distributed_by,
            name,
            subscription_from_name,
            user_order_by,
            columns,
            definition,
            properties,
            user_id,
        )?;
        Ok(Self::new(input, subscription))
    }

    #[allow(clippy::too_many_arguments)]
    fn derive_subscription_catalog(
        database_id: u32,
        schema_id: u32,
        dependent_relations: HashSet<TableId>,
        input: PlanRef,
        user_distributed_by: RequiredDist,
        name: String,
        subscription_from_name: String,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        properties: WithOptions,
        user_id: UserId,
    ) -> Result<(PlanRef, SubscriptionCatalog)> {
        let (pk, _) = derive_pk(input.clone(), user_order_by, &columns);
        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => {
                assert_matches!(user_distributed_by, RequiredDist::Any);
                RequiredDist::shard_by_key(input.schema().len(), input.expect_stream_key())
            }
        };
        let input = required_dist.enforce_if_not_satisfies(input, &Order::any())?;
        let distribution_key = input.distribution().dist_column_indices().to_vec();
        let subscription_desc = SubscriptionCatalog {
            database_id,
            schema_id,
            dependent_relations: dependent_relations.into_iter().collect(),
            id: SubscriptionId::placeholder(),
            name,
            subscription_from_name,
            definition,
            columns,
            plan_pk: pk,
            distribution_key,
            properties: properties.into_inner(),
            owner: user_id,
            initialized_at_epoch: None,
            created_at_epoch: None,
            created_at_cluster_version: None,
            initialized_at_cluster_version: None,
        };
        Ok((input, subscription_desc))
    }

    /// The table schema is: | epoch | seq id | row op | subscription columns |
    /// Pk is: | epoch | seq id |
    fn infer_kv_log_store_table_catalog(&self) -> TableCatalog {
        infer_kv_log_store_table_catalog_inner(
            &self.input,
            &self.subscription_catalog.columns,
        )
    }
}

impl PlanTreeNodeUnary for StreamSubscription {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.subscription_catalog.clone())
        // TODO(nanderstabel): Add assertions (assert_eq!)
    }
}

impl_plan_tree_node_for_unary! { StreamSubscription }

impl Distill for StreamSubscription {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let column_names = self
            .subscription_catalog
            .columns
            .iter()
            .map(|col| col.name_with_hidden().to_string())
            .map(Pretty::from)
            .collect();
        let column_names = Pretty::Array(column_names);
        let mut vec = Vec::with_capacity(2);
        vec.push(("columns", column_names));
        let pk = IndicesDisplay {
            indices: &self
                .subscription_catalog
                .plan_pk
                .iter()
                .map(|k| k.column_index)
                .collect_vec(),
            schema: self.base.schema(),
        };
        vec.push(("pk", pk.distill()));
        childless_record("Streamsubscription", vec)
    }
}

impl StreamNode for StreamSubscription {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        // We need to create a table for subscription with a kv log store.
        let table = self
            .infer_kv_log_store_table_catalog()
            .with_id(state.gen_table_id_wrapped());

        PbNodeBody::Subscription(SubscriptionNode {
            subscription_catalog: Some(self.subscription_catalog.to_proto()),
            log_store_table: Some(table.to_internal_table_prost()),
        })
    }
}

impl ExprRewritable for StreamSubscription {}

impl ExprVisitable for StreamSubscription {}
