// Copyright 2026 RisingWave Labs
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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_pb::batch_plan::PbFullTextSearchNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, to_batch_query_epoch};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeLeaf, ToDistributedBatch,
    ToLocalBatch, TryToBatchPb,
};
use crate::OptimizerContextRef;
use crate::catalog::TableId;
use crate::catalog::index_catalog::FullTextIndex;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};
use crate::scheduler::SchedulerResult;

/// Searches a full-text index and outputs the distinct stream keys of the primary table whose
/// indexed document contains all `tokens`. The output is exact as of the query epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchFullTextSearch {
    pub base: PlanBase<Batch>,
    index_name: String,
    index_table_id: TableId,
    /// The table whose rows are indexed.
    primary_table_id: TableId,
    /// The original query string, for explain only.
    query: String,
    tokens: Vec<String>,
    as_of: Option<AsOf>,
    limit: Option<u64>,
}

impl PlanTreeNodeLeaf for BatchFullTextSearch {}
impl_plan_tree_node_for_leaf! { Batch, BatchFullTextSearch }

impl BatchFullTextSearch {
    pub fn new(
        ctx: OptimizerContextRef,
        index: &FullTextIndex,
        primary_table_id: TableId,
        query: String,
        tokens: Vec<String>,
        as_of: Option<AsOf>,
    ) -> Self {
        // The index table may also contain system columns after the key columns.
        let schema = Schema::new(
            index.index_table.columns[1..=index.key_columns.len()]
                .iter()
                .map(|col| Field::from(&col.column_desc))
                .collect(),
        );
        let base = PlanBase::new_batch(ctx, schema, Distribution::Single, Order::any());
        Self {
            base,
            index_name: index.index_table.name.clone(),
            index_table_id: index.index_table.id,
            primary_table_id,
            query,
            tokens,
            as_of,
            limit: None,
        }
    }

    pub fn primary_table_id(&self) -> TableId {
        self.primary_table_id
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub fn clone_with_limit(&self, limit: u64) -> Self {
        Self {
            limit: Some(limit),
            ..self.clone()
        }
    }
}

impl Distill for BatchFullTextSearch {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let output = self
            .schema()
            .fields()
            .iter()
            .map(|field| Pretty::from(field.name.clone()))
            .collect();
        let tokens = self
            .tokens
            .iter()
            .map(|token| Pretty::from(token.clone()))
            .collect();
        let mut fields = vec![
            ("index", Pretty::from(self.index_name.clone())),
            ("query", Pretty::debug(&self.query)),
            ("tokens", Pretty::Array(tokens)),
            ("output", Pretty::Array(output)),
        ];
        if let Some(limit) = self.limit {
            fields.push(("limit", Pretty::display(&limit)));
        }
        childless_record("BatchFullTextSearch", fields)
    }
}

impl TryToBatchPb for BatchFullTextSearch {
    fn try_to_batch_prost_body(&self) -> SchedulerResult<NodeBody> {
        Ok(NodeBody::FullTextSearch(PbFullTextSearchNode {
            table_id: self.index_table_id.as_raw_id(),
            tokens: self.tokens.clone(),
            key_types: self
                .schema()
                .fields()
                .iter()
                .map(|field| field.data_type.to_protobuf())
                .collect(),
            query_epoch: to_batch_query_epoch(&self.as_of)?,
            limit: self.limit,
        }))
    }
}

impl ToDistributedBatch for BatchFullTextSearch {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone().into())
    }
}

impl ToLocalBatch for BatchFullTextSearch {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone().into())
    }
}

impl ExprRewritable<Batch> for BatchFullTextSearch {}

impl ExprVisitable for BatchFullTextSearch {}
