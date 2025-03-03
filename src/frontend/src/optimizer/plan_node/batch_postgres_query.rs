// Copyright 2025 RisingWave Labs
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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::PostgresQueryNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchPostgresQuery {
    pub base: PlanBase<Batch>,
    pub core: generic::PostgresQuery,
}

impl BatchPostgresQuery {
    pub fn new(core: generic::PostgresQuery) -> Self {
        let base = PlanBase::new_batch_with_core(&core, Distribution::Single, Order::any());

        Self { base, core }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn clone_with_dist(&self) -> Self {
        let base = self.base.clone_with_new_distribution(Distribution::Single);
        Self {
            base,
            core: self.core.clone(),
        }
    }
}

impl_plan_tree_node_for_leaf! { BatchPostgresQuery }

impl Distill for BatchPostgresQuery {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("BatchPostgresQuery", fields)
    }
}

impl ToLocalBatch for BatchPostgresQuery {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchPostgresQuery {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchPostgresQuery {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::PostgresQuery(PostgresQueryNode {
            columns: self
                .core
                .columns()
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            hostname: self.core.hostname.clone(),
            port: self.core.port.clone(),
            username: self.core.username.clone(),
            password: self.core.password.clone(),
            database: self.core.database.clone(),
            query: self.core.query.clone(),
        })
    }
}

impl ExprRewritable for BatchPostgresQuery {}

impl ExprVisitable for BatchPostgresQuery {}
