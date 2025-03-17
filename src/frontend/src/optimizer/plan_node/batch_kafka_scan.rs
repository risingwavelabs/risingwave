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

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::batch_plan::SourceNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchKafkaScan {
    pub base: PlanBase<Batch>,
    pub core: generic::Source,

    /// Kafka timestamp range.
    kafka_timestamp_range: (Bound<i64>, Bound<i64>),
}

impl BatchKafkaScan {
    pub fn new(core: generic::Source, kafka_timestamp_range: (Bound<i64>, Bound<i64>)) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            // Use `Single` by default, will be updated later with `clone_with_dist`.
            Distribution::Single,
            Order::any(),
        );

        Self {
            base,
            core,
            kafka_timestamp_range,
        }
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn kafka_timestamp_range_value(&self) -> (Option<i64>, Option<i64>) {
        let (lower_bound, upper_bound) = &self.kafka_timestamp_range;
        let lower_bound = match lower_bound {
            Included(t) => Some(*t),
            Excluded(t) => Some(*t - 1),
            Unbounded => None,
        };

        let upper_bound = match upper_bound {
            Included(t) => Some(*t),
            Excluded(t) => Some(*t + 1),
            Unbounded => None,
        };
        (lower_bound, upper_bound)
    }

    pub fn clone_with_dist(&self) -> Self {
        let base = self
            .base
            .clone_with_new_distribution(Distribution::SomeShard);
        Self {
            base,
            core: self.core.clone(),
            kafka_timestamp_range: self.kafka_timestamp_range,
        }
    }
}

impl_plan_tree_node_for_leaf! { BatchKafkaScan }

impl Distill for BatchKafkaScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().unwrap().name.clone());
        let fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
            ("filter", Pretty::debug(&self.kafka_timestamp_range_value())),
        ];
        childless_record("BatchKafkaScan", fields)
    }
}

impl ToLocalBatch for BatchKafkaScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchKafkaScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchKafkaScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.source_catalog().unwrap();
        let (with_properties, secret_refs) = source_catalog.with_properties.clone().into_parts();
        NodeBody::Source(SourceNode {
            source_id: source_catalog.id,
            info: Some(source_catalog.info.clone()),
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            with_properties,
            split: vec![],
            secret_refs,
        })
    }
}

impl ExprRewritable for BatchKafkaScan {}

impl ExprVisitable for BatchKafkaScan {}
