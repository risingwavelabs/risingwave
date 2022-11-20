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

use std::fmt;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::SourceNode;
use risingwave_pb::catalog::{ColumnIndex, SourceInfo};

use super::{LogicalSource, PlanBase, PlanRef, ToBatchProst, ToDistributedBatch, ToLocalBatch};
use crate::optimizer::property::Distribution;

/// [`BatchSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct BatchSource {
    pub base: PlanBase,
    logical: LogicalSource,
}

impl BatchSource {
    pub fn new(logical: LogicalSource) -> Self {
        let base = PlanBase::new_stream(
            logical.ctx(),
            logical.schema().clone(),
            logical.logical_pk().to_vec(),
            logical.functional_dependency().clone(),
            Distribution::Single,
            logical.source_catalog().append_only,
        );
        Self { base, logical }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }
}

impl_plan_tree_node_for_leaf! { BatchSource }

impl fmt::Display for BatchSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamSource");
        builder
            .field("source", &self.logical.source_catalog().name)
            .field(
                "columns",
                &format_args!("[{}]", &self.column_names().join(", ")),
            )
            .finish()
    }
}

impl ToLocalBatch for BatchSource {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone().into())
    }
}

impl ToDistributedBatch for BatchSource {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone().into())
    }
}

impl ToBatchProst for BatchSource {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.logical.source_catalog();
        NodeBody::Source(SourceNode {
            source_id: source_catalog.id,
            info: Some(SourceInfo {
                source_info: Some(source_catalog.info.clone()),
            }),
            row_id_index: source_catalog
                .row_id_index
                .map(|index| ColumnIndex { index: index as _ }),
            columns: source_catalog
                .columns
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            pk_column_ids: source_catalog
                .pk_col_ids
                .iter()
                .map(Into::into)
                .collect_vec(),
            properties: source_catalog.properties.clone(),
            split: vec![],
        })
    }
}
