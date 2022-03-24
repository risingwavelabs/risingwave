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
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::Source as ProstSource;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::SourceNode;

use super::{LogicalScan, PlanBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};
use crate::session::QueryContextRef;

/// [`StreamSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct StreamSource {
    pub base: PlanBase,
    source_catalog: ProstSource,
    col_descs: Vec<ColumnDesc>,
    source_type: SourceType,
}

impl StreamSource {
    pub fn create(ctx: QueryContextRef, pk_idx: Vec<usize>, source_catalog: ProstSource) -> Self {
        let (source_type, prost_columns) = match &source_catalog.info {
            Some(Info::StreamSource(source)) => (SourceType::Source, source.columns.clone()),
            Some(Info::TableSource(source)) => (SourceType::Table, source.columns.clone()),
            None => unreachable!(),
        };
        let col_descs = prost_columns
            .iter()
            .map(|c| c.column_desc.as_ref().unwrap())
            .map(ColumnDesc::from)
            .collect_vec();

        let fields = col_descs.iter().map(Field::from).collect();
        let base =
            PlanBase::new_stream(ctx, Schema { fields }, pk_idx, Distribution::any().clone());
        Self {
            base,
            source_catalog,
            col_descs,
            source_type,
        }
    }
    pub fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }
}

impl WithSchema for StreamSource {
    fn schema(&self) -> &Schema {
        &self.base.schema
    }
}

impl_plan_tree_node_for_leaf! { StreamSource }

impl fmt::Display for StreamSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamSource {{ source: {},  columns: [{}] }}",
            self.source_catalog.name,
            self.column_names().join(", ")
        )
    }
}

impl ToStreamProst for StreamSource {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::SourceNode(SourceNode {
            // TODO: Refactor this id
            table_ref_id: TableRefId {
                table_id: self.source_catalog.id as i32,
                ..Default::default()
            }
            .into(),
            column_ids: self.col_descs.iter().map(|c| c.column_id.into()).collect(),
            source_type: self.source_type as i32,
        })
    }
}
