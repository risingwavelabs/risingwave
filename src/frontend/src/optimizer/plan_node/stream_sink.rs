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

use std::fmt;

use risingwave_common::catalog::Field;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{ExprRewritable, PlanBase, PlanRef, StreamNode};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone)]
pub struct StreamSink {
    pub base: PlanBase,
    input: PlanRef,
    // TODO(yuhao): Maybe use a real `SinkCatalog` here. @st1page
    sink_catalog: TableCatalog,
}

impl StreamSink {
    #[must_use]
    pub fn new(input: PlanRef, sink_catalog: TableCatalog) -> Self {
        let base = PlanBase::derive_stream_plan_base(&input);
        Self::with_base(input, sink_catalog, base)
    }

    pub fn with_base(input: PlanRef, sink_catalog: TableCatalog, base: PlanBase) -> Self {
        Self {
            base,
            input,
            sink_catalog,
        }
    }

    pub fn sink_catalog(&self) -> &TableCatalog {
        &self.sink_catalog
    }
}

impl PlanTreeNodeUnary for StreamSink {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sink_catalog.clone())
        // TODO(nanderstabel): Add assertions (assert_eq!)
    }
}

impl_plan_tree_node_for_unary! { StreamSink }

impl fmt::Display for StreamSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamSink");
        builder.finish()
    }
}

impl StreamNode for StreamSink {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::Sink(SinkNode {
            table_id: self.sink_catalog.id().into(),
            fields: self
                .sink_catalog
                .columns()
                .iter()
                .map(|c| Field::from(c.column_desc.clone()).to_prost())
                .collect(),
            sink_pk: self
                .sink_catalog
                .pk()
                .iter()
                .map(|c| c.index as u32)
                .collect(),
            properties: self.sink_catalog.properties.inner().clone(),
        })
    }
}

impl ExprRewritable for StreamSink {}
