use std::fmt;

use risingwave_common::catalog::ColumnDesc;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Clone, Debug)]
pub struct StreamDml {
    pub base: PlanBase,
    input: PlanRef,
    column_descs: Vec<ColumnDesc>,
}

impl StreamDml {
    pub fn new(input: PlanRef, column_descs: Vec<ColumnDesc>) -> Self {
        // TODO: Extract this as a function.
        let base = PlanBase::derive_stream_plan_base(&input);
        Self {
            base,
            input,
            column_descs,
        }
    }

    fn column_names(&self) -> Vec<String> {
        self.column_descs
            .iter()
            .map(|column_desc| column_desc.name.clone())
            .collect()
    }
}

impl fmt::Display for StreamDml {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamDml {{ columns: {} }}",
            format_args!("[{}]", &self.column_names().join(", "))
        )
    }
}

impl PlanTreeNodeUnary for StreamDml {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.column_descs.clone())
    }
}

impl_plan_tree_node_for_unary! {StreamDml}

impl StreamNode for StreamDml {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::Dml(DmlNode {
            // Meta will fill this table id.
            table_id: 0,
            column_descs: self.column_descs.iter().map(Into::into).collect(),
        })
    }
}
