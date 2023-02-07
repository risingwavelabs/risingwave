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

use std::collections::HashMap;
use std::fmt;

use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::WatermarkDesc;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::utils::TableCatalogBuilder;
use super::{PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{TableCatalog, WithOptions};

#[derive(Clone, Debug)]
pub struct StreamWatermarkFilter {
    pub base: PlanBase,
    input: PlanRef,
    watermark_descs: Vec<WatermarkDesc>,
}

impl StreamWatermarkFilter {
    pub fn new(input: PlanRef, watermark_descs: Vec<WatermarkDesc>) -> Self {
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
            // TODO: https://github.com/risingwavelabs/risingwave/issues/7205
            input.watermark_columns().clone(),
        );
        Self {
            base,
            input,
            watermark_descs,
        }
    }
}

impl fmt::Display for StreamWatermarkFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            // TODO(yuhao): display watermark filter expr
            "StreamWatermarkFilter",
        )
    }
}

impl PlanTreeNodeUnary for StreamWatermarkFilter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.watermark_descs.clone())
    }
}

impl_plan_tree_node_for_unary! {StreamWatermarkFilter}

pub fn infer_internal_table_catalog(watermark_type: DataType) -> TableCatalog {
    let mut builder = TableCatalogBuilder::new(WithOptions::new(HashMap::default()));

    let key = Field {
        data_type: DataType::Int16,
        name: "vnode".to_string(),
        sub_fields: vec![],
        type_name: "".to_string(),
    };
    let value = Field {
        data_type: watermark_type,
        name: "offset".to_string(),
        sub_fields: vec![],
        type_name: "".to_string(),
    };

    let ordered_col_idx = builder.add_column(&key);
    builder.add_column(&value);
    builder.add_order_column(ordered_col_idx, OrderType::Ascending);

    builder.set_vnode_col_idx(0);
    builder.set_value_indices(vec![1]);

    builder.build(vec![0])
}

impl StreamNode for StreamWatermarkFilter {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        // TODO(yuhao): allow multiple watermark on source.
        let [watermark_desc]: [_; 1] = self.watermark_descs.clone().try_into().unwrap();
        let watermark_type = (&watermark_desc.expr.clone().unwrap().return_type.unwrap()).into();

        let table = infer_internal_table_catalog(watermark_type);

        ProstStreamNode::WatermarkFilter(WatermarkFilterNode {
            watermark_desc: Some(watermark_desc),
            table: Some(
                table
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
        })
    }
}
