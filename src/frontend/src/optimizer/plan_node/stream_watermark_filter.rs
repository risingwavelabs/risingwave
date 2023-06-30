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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::RwError;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::WatermarkDesc;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::utils::{childless_record, Distill, TableCatalogBuilder};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{ExprDisplay, ExprImpl};
use crate::optimizer::plan_node::utils::formatter_debug_plan_node;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{TableCatalog, WithOptions};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamWatermarkFilter {
    pub base: PlanBase,
    input: PlanRef,
    watermark_descs: Vec<WatermarkDesc>,
}

impl StreamWatermarkFilter {
    pub fn new(input: PlanRef, watermark_descs: Vec<WatermarkDesc>) -> Self {
        let mut watermark_columns = FixedBitSet::with_capacity(input.schema().len());
        for i in &watermark_descs {
            watermark_columns.insert(i.get_watermark_idx() as usize)
        }
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
            false, // TODO(rc): decide EOWC property
            watermark_columns,
        );
        Self::with_base(base, input, watermark_descs)
    }

    fn with_base(base: PlanBase, input: PlanRef, watermark_descs: Vec<WatermarkDesc>) -> Self {
        Self {
            base,
            input,
            watermark_descs,
        }
    }
}

impl Distill for StreamWatermarkFilter {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let input_schema = self.input.schema();

        let display_watermark_descs = (self.watermark_descs.iter())
            .map(|desc| {
                let expr = ExprDisplay {
                    expr: &ExprImpl::from_expr_proto(desc.get_expr().unwrap()).unwrap(),
                    input_schema,
                };
                let fields = vec![
                    ("idx", Pretty::debug(&desc.watermark_idx)),
                    ("expr", Pretty::display(&expr)),
                ];
                Pretty::childless_record("Desc", fields)
            })
            .collect();
        let fields = vec![("watermark_descs", Pretty::Array(display_watermark_descs))];
        childless_record("StreamWatermarkFilter", fields)
    }
}
impl fmt::Display for StreamWatermarkFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DisplayWatermarkDesc<'a> {
            watermark_idx: u32,
            expr: ExprImpl,
            input_schema: &'a Schema,
        }

        impl fmt::Debug for DisplayWatermarkDesc<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let expr_display = ExprDisplay {
                    expr: &self.expr,
                    input_schema: self.input_schema,
                };
                write!(f, "idx: {}, expr: {}", self.watermark_idx, expr_display)
            }
        }

        let mut builder = formatter_debug_plan_node!(f, "StreamWatermarkFilter");
        let input_schema = self.input.schema();

        let display_watermark_descs: Vec<_> = self
            .watermark_descs
            .iter()
            .map(|desc| {
                Ok::<_, RwError>(DisplayWatermarkDesc {
                    watermark_idx: desc.watermark_idx,
                    expr: ExprImpl::from_expr_proto(desc.get_expr()?)?,
                    input_schema,
                })
            })
            .try_collect()
            .map_err(|_| fmt::Error)?;
        builder.field("watermark_descs", &display_watermark_descs);
        builder.finish()
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
    builder.add_order_column(ordered_col_idx, OrderType::ascending());

    builder.set_vnode_col_idx(0);
    builder.set_value_indices(vec![1]);

    builder.build(vec![0], 1)
}

impl StreamNode for StreamWatermarkFilter {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        // TODO(yuhao): allow multiple watermark on source.
        let [watermark_desc]: [_; 1] = self.watermark_descs.clone().try_into().unwrap();
        let watermark_type = (&watermark_desc.expr.unwrap().return_type.unwrap()).into();

        let table = infer_internal_table_catalog(watermark_type);

        PbNodeBody::WatermarkFilter(WatermarkFilterNode {
            watermark_descs: self.watermark_descs.clone(),
            tables: vec![table
                .with_id(state.gen_table_id_wrapped())
                .to_internal_table_prost()],
        })
    }
}

// TODO(yuhao): may impl a `ExprRewritable` after store `ExplImpl` in catalog.
impl ExprRewritable for StreamWatermarkFilter {}
