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

use std::assert_matches::assert_matches;
use std::collections::HashSet;
use std::fmt;
use std::io::{Error, ErrorKind};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, USER_COLUMN_ID_OFFSET};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_connector::sink::catalog::{SinkId, SinkType};
use risingwave_connector::sink::{
    SINK_FORMAT_APPEND_ONLY, SINK_FORMAT_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{ExprRewritable, PlanBase, PlanRef, StreamNode};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::WithOptions;

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone)]
pub struct StreamSink {
    pub base: PlanBase,
    input: PlanRef,
    sink_desc: SinkDesc,
}

impl StreamSink {
    #[must_use]
    pub fn new(input: PlanRef, sink_desc: SinkDesc) -> Self {
        let base = PlanBase::derive_stream_plan_base(&input);
        Self {
            base,
            input,
            sink_desc,
        }
    }

    pub fn sink_desc(&self) -> &SinkDesc {
        &self.sink_desc
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input: PlanRef,
        name: String,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        user_cols: FixedBitSet,
        out_names: Vec<String>,
        definition: String,
        properties: WithOptions,
    ) -> Result<Self> {
        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => {
                assert_matches!(user_distributed_by, RequiredDist::Any);
                RequiredDist::shard_by_key(input.schema().len(), input.logical_pk())
            }
        };
        let input = required_dist.enforce_if_not_satisfies(input, &Order::any())?;

        // Used to validate and deduplicate column names.
        let mut col_names = HashSet::new();
        for name in &out_names {
            if !col_names.insert(name.clone()) {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{}\" specified more than once",
                    name
                )))?;
            }
        }

        let mut out_name_iter = out_names.into_iter();
        let schema = input.schema();
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let mut c = ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        field,
                        i as i32 + USER_COLUMN_ID_OFFSET,
                    ),
                    is_hidden: !user_cols.contains(i),
                };
                c.column_desc.name = if !c.is_hidden {
                    out_name_iter.next().unwrap()
                } else {
                    let mut name = field.name.clone();
                    let mut count = 0;

                    while !col_names.insert(name.clone()) {
                        count += 1;
                        name = format!("{}#{}", field.name, count);
                    }

                    name
                };
                c
            })
            .collect_vec();

        let sink = Self::derive_sink_desc(
            input.clone(),
            name,
            user_order_by,
            columns,
            definition,
            properties,
        )?;

        Ok(Self::new(input, sink))
    }

    fn derive_sink_desc(
        input: PlanRef,
        name: String,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        properties: WithOptions,
    ) -> Result<SinkDesc> {
        let pk_indices = input.logical_pk().iter().copied().unique().collect_vec();
        let schema = input.schema();
        let distribution = input.distribution();

        // Assert the uniqueness of column names and IDs.
        if let Some(name) = columns.iter().map(|c| c.name()).duplicates().next() {
            panic!("duplicated column name \"{name}\"");
        }
        if let Some(id) = columns.iter().map(|c| c.column_id()).duplicates().next() {
            panic!("duplicated column ID {id}");
        }

        let mut in_order = FixedBitSet::with_capacity(schema.len());
        let mut pk_list = vec![];
        for field in &user_order_by.field_order {
            let idx = field.index;
            pk_list.push(field.to_order_pair());
            in_order.insert(idx);
        }

        for &idx in &pk_indices {
            if in_order.contains(idx) {
                continue;
            }
            pk_list.push(OrderPair {
                column_idx: idx,
                order_type: OrderType::Ascending,
            });
            in_order.insert(idx);
        }

        let distribution_key = distribution.dist_column_indices().to_vec();
        let sink_type = Self::derive_sink_type(input.append_only(), &properties)?;

        Ok(SinkDesc {
            id: SinkId::placeholder(),
            name,
            definition,
            columns,
            pk: pk_list,
            stream_key: pk_indices,
            distribution_key,
            properties: properties.into_inner(),
            sink_type,
        })
    }

    fn derive_sink_type(input_append_only: bool, properties: &WithOptions) -> Result<SinkType> {
        let frontend_derived_append_only = input_append_only;
        let user_defined_append_only =
            properties.value_eq_ignore_case(SINK_FORMAT_OPTION, SINK_FORMAT_APPEND_ONLY);
        let user_force_append_only =
            properties.value_eq_ignore_case(SINK_USER_FORCE_APPEND_ONLY_OPTION, "true");

        match (
            frontend_derived_append_only,
            user_defined_append_only,
            user_force_append_only,
        ) {
            (true, true, _) => Ok(SinkType::AppendOnly),
            (false, true, true) => Ok(SinkType::ForceAppendOnly),
            (_, false, false) => Ok(SinkType::Upsert),
            (false, true, false) => {
                Err(ErrorCode::SinkError(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                        "The sink cannot be append-only. Please add \"force_append_only='true'\" in WITH options to force the sink to be append-only. Notice that this will cause the sink executor to drop any UPDATE or DELETE message.",
                )))
                .into())
            }
            (_, false, true) => {
                Err(ErrorCode::SinkError(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot force the sink to be append-only without \"format='append_only'\"in WITH options",
                )))
                .into())
            }
        }
    }
}

impl PlanTreeNodeUnary for StreamSink {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sink_desc.clone())
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
            sink_desc: Some(self.sink_desc.to_proto()),
        })
    }
}

impl ExprRewritable for StreamSink {}
