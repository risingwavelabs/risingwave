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
use std::fmt;
use std::io::{Error, ErrorKind};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_connector::sink::catalog::{SinkId, SinkType};
use risingwave_connector::sink::{
    SINK_FORMAT_APPEND_ONLY, SINK_FORMAT_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::derive::{derive_columns, derive_pk};
use super::utils::IndicesDisplay;
use super::{ExprRewritable, PlanBase, PlanRef, StreamNode};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::WithOptions;

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let columns = derive_columns(input.schema(), out_names, &user_cols)?;

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
        let distribution_key = input.distribution().dist_column_indices().to_vec();
        let sink_type = Self::derive_sink_type(input.append_only(), &properties)?;
        let (pk, stream_key) = derive_pk(input, user_order_by, &columns);

        if sink_type == SinkType::Upsert && pk.is_empty() {
            return Err(ErrorCode::SinkError(Box::new(Error::new(
                ErrorKind::InvalidInput,
                "No primary key for the upsert sink. Please include the primary key explicitly in sink definition or make the sink append-only.",
            )))
            .into());
        }

        Ok(SinkDesc {
            id: SinkId::placeholder(),
            name,
            definition,
            columns,
            pk: pk.iter().map(|k| k.to_column_order()).collect_vec(),
            stream_key,
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
                    "Cannot force the sink to be append-only without \"format='append_only'\"in WITH options.",
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

        let sink_type = if self.sink_desc.sink_type.is_append_only() {
            "append-only"
        } else {
            "upsert"
        };
        let column_names = self
            .sink_desc
            .columns
            .iter()
            .map(|col| col.column_desc.name.clone())
            .collect_vec()
            .join(", ");
        builder
            .field("type", &format_args!("{}", sink_type))
            .field("columns", &format_args!("[{}]", column_names));

        if self.sink_desc.sink_type.is_upsert() {
            builder.field(
                "pk",
                &IndicesDisplay {
                    indices: &self.sink_desc.pk.iter().map(|k| k.column_idx).collect_vec(),
                    input_schema: &self.base.schema,
                },
            );
        }

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
