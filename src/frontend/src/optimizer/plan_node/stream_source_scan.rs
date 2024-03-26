// Copyright 2024 RisingWave Labs
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

use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::parser::additional_columns::add_partition_offset_cols;
use risingwave_pb::stream_plan::stream_node::{NodeBody, PbNodeBody};
use risingwave_pb::stream_plan::PbStreamNode;

use super::stream::prelude::*;
use super::utils::TableCatalogBuilder;
use super::{PlanBase, PlanRef};
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{childless_record, Distill};
use crate::optimizer::plan_node::{generic, ExprRewritable, StreamNode};
use crate::optimizer::property::Distribution;
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{Explain, TableCatalog};

/// `StreamSourceScan` scans from a *shared source*. It forwards data from the upstream [`StreamSource`],
/// and also backfills data from the external source.
///
/// Unlike [`StreamSource`], which is a leaf node in the stream graph, `StreamSourceScan` is converted to `merge -> backfill`
///
/// [`StreamSource`]:super::StreamSource
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSourceScan {
    pub base: PlanBase<Stream>,
    core: generic::Source,
}

impl_plan_tree_node_for_leaf! { StreamSourceScan }

impl StreamSourceScan {
    pub fn new(mut core: generic::Source) -> Self {
        // XXX: do we need to include partition and offset cols here? It's needed by Backfill's input, but maybe not output?
        // But the source's "schema" contains the hidden columns.
        if let Some(source_catalog) = &core.catalog
            && source_catalog.info.is_shared
        {
            let (columns_exist, additional_columns) =
                add_partition_offset_cols(&core.column_catalog, &source_catalog.connector_name());
            for (existed, mut c) in columns_exist.into_iter().zip_eq_fast(additional_columns) {
                c.is_hidden = true;
                if !existed {
                    core.column_catalog.push(c);
                }
            }
        }

        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::SomeShard,
            core.catalog.as_ref().map_or(true, |s| s.append_only),
            false,
            FixedBitSet::with_capacity(core.column_catalog.len()),
        );

        Self { base, core }
    }

    fn get_columns(&self) -> Vec<&str> {
        self.core
            .column_catalog
            .iter()
            .map(|column| column.name())
            .collect()
    }

    pub fn source_catalog(&self) -> Rc<SourceCatalog> {
        self.core
            .catalog
            .clone()
            .expect("source backfill should have source cataglog")
    }

    pub fn infer_internal_table_catalog() -> TableCatalog {
        // note that source's internal table is to store partition_id -> offset mapping and its
        // schema is irrelevant to input schema
        // On the premise of ensuring that the materialized_source data can be cleaned up, keep the
        // state in source.
        // Source state doesn't maintain retention_seconds, internal_table_subset function only
        // returns retention_seconds so default is used here
        let mut builder = TableCatalogBuilder::default();

        let key = Field {
            data_type: DataType::Varchar,
            name: "partition_id".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };
        let value = Field {
            data_type: DataType::Jsonb,
            name: "backfill_progress".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };

        let ordered_col_idx = builder.add_column(&key);
        builder.add_column(&value);
        builder.add_order_column(ordered_col_idx, OrderType::ascending());
        // read prefix hint is 0. We need to scan all data in the state table.
        builder.build(vec![], 0)
    }

    pub fn adhoc_to_stream_prost(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbStreamNode> {
        use risingwave_pb::stream_plan::*;

        let stream_key = self
            .stream_key()
            .unwrap_or_else(|| {
                panic!(
                    "should always have a stream key in the stream plan but not, sub plan: {}",
                    PlanRef::from(self.clone()).explain_to_string()
                )
            })
            .iter()
            .map(|x| *x as u32)
            .collect_vec();

        let source_catalog = self.source_catalog();
        let backfill = SourceBackfillNode {
            upstream_source_id: source_catalog.id,
            source_name: source_catalog.name.clone(),
            state_table: Some(
                Self::infer_internal_table_catalog()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            info: Some(source_catalog.info.clone()),
            // XXX: what's the usage of this?
            row_id_index: self.core.row_id_index.map(|index| index as _),
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            with_properties: source_catalog.with_properties.clone().into_iter().collect(),
            rate_limit: self.base.ctx().overwrite_options().streaming_rate_limit,
        };

        let fields = self.schema().to_prost();
        // plan: merge -> backfill
        Ok(PbStreamNode {
            fields: fields.clone(),
            input: vec![
                // The merge node body will be filled by the `ActorBuilder` on the meta service.
                PbStreamNode {
                    node_body: Some(PbNodeBody::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields,
                    stream_key: vec![], // not used
                    ..Default::default()
                },
            ],
            node_body: Some(PbNodeBody::SourceBackfill(backfill)),
            stream_key,
            operator_id: self.base.id().0 as u64,
            identity: self.distill_to_string(),
            append_only: self.append_only(),
        })
    }
}

impl Distill for StreamSourceScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let columns = self
            .get_columns()
            .iter()
            .map(|ele| Pretty::from(ele.to_string()))
            .collect();
        let col = Pretty::Array(columns);
        childless_record("StreamSourceScan", vec![("columns", col)])
    }
}

impl ExprRewritable for StreamSourceScan {}

impl ExprVisitable for StreamSourceScan {}

impl StreamNode for StreamSourceScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        unreachable!("stream source backfill cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}
