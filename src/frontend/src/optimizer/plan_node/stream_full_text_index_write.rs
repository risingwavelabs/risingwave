// Copyright 2026 RisingWave Labs
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

use std::num::NonZeroU32;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::CreateType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::PbFullTextIndexInfo;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::object_index_write::{ObjectIndexTableInfo, derive_object_index_table_catalog};
use crate::TableCatalog;
use crate::catalog::{DatabaseId, SchemaId};
use crate::error::ErrorCode;
use crate::optimizer::StreamOptimizedLogicalPlanRoot;
use crate::optimizer::plan_node::derive::derive_columns;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanRef, PhysicalPlanRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::{Distill, childless_record, plan_can_use_background_ddl};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanNodeMeta, PlanTreeNodeUnary, Stream, StreamNode,
    StreamPlanRef as PlanRef, reorganize_elements_id,
};
use crate::optimizer::property::{Cardinality, Distribution, RequiredDist, StreamKind};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// Writes the rows of the primary table into a full-text index.
///
/// The columns of the input and of the index table are
/// `[indexed document, stream key of the primary table...]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFullTextIndexWrite {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    table: TableCatalog,
}

impl StreamFullTextIndexWrite {
    fn new(input: PlanRef, table: TableCatalog) -> crate::error::Result<Self> {
        // The writer handles deletes and updates, but needs the old value of a row to remove it
        // from the index.
        if input.stream_kind() == StreamKind::Upsert {
            return Err(ErrorCode::NotSupported(
                "cannot create full-text index on upsert workflow".to_owned(),
                "try create full-text index on a table or materialized view".to_owned(),
            )
            .into());
        }
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            Some(table.stream_key()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.stream_kind(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );
        Ok(Self { base, input, table })
    }

    pub fn create(
        StreamOptimizedLogicalPlanRoot {
            plan: input,
            required_dist: user_distributed_by,
            required_order: user_order_by,
            out_fields: user_cols,
            out_names,
            ..
        }: StreamOptimizedLogicalPlanRoot,
        name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        definition: String,
        cardinality: Cardinality,
        retention_seconds: Option<NonZeroU32>,
        full_text_index_info: PbFullTextIndexInfo,
    ) -> crate::error::Result<Self> {
        let input = RequiredDist::PhysicalDist(Distribution::Single)
            .streaming_enforce_if_not_satisfies(input)?;
        // the hidden column name might refer some expr id
        let input = reorganize_elements_id(input);
        let columns = derive_columns(input.schema(), out_names, &user_cols)?;

        let create_type = if input.ctx().session_ctx().config().background_ddl()
            && plan_can_use_background_ddl(&input)
        {
            CreateType::Background
        } else {
            CreateType::Foreground
        };

        let table = derive_object_index_table_catalog(
            input.clone(),
            name,
            database_id,
            schema_id,
            user_distributed_by,
            user_order_by,
            columns,
            definition,
            cardinality,
            retention_seconds,
            create_type,
            ObjectIndexTableInfo::FullText(full_text_index_info),
        )?;

        Self::new(input, table)
    }

    pub(crate) fn table(&self) -> &TableCatalog {
        &self.table
    }
}

impl Distill for StreamFullTextIndexWrite {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let table = self.table();
        let info = table
            .full_text_index_info
            .as_ref()
            .expect("full-text index table should have full-text index info");
        let key_columns = table.columns[1..]
            .iter()
            .filter(|col| !col.is_rw_sys_column())
            .map(|col| Pretty::from(col.name().to_owned()))
            .collect();
        let vec = vec![
            (
                "document_column",
                Pretty::from(table.columns[0].name().to_owned()),
            ),
            ("key_columns", Pretty::Array(key_columns)),
            ("analyzer", Pretty::from(info.analyzer.clone())),
        ];
        childless_record("StreamFullTextIndexWrite", vec)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamFullTextIndexWrite {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let new = Self::new(input, self.table().clone()).unwrap();
        new.base
            .schema()
            .fields
            .iter()
            .zip_eq_fast(self.base.schema().fields.iter())
            .for_each(|(a, b)| {
                assert_eq!(a.data_type, b.data_type);
            });
        assert_eq!(new.plan_base().stream_key(), self.plan_base().stream_key());
        new
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamFullTextIndexWrite }

impl StreamNode for StreamFullTextIndexWrite {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        PbNodeBody::FullTextIndexWrite(Box::new(FullTextIndexWriteNode {
            table: Some(self.table().to_prost()),
        }))
    }
}

impl ExprRewritable<Stream> for StreamFullTextIndexWrite {}

impl ExprVisitable for StreamFullTextIndexWrite {}
