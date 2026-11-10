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

//! Shared by [`super::StreamVectorIndexWrite`] and [`super::StreamFullTextIndexWrite`].

use std::num::NonZeroU32;

use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, ConflictBehavior, CreateType, Engine, StreamJobStatus, TableId,
};
use risingwave_common::hash::VnodeCount;
use risingwave_common::id::FragmentId;
use risingwave_pb::catalog::{PbFullTextIndexInfo, PbVectorIndexInfo};

use crate::TableCatalog;
use crate::catalog::table_catalog::TableType;
use crate::catalog::{DatabaseId, SchemaId};
use crate::optimizer::plan_node::StreamPlanRef as PlanRef;
use crate::optimizer::plan_node::derive::derive_pk;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::property::{Cardinality, Order, RequiredDist};

/// Info of an object index, i.e. an index whose data is stored in its own objects instead of SSTs.
pub(super) enum ObjectIndexTableInfo {
    Vector(PbVectorIndexInfo),
    FullText(PbFullTextIndexInfo),
}

/// Derives the catalog of an object index. Rows are kept in index objects, not a state table.
#[expect(clippy::too_many_arguments)]
pub(super) fn derive_object_index_table_catalog(
    rewritten_input: PlanRef,
    name: String,
    database_id: DatabaseId,
    schema_id: SchemaId,
    user_distributed_by: RequiredDist,
    user_order_by: Order,
    columns: Vec<ColumnCatalog>,
    definition: String,
    cardinality: Cardinality,
    retention_seconds: Option<NonZeroU32>,
    create_type: CreateType,
    info: ObjectIndexTableInfo,
) -> crate::error::Result<TableCatalog> {
    let input = rewritten_input;
    let (table_type, vector_index_info, full_text_index_info) = match info {
        ObjectIndexTableInfo::Vector(info) => (TableType::VectorIndex, Some(info), None),
        ObjectIndexTableInfo::FullText(info) => (TableType::FullTextIndex, None, Some(info)),
    };

    let value_indices = (0..columns.len()).collect_vec();
    let distribution_key = input.distribution().dist_column_indices().to_vec();
    let vnode_count = if distribution_key.is_empty() {
        VnodeCount::Singleton
    } else {
        VnodeCount::Placeholder
    };
    let append_only = input.append_only();
    // TODO(rc): In `TableCatalog` we still use `FixedBitSet` for watermark columns, ignoring the watermark group information.
    // We will record the watermark group information in `TableCatalog` in the future. For now, let's flatten the watermark columns.
    let watermark_columns = input.watermark_columns().indices().collect();

    let (table_pk, stream_key) = derive_pk(input, user_distributed_by, user_order_by, &columns);
    let dist_key_in_pk = distribution_key
        .iter()
        .map(|idx| {
            table_pk
                .iter()
                .position(|order| order.column_index == *idx)
                .unwrap()
        })
        .collect();
    // assert: `stream_key` is a subset of `table_pk`

    // Index writes go through the object index writer APIs instead of StateTable
    // get/prefix-iter reads, so prefix SST filters are not consumed.
    let read_prefix_len_hint = 0;
    // We don't need to fill in table id for table in frontend. The id will be generated on
    // meta catalog service.
    Ok(TableCatalog {
        id: TableId::placeholder(),
        schema_id,
        database_id,
        associated_source_id: None,
        name,
        columns,
        pk: table_pk,
        stream_key,
        distribution_key,
        table_type,
        append_only,
        owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
        fragment_id: FragmentId::placeholder(),
        dml_fragment_id: None,
        vnode_col_index: None,
        row_id_index: None,
        value_indices,
        definition,
        conflict_behavior: ConflictBehavior::NoCheck,
        version_column_indices: vec![],
        read_prefix_len_hint,
        version: None,
        watermark_columns,
        dist_key_in_pk,
        cardinality,
        created_at_epoch: None,
        initialized_at_epoch: None,
        create_type,
        stream_job_status: StreamJobStatus::Creating,
        description: None,
        initialized_at_cluster_version: None,
        created_at_cluster_version: None,
        retention_seconds: retention_seconds.map(|i| i.into()),
        cdc_table_id: None,
        vnode_count, // will be filled in by the meta service later
        webhook_info: None,
        job_id: None,
        engine: Engine::Hummock,
        clean_watermark_index_in_pk: None,
        clean_watermark_indices: vec![],
        refreshable: false,
        vector_index_info,
        full_text_index_info,
        cdc_table_type: None,
    })
}
