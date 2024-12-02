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

use std::collections::HashMap;
use std::default::Default;
use std::vec;

use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, Str, StrAssocArr, XmlNode};
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, ConflictBehavior, CreateType, Field, FieldDisplay, Schema,
    StreamJobStatus, OBJECT_ID_PLACEHOLDER,
};
use risingwave_common::constants::log_store::v2::{
    KV_LOG_STORE_PREDEFINED_COLUMNS, PK_ORDERING, VNODE_COLUMN_INDEX,
};
use risingwave_common::hash::VnodeCount;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use crate::catalog::table_catalog::TableType;
use crate::catalog::{ColumnId, TableCatalog, TableId};
use crate::optimizer::property::{Cardinality, Order, RequiredDist};
use crate::optimizer::StreamScanType;
use crate::utils::{Condition, IndexSet};

#[derive(Default)]
pub struct TableCatalogBuilder {
    /// All columns in this table
    columns: Vec<ColumnCatalog>,
    pk: Vec<ColumnOrder>,
    value_indices: Option<Vec<usize>>,
    vnode_col_idx: Option<usize>,
    column_names: HashMap<String, i32>,
    watermark_columns: Option<FixedBitSet>,
    dist_key_in_pk: Option<Vec<usize>>,
}

/// For DRY, mainly used for construct internal table catalog in stateful streaming executors.
/// Be careful of the order of add column.
impl TableCatalogBuilder {
    /// Add a column from Field info, return the column index of the table
    pub fn add_column(&mut self, field: &Field) -> usize {
        let column_idx = self.columns.len();
        let column_id = column_idx as i32;
        // Add column desc.
        let mut column_desc = ColumnDesc::from_field_with_column_id(field, column_id);

        // Replace dot of the internal table column name with underline.
        column_desc.name = column_desc.name.replace('.', "_");
        // Avoid column name duplicate.
        self.avoid_duplicate_col_name(&mut column_desc);

        self.columns.push(ColumnCatalog {
            column_desc,
            // All columns in internal table are invisible to batch query.
            is_hidden: false,
        });
        column_idx
    }

    /// Extend the columns with column ids reset. The input columns should NOT have duplicate names.
    ///
    /// Returns the indices of the extended columns.
    pub fn extend_columns(&mut self, columns: &[ColumnCatalog]) -> Vec<usize> {
        let base_idx = self.columns.len();
        columns.iter().enumerate().for_each(|(i, col)| {
            assert!(!self.column_names.contains_key(col.name()));
            self.column_names.insert(col.name().to_string(), 0);

            // Reset the column id for the columns.
            let mut new_col = col.clone();
            new_col.column_desc.column_id = ColumnId::new((base_idx + i) as _);
            self.columns.push(new_col);
        });
        Vec::from_iter(base_idx..(base_idx + columns.len()))
    }

    /// Check whether need to add a ordered column. Different from value, order desc equal pk in
    /// semantics and they are encoded as storage key.
    pub fn add_order_column(&mut self, column_index: usize, order_type: OrderType) {
        self.pk.push(ColumnOrder::new(column_index, order_type));
    }

    /// get the current exist field number of the primary key.
    pub fn get_current_pk_len(&self) -> usize {
        self.pk.len()
    }

    pub fn set_vnode_col_idx(&mut self, vnode_col_idx: usize) {
        self.vnode_col_idx = Some(vnode_col_idx);
    }

    pub fn set_value_indices(&mut self, value_indices: Vec<usize>) {
        self.value_indices = Some(value_indices);
    }

    pub fn set_dist_key_in_pk(&mut self, dist_key_in_pk: Vec<usize>) {
        self.dist_key_in_pk = Some(dist_key_in_pk);
    }

    /// Check the column name whether exist before. if true, record occurrence and change the name
    /// to avoid duplicate.
    fn avoid_duplicate_col_name(&mut self, column_desc: &mut ColumnDesc) {
        if let Some(old_identity) = self.column_names.get(&column_desc.name) {
            let column_name = column_desc.name.clone();
            let mut identity = *old_identity;
            loop {
                column_desc.name = format!("{}_{}", column_name, identity);
                identity += 1;
                if !self.column_names.contains_key(&column_desc.name) {
                    break;
                }
            }
            *self.column_names.get_mut(&column_name).unwrap() = identity;
        }
        self.column_names.insert(column_desc.name.clone(), 0);
    }

    /// Consume builder and create `TableCatalog` (for proto). The `read_prefix_len_hint` is the
    /// anticipated read prefix pattern (number of fields) for the table, which can be utilized for
    /// implementing the table's bloom filter or other storage optimization techniques.
    pub fn build(self, distribution_key: Vec<usize>, read_prefix_len_hint: usize) -> TableCatalog {
        assert!(read_prefix_len_hint <= self.pk.len());
        let watermark_columns = match self.watermark_columns {
            Some(w) => w,
            None => FixedBitSet::with_capacity(self.columns.len()),
        };

        // If `dist_key_in_pk` is set, check if it matches with `distribution_key`.
        // Note that we cannot derive in the opposite direction, because there can be a column
        // appearing multiple times in the PK.
        if let Some(dist_key_in_pk) = &self.dist_key_in_pk {
            let derived_dist_key = dist_key_in_pk
                .iter()
                .map(|idx| self.pk[*idx].column_index)
                .collect_vec();
            assert_eq!(
                derived_dist_key, distribution_key,
                "dist_key mismatch with dist_key_in_pk"
            );
        }

        TableCatalog {
            id: TableId::placeholder(),
            associated_source_id: None,
            name: String::new(),
            dependent_relations: vec![],
            columns: self.columns.clone(),
            pk: self.pk,
            stream_key: vec![],
            distribution_key,
            // NOTE: This should be altered if `TableCatalogBuilder` is used to build something
            // other than internal tables.
            table_type: TableType::Internal,
            append_only: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            fragment_id: OBJECT_ID_PLACEHOLDER,
            dml_fragment_id: None,
            vnode_col_index: self.vnode_col_idx,
            row_id_index: None,
            value_indices: self
                .value_indices
                .unwrap_or_else(|| (0..self.columns.len()).collect_vec()),
            definition: "".into(),
            conflict_behavior: ConflictBehavior::NoCheck,
            version_column_index: None,
            read_prefix_len_hint,
            version: None, // the internal table is not versioned and can't be schema changed
            watermark_columns,
            dist_key_in_pk: self.dist_key_in_pk.unwrap_or_default(),
            cardinality: Cardinality::unknown(), // TODO(card): cardinality of internal table
            created_at_epoch: None,
            initialized_at_epoch: None,
            cleaned_by_watermark: false,
            // NOTE(kwannoel): This may not match the create type of the materialized table.
            // It should be ignored for internal tables.
            create_type: CreateType::Foreground,
            stream_job_status: StreamJobStatus::Creating,
            description: None,
            incoming_sinks: vec![],
            initialized_at_cluster_version: None,
            created_at_cluster_version: None,
            retention_seconds: None,
            cdc_table_id: None,
            vnode_count: VnodeCount::Placeholder, // will be filled in by the meta service later
            webhook_info: None,
            job_id: None,
        }
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }
}

/// See also [`super::generic::DistillUnit`].
pub trait Distill {
    fn distill<'a>(&self) -> XmlNode<'a>;

    fn distill_to_string(&self) -> String {
        let mut config = pretty_config();
        let mut output = String::with_capacity(2048);
        config.unicode(&mut output, &Pretty::Record(self.distill()));
        output
    }
}

pub(super) fn childless_record<'a>(
    name: impl Into<Str<'a>>,
    fields: StrAssocArr<'a>,
) -> XmlNode<'a> {
    XmlNode::simple_record(name, fields, Default::default())
}

macro_rules! impl_distill_by_unit {
    ($ty:ty, $core:ident, $name:expr) => {
        use pretty_xmlish::XmlNode;
        use $crate::optimizer::plan_node::generic::DistillUnit;
        use $crate::optimizer::plan_node::utils::Distill;
        impl Distill for $ty {
            fn distill<'a>(&self) -> XmlNode<'a> {
                self.$core.distill_with_name($name)
            }
        }
    };
}
pub(crate) use impl_distill_by_unit;

pub(crate) fn column_names_pretty<'a>(schema: &Schema) -> Pretty<'a> {
    let columns = (schema.fields.iter())
        .map(|f| f.name.clone())
        .map(Pretty::from)
        .collect();
    Pretty::Array(columns)
}

pub(crate) fn watermark_pretty<'a>(
    watermark_columns: &FixedBitSet,
    schema: &Schema,
) -> Option<Pretty<'a>> {
    iter_fields_pretty(watermark_columns.ones(), schema)
}

pub(crate) fn iter_fields_pretty<'a>(
    columns: impl Iterator<Item = usize>,
    schema: &Schema,
) -> Option<Pretty<'a>> {
    let arr = columns
        .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
        .map(|d| Pretty::display(&d))
        .collect::<Vec<_>>();
    if arr.is_empty() {
        None
    } else {
        Some(Pretty::Array(arr))
    }
}

#[derive(Clone, Copy)]
pub struct IndicesDisplay<'a> {
    pub indices: &'a [usize],
    pub schema: &'a Schema,
}

impl<'a> IndicesDisplay<'a> {
    /// Returns `None` means all
    pub fn from_join<'b, PlanRef: GenericPlanRef>(
        join: &'a generic::Join<PlanRef>,
        input_schema: &'a Schema,
    ) -> Pretty<'b> {
        let col_num = join.internal_column_num();
        let id = Self::from(&join.output_indices, col_num, input_schema);
        id.map_or_else(|| Pretty::from("all"), Self::distill)
    }

    /// Returns `None` means all
    fn from(indices: &'a [usize], col_num: usize, schema: &'a Schema) -> Option<Self> {
        if indices.iter().copied().eq(0..col_num) {
            return None;
        }
        Some(Self { indices, schema })
    }

    pub fn distill<'b>(self) -> Pretty<'b> {
        let vec = self.indices.iter().map(|&i| {
            let name = self.schema.fields.get(i).unwrap().name.clone();
            Pretty::from(name)
        });
        Pretty::Array(vec.collect())
    }
}

pub(crate) fn sum_affected_row(dml: PlanRef) -> Result<PlanRef> {
    let dml = RequiredDist::single().enforce_if_not_satisfies(dml, &Order::any())?;
    // Accumulate the affected rows.
    let sum_agg = PlanAggCall {
        agg_type: PbAggKind::Sum.into(),
        return_type: DataType::Int64,
        inputs: vec![InputRef::new(0, DataType::Int64)],
        distinct: false,
        order_by: vec![],
        filter: Condition::true_cond(),
        direct_args: vec![],
    };
    let agg = Agg::new(vec![sum_agg], IndexSet::empty(), dml);
    let batch_agg = BatchSimpleAgg::new(agg);
    Ok(batch_agg.into())
}

/// Call `debug_struct` on the given formatter to create a debug struct builder.
/// If a property list is provided, properties in it will be added to the struct name according to
/// the condition of that property.
macro_rules! plan_node_name {
    ($name:literal $(, { $prop:literal, $cond:expr } )* $(,)?) => {
        {
            #[allow(unused_mut)]
            let mut properties: Vec<&str> = vec![];
            $( if $cond { properties.push($prop); } )*
            let mut name = $name.to_string();
            if !properties.is_empty() {
                name += " [";
                name += &properties.join(", ");
                name += "]";
            }
            name
        }
    };
}
pub(crate) use plan_node_name;
use risingwave_common::license::Feature;
use risingwave_common::types::{DataType, Interval};
use risingwave_expr::aggregate::PbAggKind;
use risingwave_pb::plan_common::as_of::AsOfType;
use risingwave_pb::plan_common::{as_of, PbAsOf};
use risingwave_sqlparser::ast::AsOf;

use super::generic::{self, GenericPlanRef, PhysicalPlanRef};
use super::pretty_config;
use crate::error::{ErrorCode, Result};
use crate::expr::InputRef;
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{BatchSimpleAgg, PlanAggCall};
use crate::PlanRef;

pub fn infer_kv_log_store_table_catalog_inner(
    input: &PlanRef,
    columns: &[ColumnCatalog],
) -> TableCatalog {
    let mut table_catalog_builder = TableCatalogBuilder::default();

    let mut value_indices =
        Vec::with_capacity(KV_LOG_STORE_PREDEFINED_COLUMNS.len() + columns.len());

    for (name, data_type) in KV_LOG_STORE_PREDEFINED_COLUMNS {
        let indice = table_catalog_builder.add_column(&Field::with_name(data_type, name));
        value_indices.push(indice);
    }

    table_catalog_builder.set_vnode_col_idx(VNODE_COLUMN_INDEX);

    for (i, ordering) in PK_ORDERING.iter().enumerate() {
        table_catalog_builder.add_order_column(i, *ordering);
    }

    let read_prefix_len_hint = table_catalog_builder.get_current_pk_len();

    let payload_indices = table_catalog_builder.extend_columns(columns);

    value_indices.extend(payload_indices);
    table_catalog_builder.set_value_indices(value_indices);

    // Modify distribution key indices based on the pre-defined columns.
    let dist_key = input
        .distribution()
        .dist_column_indices()
        .iter()
        .map(|idx| idx + KV_LOG_STORE_PREDEFINED_COLUMNS.len())
        .collect_vec();

    table_catalog_builder.build(dist_key, read_prefix_len_hint)
}

/// Check that all leaf nodes must be stream table scan,
/// since that plan node maps to `backfill` executor, which supports recovery.
/// Some other leaf nodes like `StreamValues` do not support recovery, and they
/// cannot use background ddl.
pub(crate) fn plan_can_use_background_ddl(plan: &PlanRef) -> bool {
    if plan.inputs().is_empty() {
        if plan.as_stream_source_scan().is_some()
            || plan.as_stream_now().is_some()
            || plan.as_stream_source().is_some()
        {
            true
        } else if let Some(scan) = plan.as_stream_table_scan() {
            scan.stream_scan_type() == StreamScanType::Backfill
                || scan.stream_scan_type() == StreamScanType::ArrangementBackfill
        } else {
            false
        }
    } else {
        assert!(!plan.inputs().is_empty());
        plan.inputs().iter().all(plan_can_use_background_ddl)
    }
}

pub fn to_pb_time_travel_as_of(a: &Option<AsOf>) -> Result<Option<PbAsOf>> {
    let Some(ref a) = a else {
        return Ok(None);
    };
    Feature::TimeTravel
        .check_available()
        .map_err(|e| anyhow::anyhow!(e))?;
    let as_of_type = match a {
        AsOf::ProcessTime => {
            return Err(ErrorCode::NotSupported(
                "do not support as of proctime".to_string(),
                "please use as of timestamp".to_string(),
            )
            .into());
        }
        AsOf::TimestampNum(ts) => AsOfType::Timestamp(as_of::Timestamp { timestamp: *ts }),
        AsOf::TimestampString(ts) => {
            let date_time = speedate::DateTime::parse_str_rfc3339(ts)
                .map_err(|_e| anyhow!("fail to parse timestamp"))?;
            AsOfType::Timestamp(as_of::Timestamp {
                timestamp: date_time.timestamp_tz(),
            })
        }
        AsOf::VersionNum(_) | AsOf::VersionString(_) => {
            return Err(ErrorCode::NotSupported(
                "do not support as of version".to_string(),
                "please use as of timestamp".to_string(),
            )
            .into());
        }
        AsOf::ProcessTimeWithInterval((value, leading_field)) => {
            let interval = Interval::parse_with_fields(
                value,
                Some(crate::Binder::bind_date_time_field(leading_field.clone())),
            )
            .map_err(|_| anyhow!("fail to parse interval"))?;
            let interval_sec = (interval.epoch_in_micros() / 1_000_000) as i64;
            let timestamp = chrono::Utc::now()
                .timestamp()
                .checked_sub(interval_sec)
                .ok_or_else(|| anyhow!("invalid timestamp"))?;
            AsOfType::Timestamp(as_of::Timestamp { timestamp })
        }
    };
    Ok(Some(PbAsOf {
        as_of_type: Some(as_of_type),
    }))
}
