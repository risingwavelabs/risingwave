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

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use educe::Educe;
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::ConnectorProperties;

use super::super::utils::TableCatalogBuilder;
use super::GenericPlanNode;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::TableCatalog;

/// In which scnario the source node is created
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[expect(clippy::enum_variant_names)]
pub enum SourceNodeKind {
    /// `CREATE TABLE` with a connector.
    CreateTable,
    /// `CREATE SOURCE` with a streaming job (backfill-able source).
    CreateSourceWithStreamjob,
    /// `CREATE MATERIALIZED VIEW` which selects from a source.
    ///
    /// Note:
    /// - For non backfill-able source, `CREATE SOURCE` will not create a source node, and `CREATE MATERIALIZE VIEW` will create a `LogicalSource`.
    /// - For backfill-able source, `CREATE MATERIALIZE VIEW` will create `LogicalSourceBackfill` instead of `LogicalSource`.
    CreateMViewOrBatch,
}

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Source {
    /// If there is an external stream source, `catalog` will be `Some`. Otherwise, it is `None`.
    pub catalog: Option<Rc<SourceCatalog>>,

    // NOTE: Here we store `column_catalog` and `row_id_index`
    // because they are needed when `catalog` is None.
    // When `catalog` is Some, they are the same as these fields in `catalog`.
    pub column_catalog: Vec<ColumnCatalog>,
    pub row_id_index: Option<usize>,

    pub kind: SourceNodeKind,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,

    /// Kafka timestamp range, currently we only support kafka, so we just leave it like this.
    pub(crate) kafka_timestamp_range: (Bound<i64>, Bound<i64>),
}

impl GenericPlanNode for Source {
    fn schema(&self) -> Schema {
        let fields = self
            .column_catalog
            .iter()
            .map(|c| (&c.column_desc).into())
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.row_id_index.map(|idx| vec![idx])
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        match pk_indices {
            Some(pk_indices) => {
                FunctionalDependencySet::with_key(self.column_catalog.len(), &pk_indices)
            }
            None => FunctionalDependencySet::new(self.column_catalog.len()),
        }
    }
}

impl Source {
    pub fn is_new_fs_connector(&self) -> bool {
        self.catalog.as_ref().is_some_and(|catalog| {
            ConnectorProperties::is_new_fs_connector_b_tree_map(&catalog.with_properties)
        })
    }

    /// The columns in stream/batch source node indicate the actual columns it will produce,
    /// instead of the columns defined in source catalog. The difference is generated columns.
    pub fn exclude_generated_columns(mut self) -> (Self, Option<usize>) {
        let original_row_id_index = self.row_id_index;
        // minus the number of generated columns before row_id_index.
        self.row_id_index = original_row_id_index.map(|idx| {
            let mut cnt = 0;
            for col in self.column_catalog.iter().take(idx + 1) {
                if col.is_generated() {
                    cnt += 1;
                }
            }
            idx - cnt
        });
        self.column_catalog.retain(|c| !c.is_generated());
        (self, original_row_id_index)
    }

    pub fn kafka_timestamp_range_value(&self) -> (Option<i64>, Option<i64>) {
        let (lower_bound, upper_bound) = &self.kafka_timestamp_range;
        let lower_bound = match lower_bound {
            Included(t) => Some(*t),
            Excluded(t) => Some(*t - 1),
            Unbounded => None,
        };

        let upper_bound = match upper_bound {
            Included(t) => Some(*t),
            Excluded(t) => Some(*t + 1),
            Unbounded => None,
        };
        (lower_bound, upper_bound)
    }

    pub fn infer_internal_table_catalog(require_dist_key: bool) -> TableCatalog {
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
            name: "offset_info".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };

        let ordered_col_idx = builder.add_column(&key);
        builder.add_column(&value);
        builder.add_order_column(ordered_col_idx, OrderType::ascending());

        builder.build(
            if require_dist_key {
                vec![ordered_col_idx]
            } else {
                vec![]
            },
            1,
        )
    }
}
