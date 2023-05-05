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
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use educe::Educe;
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use super::super::utils::TableCatalogBuilder;
use super::GenericPlanNode;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::{TableCatalog, WithOptions};

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone, Derivative)]
#[educe(PartialEq, Eq, Hash)]
pub struct Source {
    /// If there is an external stream source, `catalog` will be `Some`. Otherwise, it is `None`.
    pub catalog: Option<Rc<SourceCatalog>>,
    /// NOTE(Yuanxin): Here we store column descriptions, pk column ids, and row id index for plan
    /// generating, even if there is no external stream source.
    pub column_catalog: Vec<ColumnCatalog>,
    pub row_id_index: Option<usize>,
    /// Whether the "SourceNode" should generate the row id column for append only source
    pub gen_row_id: bool,
    /// True if it is a source created when creating table with a source.
    pub for_table: bool,
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

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.row_id_index.map(|idx| vec![idx])
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.logical_pk();
        match pk_indices {
            Some(pk_indices) => {
                FunctionalDependencySet::with_key(self.column_catalog.len(), &pk_indices)
            }
            None => FunctionalDependencySet::new(self.column_catalog.len()),
        }
    }
}

impl Source {
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

    pub fn infer_internal_table_catalog() -> TableCatalog {
        // note that source's internal table is to store partition_id -> offset mapping and its
        // schema is irrelevant to input schema
        // On the premise of ensuring that the materialized_source data can be cleaned up, keep the
        // state in source.
        // Source state doesn't maintain retention_seconds, internal_table_subset function only
        // returns retention_seconds so default is used here
        let mut builder = TableCatalogBuilder::new(WithOptions::new(HashMap::default()));

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

        builder.build(vec![], 1)
    }
}
