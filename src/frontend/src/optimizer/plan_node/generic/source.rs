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
use std::rc::Rc;

use derivative::Derivative;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use super::super::utils::TableCatalogBuilder;
use super::GenericPlanNode;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::ColumnId;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::{TableCatalog, WithOptions};

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone, Derivative)]
#[derivative(PartialEq, Eq, Hash)]
pub struct Source {
    /// If there is an external stream source, `catalog` will be `Some`. Otherwise, it is `None`.
    pub catalog: Option<Rc<SourceCatalog>>,
    /// NOTE(Yuanxin): Here we store column descriptions, pk column ids, and row id index for plan
    /// generating, even if there is no external stream source.
    pub column_descs: Vec<ColumnDesc>,
    pub pk_col_ids: Vec<ColumnId>,
    pub row_id_index: Option<usize>,
    /// Whether the "SourceNode" should generate the row id column for append only source
    pub gen_row_id: bool,
    /// True if it is a source created when creating table with a source.
    pub for_table: bool,
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for Source {
    fn schema(&self) -> Schema {
        let fields = self.column_descs.iter().map(Into::into).collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let mut id_to_idx = HashMap::new();
        self.column_descs.iter().enumerate().for_each(|(idx, c)| {
            id_to_idx.insert(c.column_id, idx);
        });
        self.pk_col_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied())
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.logical_pk();
        match pk_indices {
            Some(pk_indices) => {
                FunctionalDependencySet::with_key(self.column_descs.len(), &pk_indices)
            }
            None => FunctionalDependencySet::new(self.column_descs.len()),
        }
    }
}

impl Source {
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
        builder.add_order_column(ordered_col_idx, OrderType::default_ascending());

        builder.build(vec![], 1)
    }
}
