// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableDesc;
use risingwave_common::types::ParallelUnitId;
use risingwave_common::util::compress::decompress_data;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::Table as ProstTable;

use super::column_catalog::ColumnCatalog;
use super::{DatabaseId, SchemaId};
use crate::catalog::TableId;
use crate::optimizer::property::FieldOrder;

#[derive(Clone, Debug, PartialEq)]
pub struct TableCatalog {
    pub id: TableId,

    pub associated_source_id: Option<TableId>, // TODO: use SourceId

    pub name: String,

    /// All columns in this table
    pub columns: Vec<ColumnCatalog>,

    /// Keys used as materialize's storage key prefix, including MV order keys and pks.
    pub order_keys: Vec<FieldOrder>,
    /// The ORDER BY specified by the user. Describes how the MV is logically ordered.
    /// Only used for MV table.
    pub user_order_by: Option<Vec<FieldOrder>>,

    /// Primary key columns indices.
    pub pks: Vec<usize>,

    /// Distribution key column indices.
    pub distribution_keys: Vec<usize>,

    /// If set to Some(TableId), then this table is an index on another table.
    pub is_index_on: Option<TableId>,

    /// The appendonly attribute is derived from `StreamMaterialize` and `StreamTableScan` relies
    /// on this to derive an append-only stream plan
    pub appendonly: bool,

    /// Owner of the table.
    pub owner: String,

    /// Mapping from vnode to parallel unit. Indicates data distribution and partition of the
    /// table.
    pub vnode_mapping: Option<Vec<ParallelUnitId>>,

    pub properties: HashMap<String, String>,
}

impl TableCatalog {
    /// Get a reference to the table catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Get the table catalog's associated source id.
    #[must_use]
    pub fn associated_source_id(&self) -> Option<TableId> {
        self.associated_source_id
    }

    /// Get a reference to the table catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a reference to the table catalog's pk desc.
    pub fn order_keys(&self) -> &[FieldOrder] {
        self.order_keys.as_ref()
    }

    /// Get a [`TableDesc`] of the table.
    pub fn table_desc(&self) -> TableDesc {
        TableDesc {
            table_id: self.id,
            order_keys: self
                .order_keys
                .iter()
                .map(FieldOrder::to_order_pair)
                .collect(),
            user_order_by: self
                .user_order_by
                .as_ref()
                .map(|vec| vec.iter().map(FieldOrder::to_order_pair).collect()),
            pks: self.pks.clone(),
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            distribution_keys: self.distribution_keys.clone(),
            appendonly: self.appendonly,
            vnode_mapping: self.vnode_mapping.clone(),
        }
    }

    /// Get a reference to the table catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn distribution_keys(&self) -> &[usize] {
        self.distribution_keys.as_ref()
    }

    pub fn to_prost(&self, schema_id: SchemaId, database_id: DatabaseId) -> ProstTable {
        ProstTable {
            id: self.id.table_id as u32,
            schema_id,
            database_id,
            name: self.name.clone(),
            columns: self.columns().iter().map(|c| c.to_protobuf()).collect(),
            order_keys: self.order_keys.iter().map(|o| o.to_protobuf()).collect(),
            user_order_by: self
                .user_order_by
                .as_ref()
                .map(|vec| vec.iter().map(|o| o.to_protobuf()).collect())
                .unwrap_or_default(),
            pk: self.pks.iter().map(|x| *x as _).collect(),
            dependent_relations: vec![],
            optional_associated_source_id: self
                .associated_source_id
                .map(|source_id| OptionalAssociatedSourceId::AssociatedSourceId(source_id.into())),
            is_index: self.is_index_on.is_some(),
            index_on_id: self.is_index_on.unwrap_or_default().table_id(),
            distribution_keys: self
                .distribution_keys
                .iter()
                .map(|k| *k as i32)
                .collect_vec(),
            appendonly: self.appendonly,
            owner: self.owner.clone(),
            mapping: None,
            properties: HashMap::default(),
        }
    }
}

impl From<ProstTable> for TableCatalog {
    fn from(tb: ProstTable) -> Self {
        let id = tb.id;
        let associated_source_id = tb.optional_associated_source_id.map(|id| match id {
            OptionalAssociatedSourceId::AssociatedSourceId(id) => id,
        });
        let name = tb.name.clone();
        let mut col_names = HashSet::new();
        let mut col_index: HashMap<i32, usize> = HashMap::new();
        let columns: Vec<ColumnCatalog> = tb.columns.into_iter().map(ColumnCatalog::from).collect();
        for (idx, catalog) in columns.clone().into_iter().enumerate() {
            for col_desc in catalog.column_desc.flatten() {
                let col_name = col_desc.name.clone();
                if !col_names.insert(col_name.clone()) {
                    panic!("duplicated column name {} in table {} ", col_name, tb.name)
                }
            }

            let col_id = catalog.column_desc.column_id.get_id();
            col_index.insert(col_id, idx);
        }

        let order_keys = tb
            .order_keys
            .iter()
            .map(FieldOrder::from_protobuf)
            .collect();
        let user_order_by = tb
            .user_order_by
            .iter()
            .map(FieldOrder::from_protobuf)
            .collect();

        let vnode_mapping = if let Some(mapping) = tb.mapping.as_ref() {
            decompress_data(&mapping.original_indices, &mapping.data)
        } else {
            vec![]
        };

        Self {
            id: id.into(),
            associated_source_id: associated_source_id.map(Into::into),
            name,
            order_keys,
            user_order_by: Some(user_order_by),
            columns,
            is_index_on: if tb.is_index {
                Some(tb.index_on_id.into())
            } else {
                None
            },
            distribution_keys: tb
                .distribution_keys
                .iter()
                .map(|k| *k as usize)
                .collect_vec(),
            pks: tb.pk.iter().map(|x| *x as _).collect(),
            appendonly: tb.appendonly,
            owner: tb.owner,
            vnode_mapping: Some(vnode_mapping),
            properties: tb.properties,
        }
    }
}

impl From<&ProstTable> for TableCatalog {
    fn from(tb: &ProstTable) -> Self {
        tb.clone().into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::*;
    use risingwave_common::util::compress::compress_data;
    use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::common::ParallelUnitMapping;
    use risingwave_pb::plan_common::{
        ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc,
    };

    use crate::catalog::column_catalog::ColumnCatalog;
    use crate::catalog::row_id_column_desc;
    use crate::catalog::table_catalog::TableCatalog;
    use crate::optimizer::property::{Direction, FieldOrder};

    #[test]
    fn test_into_table_catalog() {
        let mapping = [1, 1, 2, 2, 3, 3, 4, 4].to_vec();
        let (original_indices, data) = compress_data(&mapping);
        let table: TableCatalog = ProstTable {
            is_index: false,
            index_on_id: 0,
            id: 0,
            schema_id: 0,
            database_id: 0,
            name: "test".to_string(),
            columns: vec![
                ProstColumnCatalog {
                    column_desc: Some((&row_id_column_desc()).into()),
                    is_hidden: true,
                },
                ProstColumnCatalog {
                    column_desc: Some(ProstColumnDesc::new_struct(
                        "country",
                        1,
                        ".test.Country",
                        vec![
                            ProstColumnDesc::new_atomic(
                                DataType::Varchar.to_protobuf(),
                                "country.address",
                                2,
                            ),
                            ProstColumnDesc::new_atomic(
                                DataType::Varchar.to_protobuf(),
                                "country.zipcode",
                                3,
                            ),
                        ],
                    )),
                    is_hidden: false,
                },
            ],
            order_keys: vec![FieldOrder {
                index: 0,
                direct: Direction::Asc,
            }
            .to_protobuf()],
            user_order_by: vec![],
            pk: vec![0],
            dependent_relations: vec![],
            distribution_keys: vec![],
            optional_associated_source_id: OptionalAssociatedSourceId::AssociatedSourceId(233)
                .into(),
            appendonly: false,
            owner: risingwave_common::catalog::DEFAULT_SUPPER_USER.to_string(),
            mapping: Some(ParallelUnitMapping {
                table_id: 0,
                original_indices,
                data,
            }),
            properties: HashMap::from([(String::from("ttl"), String::from("300"))]),
        }
        .into();

        assert_eq!(
            table,
            TableCatalog {
                is_index_on: None,
                id: TableId::new(0),
                associated_source_id: Some(TableId::new(233)),
                name: "test".to_string(),
                columns: vec![
                    ColumnCatalog::row_id_column(),
                    ColumnCatalog {
                        column_desc: ColumnDesc {
                            data_type: DataType::Struct {
                                fields: vec![DataType::Varchar, DataType::Varchar].into()
                            },
                            column_id: ColumnId::new(1),
                            name: "country".to_string(),
                            field_descs: vec![
                                ColumnDesc {
                                    data_type: DataType::Varchar,
                                    column_id: ColumnId::new(2),
                                    name: "country.address".to_string(),
                                    field_descs: vec![],
                                    type_name: String::new(),
                                },
                                ColumnDesc {
                                    data_type: DataType::Varchar,
                                    column_id: ColumnId::new(3),
                                    name: "country.zipcode".to_string(),
                                    field_descs: vec![],
                                    type_name: String::new(),
                                }
                            ],
                            type_name: ".test.Country".to_string()
                        },
                        is_hidden: false
                    }
                ],
                pks: vec![0],
                order_keys: vec![FieldOrder {
                    index: 0,
                    direct: Direction::Asc,
                }],
                user_order_by: None,
                distribution_keys: vec![],
                appendonly: false,
                owner: risingwave_common::catalog::DEFAULT_SUPPER_USER.to_string(),
                vnode_mapping: Some(mapping),
                properties: HashMap::from([(String::from("ttl"), String::from("300"))]),
            }
        );
    }
}
