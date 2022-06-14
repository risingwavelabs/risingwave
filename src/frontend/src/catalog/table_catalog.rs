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
use risingwave_common::catalog::{ColumnDesc, OrderedColumnDesc, TableDesc, DEFAULT_SUPPER_USER};
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::plan_common::OrderType as ProstOrderType;

use super::column_catalog::ColumnCatalog;
use super::{DatabaseId, SchemaId};
use crate::catalog::TableId;

#[derive(Clone, Debug, PartialEq)]
pub struct TableCatalog {
    pub id: TableId,

    pub associated_source_id: Option<TableId>, // TODO: use SourceId

    pub name: String,

    /// All columns in this table
    pub columns: Vec<ColumnCatalog>,

    /// Keys used as materialize's storage key prefix, including MV order keys and pks.
    pub order_desc: Vec<OrderedColumnDesc>,

    /// Primary key columns indices.
    pub pks: Vec<usize>,

    /// Distribution key column indices.
    pub distribution_keys: Vec<usize>,

    /// If set to Some(TableId), then this table is an index on another table.
    pub is_index_on: Option<TableId>,

    /// The appendonly attribute is derived from `StreamMaterialize` and `StreamTableScan` relies
    /// on this to derive an append-only stream plan
    pub appendonly: bool,

    //owner of table
    pub owner: String,
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
    pub fn order_desc(&self) -> &[OrderedColumnDesc] {
        self.order_desc.as_ref()
    }

    /// Get a [`TableDesc`] of the table.
    pub fn table_desc(&self) -> TableDesc {
        TableDesc {
            table_id: self.id,
            order_desc: self.order_desc.clone(),
            pks: self.pks.clone(),
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            distribution_keys: self.distribution_keys.clone(),
            appendonly: self.appendonly,
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
        let (order_column_ids, orders) = self
            .order_desc()
            .iter()
            .map(|col| {
                (
                    col.column_desc.column_id.get_id(),
                    col.order.to_prost() as i32,
                )
            })
            .unzip();

        ProstTable {
            id: self.id.table_id as u32,
            schema_id,
            database_id,
            name: self.name.clone(),
            columns: self.columns().iter().map(|c| c.to_protobuf()).collect(),
            order_column_ids,
            orders,
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
            owner: DEFAULT_SUPPER_USER.to_string(),
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
        let mut col_descs: HashMap<i32, ColumnDesc> = HashMap::new();
        let columns: Vec<ColumnCatalog> = tb.columns.into_iter().map(ColumnCatalog::from).collect();
        for catalog in columns.clone() {
            for col_desc in catalog.column_desc.flatten() {
                let col_name = col_desc.name.clone();
                if !col_names.insert(col_name.clone()) {
                    panic!("duplicated column name {} in table {} ", col_name, tb.name)
                }
                let col_id = col_desc.column_id.get_id();
                col_descs.insert(col_id, col_desc);
            }
        }

        let order_desc = tb
            .order_column_ids
            .clone()
            .into_iter()
            .zip_eq(
                tb.orders
                    .into_iter()
                    .map(|x| OrderType::from_prost(&ProstOrderType::from_i32(x).unwrap())),
            )
            .map(|(col_id, order)| OrderedColumnDesc {
                column_desc: col_descs.get(&col_id).unwrap().clone(),
                order,
            })
            .collect();

        Self {
            id: id.into(),
            associated_source_id: associated_source_id.map(Into::into),
            name,
            order_desc,
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
    use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc, TableId};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::*;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::plan_common::{
        ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc,
    };

    use crate::catalog::column_catalog::ColumnCatalog;
    use crate::catalog::row_id_column_desc;
    use crate::catalog::table_catalog::TableCatalog;

    #[test]
    fn test_into_table_catalog() {
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
            order_column_ids: vec![0],
            pk: vec![0],
            orders: vec![OrderType::Ascending.to_prost() as i32],
            dependent_relations: vec![],
            distribution_keys: vec![],
            optional_associated_source_id: OptionalAssociatedSourceId::AssociatedSourceId(233)
                .into(),
            appendonly: false,
            owner: risingwave_common::catalog::DEFAULT_SUPPER_USER.to_string(),

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
                order_desc: vec![OrderedColumnDesc {
                    column_desc: row_id_column_desc(),
                    order: OrderType::Ascending
                }],
                distribution_keys: vec![],
                appendonly: false,
                owner: risingwave_common::catalog::DEFAULT_SUPPER_USER.to_string(),
            }
        );
    }
}
