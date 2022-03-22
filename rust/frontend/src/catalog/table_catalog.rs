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
//
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{CellBasedTableDesc, ColumnDesc, OrderedColumnDesc};
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::plan::OrderType as ProstOrderType;

use super::column_catalog::ColumnCatalog;
use crate::catalog::TableId;

#[derive(Clone, Debug, PartialEq)]
pub struct TableCatalog {
    id: TableId,
    name: String,
    columns: Vec<ColumnCatalog>,
    pk_desc: Vec<OrderedColumnDesc>,
}

impl TableCatalog {
    /// Get a reference to the table catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Get a reference to the table catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a reference to the table catalog's pk desc.
    pub fn pk_desc(&self) -> &[OrderedColumnDesc] {
        self.pk_desc.as_ref()
    }

    /// Get a CellBasedTableDesc of the table.
    pub fn cell_based_table(&self) -> CellBasedTableDesc {
        CellBasedTableDesc {
            table_id: self.id,
            pk: self.pk_desc.clone(),
        }
    }

    /// Get a reference to the table catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl From<ProstTable> for TableCatalog {
    fn from(tb: ProstTable) -> Self {
        let id = tb.id;
        let name = tb.name.clone();
        // let columns = tb.columns;
        let mut col_names = HashSet::new();
        let mut col_descs: HashMap<i32, ColumnDesc> = HashMap::new();
        let columns: Vec<ColumnCatalog> = tb.columns.into_iter().map(ColumnCatalog::from).collect();
        for catalog in columns.clone() {
            for col_desc in catalog.column_desc.get_column_descs() {
                let col_name = col_desc.name.clone();
                if !col_names.insert(col_name.clone()) {
                    panic!("duplicated column name {} in talbe {} ", col_name, tb.name)
                }
                let col_id = col_desc.column_id.get_id();
                col_descs.insert(col_id, col_desc);
            }
        }
        let pk_desc = tb
            .pk_column_ids
            .clone()
            .into_iter()
            .zip_eq(
                tb.pk_orders
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
            name,
            pk_desc,
            columns,
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
    use risingwave_common::types::*;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::plan::{ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc};

    use crate::catalog::column_catalog::ColumnCatalog;
    use crate::catalog::table_catalog::TableCatalog;
    use crate::handler::create_table::ROWID_NAME;

    #[test]
    fn test_into_table_catalog() {
        let table: TableCatalog = ProstTable {
            id: 0,
            schema_id: 0,
            database_id: 0,
            name: "test".to_string(),
            columns: vec![
                ProstColumnCatalog {
                    column_desc: Some(ProstColumnDesc {
                        column_id: 0,
                        name: ROWID_NAME.to_string(),
                        field_descs: vec![],
                        column_type: Some(DataType::Int32.to_protobuf()),
                        type_name: String::new(),
                    }),
                    is_hidden: true,
                },
                ProstColumnCatalog {
                    column_desc: Some(ProstColumnDesc::new_struct(
                        DataType::Struct {
                            fields: vec![].into(),
                        }
                        .to_protobuf(),
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
            pk_column_ids: vec![0],
            pk_orders: vec![OrderType::Ascending.to_prost() as i32],
            dependent_relations: vec![],
            optional_associated_source_id: None,
        }
        .into();

        assert_eq!(
            table,
            TableCatalog {
                id: TableId::new(0),
                name: "test".to_string(),
                columns: vec![
                    ColumnCatalog {
                        column_desc: ColumnDesc {
                            data_type: DataType::Int32,
                            column_id: ColumnId::new(0),
                            name: ROWID_NAME.to_string(),
                            field_descs: vec![],
                            type_name: String::new()
                        },
                        is_hidden: true,
                    },
                    ColumnCatalog {
                        column_desc: ColumnDesc {
                            data_type: DataType::Struct {
                                fields: vec![DataType::Varchar,DataType::Varchar].into()
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
                pk_desc: vec![OrderedColumnDesc {
                    column_desc: ColumnDesc {
                        data_type: DataType::Int32,
                        column_id: ColumnId::new(0),
                        name: ROWID_NAME.to_string(),
                        field_descs: vec![],
                        type_name: String::new()
                    },
                    order: OrderType::Ascending
                }]
            }
        );
    }
}
