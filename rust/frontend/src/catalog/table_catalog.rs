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

#[derive(Clone, Debug)]
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
        let columns = tb.columns.clone();
        let mut col_names = HashSet::new();
        let mut col_descs: HashMap<i32, ColumnDesc> = HashMap::new();
        for col in &columns {
            let col_desc = col.column_desc.clone().unwrap();
            let col_name = col_desc.name.clone();
            if !col_names.insert(col_name.clone()) {
                panic!("duplicated column name {} in talbe {} ", col_name, tb.name)
            }
            let col_id = col_desc.column_id;
            col_descs.insert(col_id, col_desc.into());
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

        let columns = columns.into_iter().map(ColumnCatalog::from).collect();
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
