use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{CellBasedTableDesc, ColumnDesc, OrderedColumnDesc};
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::plan::{ColumnCatalog, OrderType as ProstOrderType};

use crate::catalog::TableId;

#[derive(Clone)]
pub struct TableCatalog {
    id: TableId,
    columns: Vec<ColumnCatalog>,
    pk_desc: Vec<OrderedColumnDesc>,
}

pub const ROWID_NAME: &str = "_row_id";

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
            pk: self.pk_desc,
        }
    }
}

impl From<&ProstTable> for TableCatalog {
    fn from(tb: &ProstTable) -> Self {
        let id = tb.id;
        let columns = tb.column_catalog;
        let mut col_names = HashSet::new();
        let mut col_descs: HashMap<i32, ColumnDesc> = HashMap::new();
        for col in &columns {
            let col_desc = col.column_desc.unwrap();
            let col_name = col_desc.name.clone();
            if !col_names.insert(col_name) {
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
                    .clone()
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
            columns,
            pk_desc,
        }
    }
}
