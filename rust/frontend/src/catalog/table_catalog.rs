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
        let mut col_names = HashSet::new();
        let mut col_descs: HashMap<i32, ColumnDesc> = HashMap::new();
        let columns: Vec<ColumnCatalog> = tb
            .columns
            .clone()
            .into_iter()
            .map(ColumnCatalog::from)
            .collect();
        for col in columns.clone() {
            for col_desc in col.get_column_descs() {
                let col_name = col_desc.name.clone();
                if !col_names.insert(col_name.clone()) {
                    panic!("duplicated column name {} in talbe {} ", col_name, tb.name)
                }
                let col_id = col_desc.column_id;
                col_descs.insert(col_id.get_id(), col_desc);
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
