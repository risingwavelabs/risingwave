use super::{OrderedColumnDesc, TableId};

/// the catalog of physical table in our system which can be query by batch, and the enum including
/// the table descriptor all information for compute node to access data of the table.
enum PhysicalTable {
    CellBased(CellBasedTableDesc),
}

/// the table descriptor of table with cell based encoding in state store
#[derive(Debug, Clone)]
pub struct CellBasedTableDesc {
    /// id of the table, to find in Storage()
    pub table_id: TableId,
    /// the primary key columns' descriptor
    pub pk: Vec<OrderedColumnDesc>,
    // TODO: the other columns' descriptor, or all the columns? it is actually not used in the
    // compute node... for the `RowSeqScan` plan node include its requiring `columnDesc`. so we
    // don't need add it to the desc
    // pub columns: Vec<ColumnDesc>,
}
