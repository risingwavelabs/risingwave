use super::{OrderedColumnDesc, TableId};

enum PhysicalTable {
    CellBased(CellBasedTableDesc),
}

/// the table descriptor of table with cell based encoding in state store and include all
/// information for compute node to access data of the table.
#[derive(Debug, Clone)]
pub struct CellBasedTableDesc {
    /// id of the table, to find in Storage()
    pub table_id: TableId,
    /// the primary key columns' descriptor
    pub pk: Vec<OrderedColumnDesc>,
    // TODO: the other columns' descriptor, or all the columns? it is actually not used in the
    // compute node... for the plan node include its requiring `columnDesc`.
    // pub columns: Vec<ColumnDesc>,
}
