use super::{OrderedColumnDesc, TableId};

/// the table descriptor of table with cell based encoding in state store and include all
/// information for compute node to access data of the table.
#[derive(Debug, Clone)]
pub struct CellBasedTableDesc {
    /// id of the table, to find in Storage()
    pub table_id: TableId,
    /// the primary key columns' descriptor
    pub pk: Vec<OrderedColumnDesc>,
}
