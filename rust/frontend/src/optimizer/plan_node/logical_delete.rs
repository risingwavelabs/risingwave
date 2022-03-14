/// [`LogicalDelete`] iterates on input relation and insert the data into specified table.
///
/// It corresponds to the `INSERT` statements in SQL. Especially, for `INSERT ... VALUES`
/// statements, the input relation would be [`super::LogicalValues`].
#[derive(Debug, Clone)]
pub struct LogicalDelete {
    pub base: LogicalBase,
    table: BaseTableRef,
    columns: Vec<ColumnId>,
    input: PlanRef,
}
