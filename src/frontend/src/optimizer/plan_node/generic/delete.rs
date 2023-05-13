use std::fmt;

use risingwave_common::catalog::TableVersionId;

use crate::catalog::TableId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Delete<PlanRef> {
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub returning: bool,
}

impl<PlanRef> Delete<PlanRef> {
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        returning: bool,
    ) -> Self {
        Self {
            input,
            table_name,
            table_id,
            table_version_id,
            returning,
        }
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}{} }}",
            name,
            self.table_name,
            if self.returning {
                ", returning: true"
            } else {
                ""
            }
        )
    }
}
