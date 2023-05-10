use std::fmt;
use std::hash::Hash;

use educe::Educe;
use risingwave_common::catalog::TableVersionId;

use crate::catalog::TableId;
use crate::expr::ExprImpl;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Update<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub exprs: Vec<ExprImpl>,
    pub returning: bool,
}

impl<PlanRef: Eq + Hash> Update<PlanRef> {
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        exprs: Vec<ExprImpl>,
        returning: bool,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_version_id,
            input,
            exprs,
            returning,
        }
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}, exprs: {:?}{} }}",
            name,
            self.table_name,
            self.exprs,
            if self.returning {
                ", returning: true"
            } else {
                ""
            }
        )
    }
}
