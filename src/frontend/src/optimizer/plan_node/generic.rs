use std::rc::Rc;

use risingwave_common::catalog::TableDesc;

use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::IndexCatalog;
use crate::expr::ExprImpl;
use crate::optimizer::property::Order;
use crate::utils::Condition;

/// `TopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct TopN<PlanRef> {
    pub input: PlanRef,
    pub limit: usize,
    pub offset: usize,
    pub order: Order,
    pub group_key: Vec<usize>,
}

/// [`Scan`] returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct Scan {
    pub table_name: String,
    pub is_sys_table: bool,
    /// Include `output_col_idx` and columns required in `predicate`
    pub required_col_idx: Vec<usize>,
    pub output_col_idx: Vec<usize>,
    // Descriptor of the table
    pub table_desc: Rc<TableDesc>,
    // Descriptors of all indexes on this table
    pub indexes: Vec<Rc<IndexCatalog>>,
    /// The pushed down predicates. It refers to column indexes of the table.
    pub predicate: Condition,
}

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct Source(pub Rc<SourceCatalog>);

/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct Project<PlanRef> {
    exprs: Vec<ExprImpl>,
    input: PlanRef,
}
