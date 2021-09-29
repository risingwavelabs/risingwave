use std::sync::Arc;

use super::MemColumnarTable;
use super::MemRowTable;

#[derive(Clone)]
pub enum TableRef {
    Columnar(Arc<MemColumnarTable>),
    Row(Arc<MemRowTable>),
}
