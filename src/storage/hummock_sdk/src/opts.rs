use bincode::{Decode, Encode};
use risingwave_common::catalog::{TableId, TableOption};

#[derive(Clone, Default, Copy, Encode, Decode, Debug, PartialEq, Eq)]
pub struct NewLocalOptions {
    pub table_id: TableId,
    /// Whether the operation is consistent. The term `consistent` requires the following:
    ///
    /// 1. A key cannot be inserted or deleted for more than once, i.e. inserting to an existing
    /// key or deleting an non-existing key is not allowed.
    ///
    /// 2. The old value passed from
    /// `update` and `delete` should match the original stored value.
    pub is_consistent_op: bool,
    pub table_option: TableOption,
}

impl NewLocalOptions {
    pub fn for_test(table_id: TableId) -> Self {
        Self {
            table_id,
            is_consistent_op: false,
            table_option: TableOption {
                retention_seconds: None,
            },
        }
    }
}
