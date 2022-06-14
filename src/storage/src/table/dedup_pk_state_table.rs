use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::sort_util::OrderType;

use crate::dedup_pk_cell_based_row_serializer::DedupPkCellBasedRowSerializer;
use crate::table::state_table::StateTableExtended;
use crate::{Keyspace, StateStore};

pub type DedupPkStateTable<S> = StateTableExtended<S, DedupPkCellBasedRowSerializer>;

impl<S: StateStore> DedupPkStateTable<S> {
    pub fn new_dedup_pk_state_table(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        dist_key_indices: Option<Vec<usize>>,
        pk_indices: Vec<usize>,
    ) -> Self {
        StateTableExtended::new_extended(
            keyspace,
            column_descs,
            order_types,
            dist_key_indices,
            pk_indices,
            DedupPkCellBasedRowSerializer::new(),
        )
    }
}
