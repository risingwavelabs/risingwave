// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        let cell_deserializer = DedupPkCellBasedRowSerializer::new(&pk_indices, &column_descs);
        StateTableExtended::new_extended(
            keyspace,
            column_descs,
            order_types,
            dist_key_indices,
            pk_indices,
            cell_deserializer,
        )
    }
}
