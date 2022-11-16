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

use std::ops::Bound;
use std::pin::Pin;

use risingwave_common::buffer::Bitmap;
use risingwave_storage::table::streaming_table::state_table::{RowStream, StateTable};
use risingwave_storage::StateStore;

use super::StreamExecutorResult;

pub(super) async fn get_rowstreams_by_new_vnodes<'a, 'b, 'c, S: StateStore>(
    prev_vnode_bitmap: Option<&'a Bitmap>,
    curr_vnode_bitmap: &'b Bitmap,
    state_table: &'c StateTable<S>,
) -> StreamExecutorResult<Vec<Pin<Box<RowStream<'c, S>>>>> {
    // Read data with vnodes that are newly owned by this executor from state store. This is
    // performed both on initialization and on scaling.
    let newly_owned_vnodes = if let Some(prev_vnode_bitmap) = prev_vnode_bitmap {
        Bitmap::bit_saturate_subtract(curr_vnode_bitmap, prev_vnode_bitmap)
    } else {
        curr_vnode_bitmap.to_owned()
    };
    let mut values_per_vnode = Vec::with_capacity(newly_owned_vnodes.num_high_bits());
    for owned_vnode in newly_owned_vnodes.ones() {
        let value_iter = state_table
            .iter_with_pk_range(&(Bound::Unbounded, Bound::Unbounded), owned_vnode as _)
            .await?;
        let value_iter = Box::pin(value_iter);
        values_per_vnode.push(value_iter);
    }
    Ok(values_per_vnode)
}
