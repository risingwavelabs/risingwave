// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::vnode::VirtualNode;

/// A trait for accessing the vnode count field with backward compatibility.
pub trait VnodeCountCompat {
    /// Returns the vnode count, or [`VirtualNode::COUNT`] if the vnode count is not set,
    /// typically for backward compatibility.
    ///
    /// See the documentation on the field of the implementing type for more details.
    fn vnode_count(&self) -> usize;
}

macro_rules! impl_maybe_vnode_count_compat {
    ($($ty:ty),* $(,)?) => {
        $(
            impl VnodeCountCompat for $ty {
                fn vnode_count(&self) -> usize {
                    self.maybe_vnode_count
                        .map_or(VirtualNode::COUNT, |v| v as _)
                }
            }
        )*
    };
}

impl_maybe_vnode_count_compat!(
    risingwave_pb::plan_common::StorageTableDesc,
    risingwave_pb::catalog::Table,
    risingwave_pb::meta::table_fragments::Fragment,
);
