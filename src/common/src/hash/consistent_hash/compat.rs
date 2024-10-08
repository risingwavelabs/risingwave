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
    /// Returns the vnode count, or [`VirtualNode::COUNT_FOR_COMPAT`] if the vnode count is not set,
    /// typically for backward compatibility.
    ///
    /// See the documentation on the field of the implementing type for more details.
    fn vnode_count(&self) -> usize;
}

/// Implement the trait for given types by delegating to the `maybe_vnode_count` field.
///
/// The reason why there's a `maybe_` prefix is that, a getter method with the same name
/// as the field will be generated for `prost` structs. Directly naming it `vnode_count`
/// will lead to the method `vnode_count()` returning `0` when the field is unset, which
/// can be misleading sometimes.
///
/// Instead, we name the field as `maybe_vnode_count` and provide the method `vnode_count`
/// through this trait, ensuring that backward compatibility is handled properly.
macro_rules! impl_maybe_vnode_count_compat {
    ($($ty:ty),* $(,)?) => {
        $(
            impl VnodeCountCompat for $ty {
                fn vnode_count(&self) -> usize {
                    self.maybe_vnode_count
                        .map_or(VirtualNode::COUNT_FOR_COMPAT, |v| v as _)
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
