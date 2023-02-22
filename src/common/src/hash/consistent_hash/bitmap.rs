// Copyright 2023 RisingWave Labs
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

use std::ops::RangeInclusive;

use crate::buffer::Bitmap;
use crate::hash::VirtualNode;

/// An extension trait for `Bitmap` to support virtual node operations.
#[easy_ext::ext(VnodeBitmapExt)]
impl Bitmap {
    /// Enumerates the virtual nodes set to 1 in the bitmap.
    pub fn iter_vnodes(&self) -> impl Iterator<Item = VirtualNode> + '_ {
        self.iter_ones().map(VirtualNode::from_index)
    }

    /// Returns an iterator which yields the position ranges of continuous virtual nodes set to 1 in
    /// the bitmap.
    pub fn vnode_ranges(&self) -> impl Iterator<Item = RangeInclusive<VirtualNode>> + '_ {
        self.high_ranges()
            .map(|r| (VirtualNode::from_index(*r.start())..=VirtualNode::from_index(*r.end())))
    }
}
