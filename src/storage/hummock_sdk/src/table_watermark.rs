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

use std::collections::BTreeMap;

use bytes::Bytes;
use risingwave_common::hash::VirtualNode;

#[derive(Clone)]
pub struct ReadTableWatermark {
    pub direction: WatermarkDirection,
    pub vnode_watermarks: BTreeMap<VirtualNode, Bytes>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WatermarkDirection {
    Ascending,
    Descending,
}

impl WatermarkDirection {
    pub fn filter_by_watermark(&self, key: impl AsRef<[u8]>, watermark: impl AsRef<[u8]>) -> bool {
        let key = key.as_ref();
        let watermark = watermark.as_ref();
        match self {
            WatermarkDirection::Ascending => key < watermark,
            WatermarkDirection::Descending => key > watermark,
        }
    }
}
