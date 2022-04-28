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

use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_pb::hummock::SstableInfo;

pub trait OverlapStrategy: Send + Sync {
    fn check_overlap(&self, a: &SstableInfo, b: &SstableInfo) -> bool;
}

#[derive(Default)]
pub struct RangeOverlapStrategy {}

impl OverlapStrategy for RangeOverlapStrategy {
    fn check_overlap(&self, a: &SstableInfo, b: &SstableInfo) -> bool {
        let key_range1 = KeyRange::from(a.key_range.as_ref().unwrap());
        let key_range2 = KeyRange::from(b.key_range.as_ref().unwrap());
        key_range1.full_key_overlap(&key_range2)
    }
}
