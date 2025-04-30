// Copyright 2025 RisingWave Labs
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

use risingwave_common::bail;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::Result;

use super::Bucket;

#[derive(Clone, Copy, Default, Debug, EstimateSize, PartialEq, Eq)]
pub struct AppendOnlyBucket(pub u8);

impl Bucket for AppendOnlyBucket {
    fn update(&mut self, index: u8, retract: bool) -> Result<()> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }
        if retract {
            bail!("HyperLogLog: Deletion in append-only bucket");
        }
        if index > self.0 {
            self.0 = index;
        }
        Ok(())
    }

    fn max(&self) -> u8 {
        self.0
    }
}
