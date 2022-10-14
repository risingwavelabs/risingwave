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

use risingwave_common::types::NaiveDateTimeWrapper;
use unix_ts::Timestamp;

use crate::Result;

#[inline(always)]
pub fn to_timestamp(unix: i64) -> Result<NaiveDateTimeWrapper> {
    Ok(NaiveDateTimeWrapper::new(
        Timestamp::from_micros(unix).to_naive_datetime(),
    ))
}

// #[cfg(test)]
// mod tests {
//     use super::*;

// }
