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

use risingwave_common::types::Timestamptz;
use risingwave_common::util::epoch;
use risingwave_expr::{ExprError, Result, function};

/// Get the processing time in Timestamptz scalar from the task-local epoch.
#[function("proctime() -> timestamptz", volatile)]
fn proctime() -> Result<Timestamptz> {
    let epoch = epoch::task_local::curr_epoch().ok_or(ExprError::Context("EPOCH"))?;
    Ok(epoch.as_timestamptz())
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::{Epoch, EpochPair};

    use super::*;

    #[tokio::test]
    async fn test_proctime() {
        let curr_epoch = Epoch::now();
        let epoch = EpochPair {
            curr: curr_epoch.0,
            prev: 0,
        };

        let proctime = epoch::task_local::scope(epoch, async { proctime().unwrap() }).await;

        assert_eq!(
            proctime,
            Timestamptz::from_millis(curr_epoch.as_unix_millis() as i64).unwrap()
        );
    }
}
