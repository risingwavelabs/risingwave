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

use risingwave_common::license::Feature;
use risingwave_expr::{function, ExprError, Result};

/// A function that checks if the `TestPaid` feature is available.
///
/// It's mainly for testing purposes only.
#[function("test_paid_tier() -> boolean")]
pub fn test_paid_tier() -> Result<bool> {
    Feature::TestPaid
        .check_available()
        .map_err(|e| ExprError::Internal(anyhow::Error::from(e)))?;
    Ok(true)
}
