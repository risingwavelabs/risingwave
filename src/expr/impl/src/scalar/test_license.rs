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

use anyhow::Context;
use risingwave_common::license::{Feature, LicenseManager};
use risingwave_common::types::JsonbVal;
use risingwave_expr::{ExprError, Result, function};

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

#[function("license() -> jsonb")]
pub fn license() -> Result<JsonbVal> {
    let license = LicenseManager::get()
        .license()
        .map_err(|e| ExprError::Internal(anyhow::Error::from(e)))?;

    let value = jsonbb::to_value(license)
        .context("failed to serialize license")
        .map_err(ExprError::Internal)?;

    Ok(JsonbVal::from(value))
}
