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

use risingwave_license::LicenseManager;

use super::diff::SystemParamsDiff;
use super::reader::SystemParamsReader;
use crate::util::tracing::layer::toggle_otel_layer;

/// Node-independent handler for system parameter changes.
#[derive(Debug)]
pub struct CommonHandler;

impl CommonHandler {
    /// Create a new handler with the initial parameters.
    pub fn new(initial: SystemParamsReader) -> Self {
        let this = Self;
        this.handle_change(&SystemParamsDiff::from_initial(initial));
        this
    }

    /// Handle the change of system parameters.
    pub fn handle_change(&self, diff: &SystemParamsDiff) {
        // Toggle the distributed tracing layer.
        if let Some(enabled) = diff.enable_tracing {
            toggle_otel_layer(enabled);
        }
        // Refresh the license key.
        if let Some(key) = diff.license_key.as_ref() {
            LicenseManager::get().refresh(key.as_ref());
        }
    }
}
