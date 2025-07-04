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

#![allow(clippy::doc_markdown)]

use strum::VariantArray;
use thiserror::Error;

use super::{LicenseError, LicenseManager, report_telemetry};

// Define all features that require a license to use.
//
// # Define a new feature
//
// To add a new feature, add a new entry at the END of `feature.json`, following the same pattern
// as the existing ones.
//
// # Check the availability of a feature
//
// To check the availability of a feature during runtime, call the method
// [`check_available`](Feature::check_available) on the feature. If the feature is not available,
// an error of type [`FeatureNotAvailable`] will be returned and you should handle it properly,
// generally by returning an error to the user.
//
// # Feature availability in tests
//
// In tests with `debug_assertions` enabled, a special license key with all features enabled is set by
// default. To test the behavior when a feature is not available, you can manually set a license key.
// Check the e2e test cases under `error_ui` for examples.
typify::import_types!(
    schema = "src/feature.json",
    derives = [strum::VariantArray, strum::IntoStaticStr],
);

impl Feature {
    /// Name of the feature.
    pub(crate) fn name(self) -> &'static str {
        self.into()
    }

    /// Get a slice of all features.
    pub(crate) fn all() -> &'static [Feature] {
        &Feature::VARIANTS
    }

    /// Get a slice of all features available as of 2.5 (before we introduce custom tier).
    pub(crate) fn all_as_of_2_5() -> &'static [Feature] {
        // `IcebergCompaction` was the last feature introduced.
        &Feature::all()[..=Feature::IcebergCompaction as usize]
    }
}

/// The error type for feature not available due to license.
#[derive(Debug, Error)]
pub enum FeatureNotAvailable {
    // TODO(license): refine error message to include tier name & better instructions
    #[error(
        "feature {feature:?} is not available based on your license\n\n\
        Hint: You may want to set a license key with `ALTER SYSTEM SET license_key = '...';` command."
    )]
    NotAvailable { feature: Feature },

    #[error("feature {feature:?} is not available due to license error")]
    LicenseError {
        feature: Feature,
        source: LicenseError,
    },
}

impl Feature {
    /// Check whether the feature is available based on the given license manager.
    pub(crate) fn check_available_with(
        self,
        manager: &LicenseManager,
    ) -> Result<(), FeatureNotAvailable> {
        let check_res = match manager.license() {
            Ok(license) => {
                if license.tier.available_features().any(|x| x == self) {
                    Ok(())
                } else {
                    Err(FeatureNotAvailable::NotAvailable { feature: self })
                }
            }
            Err(error) => Err(FeatureNotAvailable::LicenseError {
                feature: self,
                source: error,
            }),
        };

        report_telemetry(&self, self.name(), check_res.is_ok());

        check_res
    }

    /// Check whether the feature is available based on the current license.
    pub fn check_available(self) -> Result<(), FeatureNotAvailable> {
        self.check_available_with(LicenseManager::get())
    }
}
