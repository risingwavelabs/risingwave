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

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{LicenseError, LicenseManager, report_telemetry};

/// Define all features that require a license to use.
///
/// # Define a new feature
///
/// To add a new feature, add a new entry below following the same pattern as the existing ones.
/// ALWAYS add a new entry at the END to keep the order of the enum.
///
/// # Check the availability of a feature
///
/// To check the availability of a feature during runtime, call the method
/// [`check_available`](Feature::check_available) on the feature. If the feature is not available,
/// an error of type [`FeatureNotAvailable`] will be returned and you should handle it properly,
/// generally by returning an error to the user.
///
/// # Feature availability in tests
///
/// In tests with `debug_assertions` enabled, a special license key with all features enabled is set by
/// default. To test the behavior when a feature is not available, you can manually set a license key.
/// Check the e2e test cases under `error_ui` for examples.
macro_rules! for_all_features {
    ($macro:ident) => {
        $macro! {
            // name                      doc
            { TestPaid,                  "A dummy feature that's only available on paid tier for testing purposes." },
            { TimeTravel,                "Query historical data within the retention period."},
            { GlueSchemaRegistry,        "Use Schema Registry from AWS Glue rather than Confluent." },
            { SnowflakeSink,             "Delivering data to SnowFlake." },
            { DynamoDbSink,              "Delivering data to DynamoDb." },
            { OpenSearchSink,            "Delivering data to OpenSearch." },
            { BigQuerySink,              "Delivering data to BigQuery." },
            { ClickHouseSharedEngine,    "Delivering data to Shared tree on clickhouse cloud"},
            { SecretManagement,          "Secret management." },
            { SqlServerSink,             "Sink data from RisingWave to SQL Server." },
            { SqlServerCdcSource,        "CDC source connector for Sql Server." },
            { CdcAutoSchemaChange,       "Auto replicate upstream DDL to CDC Table." },
            { IcebergSinkWithGlue,       "Delivering data to Iceberg with Glue catalog." },
            { ElasticDiskCache,          "Disk cache and refilling to boost performance and reduce object store access cost." },
            { ResourceGroup,             "Resource group to isolate workload and failure." },
            { DatabaseFailureIsolation,  "Failure isolation between databases." },
            { IcebergCompaction,         "Auto iceberg compaction." },
        }
    };
}

macro_rules! def_feature {
    ($({ $name:ident, $doc:literal },)*) => {
        /// A set of features that are available based on the license.
        ///
        /// To define a new feature, add a new entry in the macro [`for_all_features`].
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub enum Feature {
            $(
                #[doc = $doc]
                $name,
            )*
        }

        impl Feature {
            /// Name of the feature.
            pub(crate) fn name(&self) -> &'static str {
                match &self {
                    $(
                        Self::$name => stringify!($name),
                    )*
                }
            }

            /// Get a slice of all features.
            pub(crate) fn all() -> &'static [Feature] {
                &[
                    $(
                        Feature::$name,
                    )*
                ]
            }
        }
    };
}

for_all_features!(def_feature);

impl Feature {
    /// Get a slice of all features available as of 2.5 (before we introduce custom tier).
    pub(crate) fn all_as_of_2_5() -> &'static [Feature] {
        // `IcebergCompaction` was the last feature introduced.
        &Feature::all()[..=Feature::IcebergCompaction as usize]
    }
}

/// The error type for feature not available due to license.
#[derive(Debug, Error)]
pub enum FeatureNotAvailable {
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
