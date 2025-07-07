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

use std::num::NonZeroUsize;
use std::sync::{LazyLock, RwLock};

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use thiserror_ext::AsReport;

use crate::{Feature, LicenseKeyRef};

/// A feature that's specified in the custom tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MaybeFeature {
    /// A known feature that exists in the [`Feature`] enum.
    Feature(Feature),
    /// An unknown feature. It could be features introduced in future release. We still allow it to
    /// be here for compatibility purposes.
    Unknown(String),
}

/// License tier.
///
/// Each enterprise [`Feature`] is available for a specific tier and above.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// Free tier. No feature is available. This is more like a placeholder.
    Free,

    /// All features available as of 2.5.
    #[serde(rename = "paid")]
    AllAsOf2_5,

    /// All features available currently and in the future.
    All,

    /// Custom tier, with a list of available features.
    #[serde(untagged)]
    Custom {
        name: String,
        features: Vec<MaybeFeature>,
    },
}

impl Tier {
    /// Get all available features based on the license tier.
    #[auto_enums::auto_enum(Iterator)]
    pub fn available_features(&self) -> impl Iterator<Item = Feature> {
        match self {
            Tier::Free => std::iter::empty(),
            Tier::AllAsOf2_5 => Feature::all_as_of_2_5().iter().copied(),
            Tier::All => Feature::all().iter().copied(),
            Tier::Custom { features, .. } => features.iter().filter_map(|feature| match feature {
                MaybeFeature::Feature(feature) => Some(*feature),
                MaybeFeature::Unknown(_) => None,
            }),
        }
    }
}

/// Issuer of the license.
///
/// The issuer must be `prod.risingwave.com` in production, and can be `test.risingwave.com` in
/// development. This will be validated when refreshing the license key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Issuer {
    #[serde(rename = "prod.risingwave.com")]
    Prod,

    #[serde(rename = "test.risingwave.com")]
    Test,

    #[serde(untagged)]
    Unknown(String),
}

/// The content of a license.
///
/// We use JSON Web Token (JWT) to represent the license. This struct is the payload.
///
/// Prefer calling [`crate::Feature::check_available`] to check the availability of a feature,
/// other than directly checking the content of the license.
// TODO(license): Shall we add a version field?
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct License {
    /// Subject of the license.
    ///
    /// See <https://tools.ietf.org/html/rfc7519#section-4.1.2>.
    #[allow(dead_code)]
    pub sub: String,

    /// Issuer of the license.
    ///
    /// See <https://tools.ietf.org/html/rfc7519#section-4.1.1>.
    #[allow(dead_code)]
    pub iss: Issuer,

    /// Tier of the license.
    pub tier: Tier,

    /// Maximum number of compute-node CPU cores allowed to use. Typically used for the paid tier.
    pub cpu_core_limit: Option<NonZeroUsize>,

    /// Expiration time in seconds since UNIX epoch.
    ///
    /// See <https://tools.ietf.org/html/rfc7519#section-4.1.4>.
    pub exp: u64,
}

impl Default for License {
    /// The default license is a free license that never expires.
    ///
    /// Used when `license_key` is unset or invalid.
    fn default() -> Self {
        Self {
            sub: "default".to_owned(),
            tier: Tier::Free,
            iss: Issuer::Prod,
            cpu_core_limit: None,
            exp: u64::MAX,
        }
    }
}

/// The error type for invalid license key when verifying as JWT.
#[derive(Debug, Clone, Error)]
pub enum LicenseError {
    #[error("invalid license key")]
    InvalidKey(#[source] jsonwebtoken::errors::Error),

    #[error(
        "the license key is currently not effective because the CPU core in the cluster \
        ({actual}) exceeds the maximum allowed by the license key ({limit}); \
        consider removing some nodes or acquiring a new license key with a higher limit"
    )]
    CpuCoreLimitExceeded { limit: NonZeroUsize, actual: usize },
}

struct Inner {
    license: Result<License, LicenseError>,
    cached_cpu_core_count: usize,
}

/// The singleton license manager.
pub struct LicenseManager {
    inner: RwLock<Inner>,
}

static PUBLIC_KEY: LazyLock<DecodingKey> = LazyLock::new(|| {
    DecodingKey::from_rsa_pem(include_bytes!("key.pub"))
        .expect("invalid public key for license validation")
});

impl LicenseManager {
    /// Create a new license manager with the default license.
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                license: Ok(License::default()),
                cached_cpu_core_count: 0,
            }),
        }
    }

    /// Get the singleton instance of the license manager.
    pub fn get() -> &'static Self {
        static INSTANCE: LazyLock<LicenseManager> = LazyLock::new(LicenseManager::new);
        &INSTANCE
    }

    /// Refresh the license with the given license key.
    pub fn refresh(&self, license_key: LicenseKeyRef<'_>) {
        let license_key = license_key.0;
        let mut inner = self.inner.write().unwrap();

        // Empty license key means unset. Use the default one here.
        if license_key.is_empty() {
            inner.license = Ok(License::default());
            return;
        }

        // TODO(license): shall we also validate `nbf`(Not Before)?
        let mut validation = Validation::new(Algorithm::RS512);
        // Only accept `prod` issuer in production, so that we can use license keys issued by
        // the `test` issuer in development without leaking them to production.
        validation.set_issuer(&[
            "prod.risingwave.com",
            #[cfg(debug_assertions)]
            "test.risingwave.com",
        ]);

        inner.license = match jsonwebtoken::decode(license_key, &PUBLIC_KEY, &validation) {
            Ok(data) => Ok(data.claims),
            Err(error) => Err(LicenseError::InvalidKey(error)),
        };

        match &inner.license {
            Ok(license) => tracing::info!(?license, "license refreshed"),
            Err(error) => tracing::warn!(error = %error.as_report(), "invalid license key"),
        }
    }

    /// Update the cached CPU core count.
    pub fn update_cpu_core_count(&self, cpu_core_count: usize) {
        let mut inner = self.inner.write().unwrap();
        inner.cached_cpu_core_count = cpu_core_count;
    }

    /// Get the current license if it is valid.
    ///
    /// Since the license can expire, the returned license should not be cached by the caller.
    ///
    /// Prefer calling [`crate::Feature::check_available`] to check the availability of a feature,
    /// other than directly calling this method and checking the content of the license.
    pub fn license(&self) -> Result<License, LicenseError> {
        let inner = self.inner.read().unwrap();
        let license = inner.license.clone()?;

        // Check the expiration time additionally.
        if license.exp < jsonwebtoken::get_current_timestamp() {
            return Err(LicenseError::InvalidKey(
                jsonwebtoken::errors::ErrorKind::ExpiredSignature.into(),
            ));
        }

        // Check the CPU core limit.
        let actual_cpu_core = inner.cached_cpu_core_count;
        if let Some(limit) = license.cpu_core_limit
            && actual_cpu_core > limit.get()
        {
            return Err(LicenseError::CpuCoreLimitExceeded {
                limit,
                actual: actual_cpu_core,
            });
        }

        Ok(license)
    }
}

// Tests below only work in debug mode.
#[cfg(debug_assertions)]
#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::*;
    use crate::{LicenseKey, PROD_ALL_4_CORE_LICENSE_KEY_CONTENT, TEST_ALL_LICENSE_KEY_CONTENT};

    fn do_test(key: &str, expect: expect_test::Expect) {
        let manager = LicenseManager::new();
        manager.refresh(LicenseKey(key));

        match manager.license() {
            Ok(license) => expect.assert_debug_eq(&license),
            Err(error) => expect.assert_eq(&error.to_report_string()),
        }
    }

    #[test]
    fn test_all_license_key() {
        do_test(
            TEST_ALL_LICENSE_KEY_CONTENT,
            expect![[r#"
                License {
                    sub: "rw-test-all",
                    iss: Test,
                    tier: All,
                    cpu_core_limit: None,
                    exp: 10000627200,
                }
            "#]],
        );
    }

    #[test]
    fn test_prod_all_4_core_license_key() {
        do_test(
            PROD_ALL_4_CORE_LICENSE_KEY_CONTENT,
            expect![[r#"
                License {
                    sub: "rw-default-all-4-core",
                    iss: Prod,
                    tier: All,
                    cpu_core_limit: Some(
                        4,
                    ),
                    exp: 10000627200,
                }
            "#]],
        );
    }

    #[test]
    fn test_free_license_key() {
        const KEY: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
          eyJzdWIiOiJydy10ZXN0IiwidGllciI6ImZyZWUiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwiZXhwIjo5OTk5OTk5OTk5fQ.\
          ALC3Kc9LI6u0S-jeMB1YTxg1k8Azxwvc750ihuSZgjA_e1OJC9moxMvpLrHdLZDzCXHjBYi0XJ_1lowmuO_0iPEuPqN5AFpDV1ywmzJvGmMCMtw3A2wuN7hhem9OsWbwe6lzdwrefZLipyo4GZtIkg5ZdwGuHzm33zsM-X5gl_Ns4P6axHKiorNSR6nTAyA6B32YVET_FAM2YJQrXqpwA61wn1XLfarZqpdIQyJ5cgyiC33BFBlUL3lcRXLMLeYe6TjYGeV4K63qARCjM9yeOlsRbbW5ViWeGtR2Yf18pN8ysPXdbaXm_P_IVhl3jCTDJt9ctPh6pUCbkt36FZqO9A";

        do_test(
            KEY,
            expect![[r#"
                License {
                    sub: "rw-test",
                    iss: Test,
                    tier: Free,
                    cpu_core_limit: None,
                    exp: 9999999999,
                }
            "#]],
        );
    }

    #[test]
    fn test_empty_license_key() {
        // Default license will be used.
        do_test(
            "",
            expect![[r#"
                License {
                    sub: "default",
                    iss: Prod,
                    tier: Free,
                    cpu_core_limit: None,
                    exp: 18446744073709551615,
                }
            "#]],
        );
    }

    #[test]
    fn test_invalid_license_key() {
        const KEY: &str = "invalid";

        do_test(KEY, expect!["invalid license key: InvalidToken"]);
    }

    #[test]
    fn test_expired_license_key() {
        // "exp": 0
        const KEY: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
          eyJzdWIiOiJydy10ZXN0IiwidGllciI6InBhaWQiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwiZXhwIjowfQ.\
          TyYmoT5Gw9-FN7DWDbeg3myW8g_3Xlc90i4M9bGuPf2WLv9zRMJy2r9J7sl1BO7t6F1uGgyrvNxsVRVZ2XF_WAs6uNlluYBnd4Cqvsj6Xny1XJCCo8II3RIea-ZlRjp6tc1saaoe-_eTtqDH8NIIWe73vVtBeBTBU4zAiN2vCtU_Si2XuoTLBKJMIjtn0HjLNhb6-DX2P3SCzp75tMyWzr49qcsBgratyKdu_v2kqBM1qw_dTaRg2ZeNNO6scSOBwu4YHHJTL4nUaZO2yEodI_OKUztIPLYuO2A33Fb5OE57S7LTgSzmxZLf7e23Vrck7Os14AfBQr7p9ncUeyIXhA";

        do_test(KEY, expect!["invalid license key: ExpiredSignature"]);
    }

    #[test]
    fn test_invalid_issuer() {
        // "iss": "bad.risingwave.com"
        const KEY: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
          eyJzdWIiOiJydy10ZXN0IiwidGllciI6ImZyZWUiLCJpc3MiOiJiYWQucmlzaW5nd2F2ZS5jb20iLCJleHAiOjk5OTk5OTk5OTl9.\
          SUbDJTri902FbGgIoe5L3LG4edTXoR42BQCIu_KLyW41OK47bMnD2aK7JggyJmWyGtN7b_596hxM9HjU58oQtHePUo_zHi5li5IcRaMi8gqHae7CJGqOGAUo9vYOWCP5OjEuDfozJhpgcHBLzDRnSwYnWhLKtsrzb3UcpOXEqRVK7EDShBNx6kNqfYs2LlFI7ASsgFRLhoRuOTR5LeVDjj6NZfkZGsdMe1VyrODWoGT9kcAF--hBpUd1ZJ5mZ67A0_948VPFBYDbDPcTRnw1-5MvdibO-jKX49rJ0rlPXcAbqKPE_yYUaqUaORUzb3PaPgCT_quO9PWPuAFIgAb_fg";

        do_test(KEY, expect!["invalid license key: InvalidIssuer"]);
    }

    #[test]
    fn test_invalid_signature() {
        const KEY: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
         eyJzdWIiOiJydy10ZXN0IiwidGllciI6ImZyZWUiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwiZXhwIjo5OTk5OTk5OTk5fQ.\
         InvalidSignatureoe5L3LG4edTXoR42BQCIu_KLyW41OK47bMnD2aK7JggyJmWyGtN7b_596hxM9HjU58oQtHePUo_zHi5li5IcRaMi8gqHae7CJGqOGAUo9vYOWCP5OjEuDfozJhpgcHBLzDRnSwYnWhLKtsrzb3UcpOXEqRVK7EDShBNx6kNqfYs2LlFI7ASsgFRLhoRuOTR5LeVDjj6NZfkZGsdMe1VyrODWoGT9kcAF--hBpUd1ZJ5mZ67A0_948VPFBYDbDPcTRnw1-5MvdibO-jKX49rJ0rlPXcAbqKPE_yYUaqUaORUzb3PaPgCT_quO9PWPuAFIgAb_fg";

        do_test(KEY, expect!["invalid license key: InvalidSignature"]);
    }
}
