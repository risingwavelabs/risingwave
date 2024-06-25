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

use std::sync::{LazyLock, RwLock};

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;

/// License tier.
///
/// Each enterprise [`Feature`](super::Feature) is available for a specific tier and above.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// Free tier.
    ///
    /// This is more like a placeholder. If a feature is available for the free tier, there's no
    /// need to add it to the [`Feature`](super::Feature) enum at all.
    Free,

    /// Paid tier.
    // TODO: Add more tiers if needed.
    Paid,
}

/// The content of a license.
///
/// We use JSON Web Token (JWT) to represent the license. This struct is the payload.
// TODO: Shall we add a version field?
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct License {
    /// Subject of the license.
    /// See <https://tools.ietf.org/html/rfc7519#section-4.1.2>.
    #[allow(dead_code)]
    pub sub: String,

    /// Tier of the license.
    pub tier: Tier,

    /// Expiration time in seconds since UNIX epoch.
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
            exp: u64::MAX,
        }
    }
}

/// The error type for invalid license key when verifying as JWT.
#[derive(Debug, Clone, Error)]
#[error("invalid license key")]
pub struct LicenseKeyError(#[source] jsonwebtoken::errors::Error);

struct Inner {
    license: Result<License, LicenseKeyError>,
}

/// The singleton license manager.
pub(crate) struct LicenseManager {
    inner: RwLock<Inner>,
}

static PUBLIC_KEY: LazyLock<DecodingKey> = LazyLock::new(|| {
    DecodingKey::from_rsa_pem(include_bytes!("public_key.pem"))
        .expect("invalid public key for license validation")
});

impl LicenseManager {
    /// Get the singleton instance of the license manager.
    pub fn get() -> &'static Self {
        static INSTANCE: LazyLock<LicenseManager> = LazyLock::new(|| LicenseManager {
            inner: RwLock::new(Inner {
                license: Ok(License::default()),
            }),
        });

        &INSTANCE
    }

    /// Refresh the license with the given license key.
    pub fn refresh(&self, license_key: &str) {
        let mut inner = self.inner.write().unwrap();

        // Empty license key means unset. Use the default one here.
        if license_key.is_empty() {
            inner.license = Ok(License::default());
            return;
        }

        // TODO: shall we also validate `nbf`(Not Before)?
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
            Err(error) => Err(LicenseKeyError(error)),
        };
    }

    /// Get the current license if it is valid.
    ///
    /// Since the license can expire, the returned license should not be cached by the caller.
    pub(super) fn license(&self) -> Result<License, LicenseKeyError> {
        let license = self.inner.read().unwrap().license.clone()?;

        // Check the expiration time additionally.
        if license.exp < jsonwebtoken::get_current_timestamp() {
            return Err(LicenseKeyError(
                jsonwebtoken::errors::ErrorKind::ExpiredSignature.into(),
            ));
        }

        Ok(license)
    }
}

/// A license key with the paid tier that only works in tests.
pub const TEST_PAID_LICENSE_KEY: &str =
 "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
  eyJzdWIiOiJydy10ZXN0IiwidGllciI6InBhaWQiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwiZXhwIjo5OTk5OTk5OTk5fQ.\
  c6Gmb6xh3dBDYX_4cOnHUbwRXJbUCM7W3mrJA77nLC5FkoOLpGstzvQ7qfnPVBu412MFtKRDvh-Lk8JwG7pVa0WLw16DeHTtVHxZukMTZ1Q_ciZ1xKeUx_pwUldkVzv6c9j99gNqPSyTjzOXTdKlidBRLer2zP0v3Lf-ZxnMG0tEcIbTinTb3BNCtAQ8bwBSRP-X48cVTWafjaZxv_zGiJT28uV3bR6jwrorjVB4VGvqhsJi6Fd074XOmUlnOleoAtyzKvjmGC5_FvnL0ztIe_I0z_pyCMfWpyJ_J4C7rCP1aVWUImyoowLmVDA-IKjclzOW5Fvi0wjXsc6OckOc_A";
