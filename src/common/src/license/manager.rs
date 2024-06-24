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

impl Default for Tier {
    /// The default tier is `Free` in production, and `Paid` in debug mode for testing.
    fn default() -> Self {
        if cfg!(debug_assertions) {
            Self::Paid
        } else {
            Self::Free
        }
    }
}

/// The content of a license.
///
/// We use JSON Web Token (JWT) to represent the license. This struct is the payload.
// TODO: Shall we add a version field?
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct License {
    /// Subject of the license.
    /// See https://tools.ietf.org/html/rfc7519#section-4.1.2
    #[allow(dead_code)]
    pub sub: String,

    /// Tier of the license.
    pub tier: Tier,

    /// Expiration time in seconds since UNIX epoch.
    /// See https://tools.ietf.org/html/rfc7519#section-4.1.4
    pub exp: u64,
}

impl Default for License {
    /// The default license is a free license in production, and a paid license in debug mode for
    /// testing. The default license never expires.
    ///
    /// Used when `license_key` is unset or invalid.
    fn default() -> Self {
        Self {
            sub: "default".to_owned(),
            tier: Tier::default(),
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
    DecodingKey::from_ed_pem(include_bytes!("public_key.pem"))
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

        // By default, `exp` is validated based on the current system time.
        let validation = Validation::new(Algorithm::RS256);

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
