use std::sync::{LazyLock, RwLock};

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;

/// License tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Free,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
struct License {
    /// Subject of the license.
    /// See https://tools.ietf.org/html/rfc7519#section-4.1.2
    #[allow(dead_code)]
    sub: String,

    /// Tier of the license.
    tier: Tier,

    /// Expiration time in seconds since UNIX epoch.
    /// See https://tools.ietf.org/html/rfc7519#section-4.1.4
    exp: u64,
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

        // TODO: use asymmetric encryption
        let validation = Validation::new(Algorithm::HS256);
        let decoding_key = DecodingKey::from_secret(b"my-very-private-secret");

        inner.license = match jsonwebtoken::decode(license_key, &decoding_key, &validation) {
            Ok(data) => Ok(data.claims),
            Err(error) => Err(LicenseKeyError(error)),
        };
    }

    /// Get the current license if it is valid.
    ///
    /// Since the license can expire, the returned license should not be cached by the caller.
    fn license(&self) -> Result<License, LicenseKeyError> {
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

/// Define all features that are available based on the tier of the license.
macro_rules! for_all_features {
    ($macro:ident) => {
        $macro! {
            // name                 min tier    doc
            { MyCommonFeature,      Free,       "My common feature." },
            { MyAwesomeFeature,     Paid,       "My awesome feature." },
        }
    };
}

macro_rules! def_feature {
    ($({ $name:ident, $min_tier:ident, $doc:literal },)*) => {
        /// A set of features that are available based on the tier of the license.
        #[derive(Clone, Copy, Debug)]
        pub enum Feature {
            $(
                #[doc = concat!($doc, "\n\nAvailable for tier `", stringify!($min_tier), "` and above.")]
                $name,
            )*
        }

        impl Feature {
            /// Minimum tier required to use this feature.
            fn min_tier(self) -> Tier {
                match self {
                    $(
                        Self::$name => Tier::$min_tier,
                    )*
                }
            }
        }
    };
}

for_all_features!(def_feature);

/// The error type for feature not available due to license.
#[derive(Debug, Error)]
pub enum FeatureNotAvailable {
    #[error(
        "feature {:?} is only available for tier {:?} and above, while the current tier is {:?}\n\n \
        HINT: You may want to set a valid license key with `ALTER SYSTEM SET license_key = '...';` command.",
        feature, feature.min_tier(), current_tier,
    )]
    InsufficientTier {
        feature: Feature,
        current_tier: Tier,
    },

    #[error("feature {feature:?} is not available due to license error")]
    LicenseError {
        feature: Feature,
        source: LicenseKeyError,
    },
}

impl Feature {
    /// Check whether the feature is available based on the current license.
    pub fn check_available(self) -> Result<(), FeatureNotAvailable> {
        match LicenseManager::get().license() {
            Ok(license) => {
                if license.tier >= self.min_tier() {
                    Ok(())
                } else {
                    Err(FeatureNotAvailable::InsufficientTier {
                        feature: self,
                        current_tier: license.tier,
                    })
                }
            }
            Err(error) => {
                // If there's a license key error, we still try against the default license first
                // to see if the feature is available for free.
                if License::default().tier >= self.min_tier() {
                    Ok(())
                } else {
                    Err(FeatureNotAvailable::LicenseError {
                        feature: self,
                        source: error,
                    })
                }
            }
        }
    }
}
