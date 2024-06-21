use std::sync::{LazyLock, RwLock};

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub enum Tier {
    Free,
    Paid,
}

impl Default for Tier {
    fn default() -> Self {
        if cfg!(debug_assertions) {
            Self::Paid
        } else {
            Self::Free
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct License {
    #[allow(dead_code)]
    sub: String,
    tier: Tier,
    exp: u64,
}

impl Default for License {
    fn default() -> Self {
        Self {
            sub: "default".to_owned(),
            tier: Tier::default(),
            exp: u64::MAX,
        }
    }
}

#[derive(Debug, Clone, Error)]
#[error("invalid license")]
pub struct LicenseError(#[source] jsonwebtoken::errors::Error);

pub type Result<T> = std::result::Result<T, LicenseError>;

struct Inner {
    last_token: String,
    license: Result<License>,
}

pub struct LicenseManager {
    inner: RwLock<Inner>,
}

impl LicenseManager {
    pub fn get() -> &'static Self {
        static INSTANCE: LazyLock<LicenseManager> = LazyLock::new(|| LicenseManager {
            inner: RwLock::new(Inner {
                last_token: String::new(),
                license: Ok(License::default()),
            }),
        });

        &INSTANCE
    }

    pub fn refresh(&self, token: &str) {
        let mut inner = self.inner.write().unwrap();
        inner.last_token = token.to_owned();

        if token.is_empty() {
            inner.license = Ok(License::default());
            return;
        }

        let validation = Validation::new(Algorithm::HS256);
        let decoding_key = DecodingKey::from_secret(b"my-very-private-secret");

        inner.license = match jsonwebtoken::decode(token, &decoding_key, &validation) {
            Ok(data) => Ok(data.claims),
            Err(error) => Err(LicenseError(error)),
        };
    }

    fn license(&self) -> Result<License> {
        let license = self.inner.read().unwrap().license.clone()?;

        if license.exp < jsonwebtoken::get_current_timestamp() {
            return Err(LicenseError(
                jsonwebtoken::errors::ErrorKind::ExpiredSignature.into(),
            ));
        }

        Ok(license)
    }
}

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
        #[derive(Clone, Copy, Debug)]
        pub enum Feature {
            $(
                #[doc = $doc]
                $name,
            )*
        }

        impl Feature {
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

#[derive(Debug, Error)]
pub enum FeatureNotAvailable {
    #[error(
        "feature {:?} is only available for tier {:?} and above, while the current tier is {:?}",
        feature, feature.min_tier(), current_tier,
    )]
    InsufficientTier {
        feature: Feature,
        current_tier: Tier,
    },

    #[error("feature {feature:?} is not available due to license error")]
    LicenseError {
        feature: Feature,
        source: LicenseError,
    },
}

impl Feature {
    pub fn check_available(self) -> std::result::Result<(), FeatureNotAvailable> {
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
                // If there's a license error, we still try against the default license first
                // and allow the feature if it's available.
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
