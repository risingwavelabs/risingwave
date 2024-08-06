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

use std::convert::Infallible;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A license key with the paid tier that only works in tests.
pub(crate) const TEST_PAID_LICENSE_KEY_CONTENT: &str =
 "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
  eyJzdWIiOiJydy10ZXN0IiwidGllciI6InBhaWQiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwiZXhwIjo5OTk5OTk5OTk5fQ.\
  c6Gmb6xh3dBDYX_4cOnHUbwRXJbUCM7W3mrJA77nLC5FkoOLpGstzvQ7qfnPVBu412MFtKRDvh-Lk8JwG7pVa0WLw16DeHTtVHxZukMTZ1Q_ciZ1xKeUx_pwUldkVzv6c9j99gNqPSyTjzOXTdKlidBRLer2zP0v3Lf-ZxnMG0tEcIbTinTb3BNCtAQ8bwBSRP-X48cVTWafjaZxv_zGiJT28uV3bR6jwrorjVB4VGvqhsJi6Fd074XOmUlnOleoAtyzKvjmGC5_FvnL0ztIe_I0z_pyCMfWpyJ_J4C7rCP1aVWUImyoowLmVDA-IKjclzOW5Fvi0wjXsc6OckOc_A";

/// A newtype wrapping `String` or `&str` for the license key.
///
/// - The default value is set to [`TEST_PAID_LICENSE_KEY_CONTENT`] in debug mode, and empty otherwise.
/// - The content will be redacted when printed or serialized.
#[derive(Clone, Copy, Deserialize)]
#[serde(transparent)]
pub struct LicenseKey<T = String>(pub(crate) T);

/// Alias for [`LicenseKey<&str>`].
pub type LicenseKeyRef<'a> = LicenseKey<&'a str>;

impl<T: From<&'static str>> Default for LicenseKey<T> {
    fn default() -> Self {
        Self(
            if cfg!(debug_assertions) {
                TEST_PAID_LICENSE_KEY_CONTENT
            } else {
                ""
            }
            .into(),
        )
    }
}

impl<T> From<T> for LicenseKey<T> {
    fn from(t: T) -> Self {
        Self(t)
    }
}

impl<A, B> PartialEq<LicenseKey<B>> for LicenseKey<A>
where
    A: AsRef<str>,
    B: AsRef<str>,
{
    fn eq(&self, other: &LicenseKey<B>) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl<T: AsRef<str>> Eq for LicenseKey<T> {}

impl<T: AsRef<str>> LicenseKey<T> {
    fn redact_str(&self) -> &str {
        let s = self.0.as_ref();
        if s.is_empty() {
            ""
        } else if self == &LicenseKeyRef::default() {
            "<default>"
        } else {
            "<redacted>"
        }
    }
}

impl<T: AsRef<str>> std::fmt::Debug for LicenseKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.redact_str())
    }
}

impl<T: AsRef<str>> std::fmt::Display for LicenseKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl<T: AsRef<str>> Serialize for LicenseKey<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.redact_str().serialize(serializer)
    }
}

impl FromStr for LicenseKey {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}

impl<T: AsRef<str>> Into<String> for LicenseKey<T> {
    fn into(self) -> String {
        self.0.as_ref().to_owned()
    }
}

impl<'a> From<LicenseKeyRef<'a>> for LicenseKey {
    fn from(t: LicenseKeyRef<'a>) -> Self {
        Self(t.0.to_owned())
    }
}

impl LicenseKey {
    /// Create an empty license key, which means no license key is set.
    pub fn empty() -> Self {
        Self(String::new())
    }

    /// Convert to a reference.
    pub fn as_ref(&self) -> LicenseKeyRef<'_> {
        LicenseKey(self.0.as_ref())
    }
}
