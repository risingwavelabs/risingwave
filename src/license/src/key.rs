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

use std::convert::Infallible;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A license key with the `All` tier that only works in tests.
///
/// The content is a JWT token with the following payload:
/// ```text
/// License {
///     sub: "rw-test-all",
///     iss: Test,
///     tier: All,
///     cpu_core_limit: None,
///     exp: 10000627200,
/// }
/// ```
pub(crate) const TEST_ALL_LICENSE_KEY_CONTENT: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
  eyJzdWIiOiJydy10ZXN0LWFsbCIsImlzcyI6InRlc3QucmlzaW5nd2F2ZS5jb20iLCJleHAiOjEwMDAwNjI3MjAwLCJpYXQiOjE3NTE4Njg5ODEsInRpZXIiOiJhbGwifQ.\
  la3qLwax0MtUZ_sYNPd0tCmWsfJUfUU_GUbt1RBFltAioPgF9fVWblknbrqw6TRS4KJuBY5GJc0K26ghCfwcSooduhrTy9rRmRMkQ7R9fSokQJ3nxU0DiaxK-1Ts2s5NTI7ZX_yEE4DlgUwVV1eKbJ8ihkcaNCExeZ9-BtNuJvJ7-IXm56L-TXTJR4TVsGirS3qHBoK7Nw8OKK8O8OyRAC9ul2SdWz905Ap-5f4hAiWW8fMOkxXpG1f8-UTU5AZo3Lt3YmxLvO1WXtnPro0EJnlI2ylJjgOg37SNbThCG7EQHlrBwP2vHbayH3LNpPWoSFLG2o0e5OQUmgZm8iMkXw";

/// A license key with the `All` tier and 4 core CPU limit that works in production.
///
/// This allows users to evaluate all features on a small scale. When the total CPU core in
/// the cluster exceeds the limit (4), features won't be available.
///
/// The content is a JWT token with the following payload:
/// ```text
/// License {
///     sub: "rw-default-all-4-core",
///     iss: Prod,
///     tier: All,
///     cpu_core_limit: 4,
///     exp: 10000627200,
/// }
/// ```
pub(crate) const PROD_ALL_4_CORE_LICENSE_KEY_CONTENT: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
  eyJzdWIiOiJydy1kZWZhdWx0LWFsbC00LWNvcmUiLCJpc3MiOiJwcm9kLnJpc2luZ3dhdmUuY29tIiwiZXhwIjoxMDAwMDYyNzIwMCwiaWF0IjoxNzUxODY5MTY1LCJ0aWVyIjoiYWxsIiwiY3B1X2NvcmVfbGltaXQiOjR9.\
  d4ilh2X4xnOpGP4mtIOAbkGTHRTxkqt7ZKhGpXoWxGbmYNy3RKTbHUhPZyYLZXRDW_X__LFZdxNWsh61d_A4pL_Fxq7mGdTESuWvSvyHrV34yt14MUF3ZLGyl6KaPg8X-oGc91uwJ__AO-npR0WHd8EziueZ_2lvieeTZiZWW73iRZF-DmV88wzcFuGwcZDlsc1Aajn18P9a79TEVVyeCXhj_UcLxjMIkF-3J_rO2a9aV3xMDyF1J6MHrNzRCL4fZqTeBhs4UXWI82-vwIHJaMZD2_jzgZxFkITzDaSJeoKP1cJroVq-EDUMx5MZd_80upFcdPppJCYl0UausnOJ1w";

/// A newtype wrapping `String` or `&str` for the license key.
///
/// - The default value is set to
///   * [`TEST_ALL_LICENSE_KEY_CONTENT`] in debug mode, to allow all features in tests.
///   * [`PROD_ALL_4_CORE_LICENSE_KEY_CONTENT`] in release mode, to allow evaluation of all features
///     on a small scale.
///
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
                TEST_ALL_LICENSE_KEY_CONTENT
            } else {
                PROD_ALL_4_CORE_LICENSE_KEY_CONTENT
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

impl<T: AsRef<str>> From<LicenseKey<T>> for String {
    fn from(val: LicenseKey<T>) -> Self {
        val.0.as_ref().to_owned()
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
