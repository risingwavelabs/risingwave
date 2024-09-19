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

use std::num::NonZeroU64;

use thiserror::Error;

use crate::{LicenseKeyError, LicenseManager};

/// The error type for CPU core limit exceeded as per the license key.
#[derive(Debug, Clone, Error)]
#[error("invalid license key")]
pub enum CpuCoreLimitExceeded {
    #[error("cannot check CPU core limit due to license key error")]
    LicenseKeyError(#[from] LicenseKeyError),

    #[error(
        "CPU core limit exceeded as per the license key, \
        requesting {actual} while the maximum allowed is {limit}"
    )]
    Exceeded { limit: NonZeroU64, actual: u64 },
}

impl LicenseManager {
    /// Check if the given CPU core count exceeds the limit as per the license key.
    pub fn check_cpu_core_limit(&self, cpu_core_count: u64) -> Result<(), CpuCoreLimitExceeded> {
        let license = self.license()?;

        match license.cpu_core_limit {
            Some(limit) if cpu_core_count > limit.get() => Err(CpuCoreLimitExceeded::Exceeded {
                limit,
                actual: cpu_core_count,
            }),
            _ => Ok(()),
        }
    }
}

// Tests below only work in debug mode.
#[cfg(debug_assertions)]
#[cfg(test)]
mod tests {
    use expect_test::expect;
    use thiserror_ext::AsReport as _;

    use super::*;
    use crate::{LicenseKey, TEST_PAID_LICENSE_KEY_CONTENT};

    fn do_test(key: &str, cpu_core_count: u64, expect: expect_test::Expect) {
        let manager = LicenseManager::new();
        manager.refresh(LicenseKey(key));

        match manager.check_cpu_core_limit(cpu_core_count) {
            Ok(_) => expect.assert_eq("ok"),
            Err(error) => expect.assert_eq(&error.to_report_string()),
        }
    }

    #[test]
    fn test_no_limit() {
        do_test(TEST_PAID_LICENSE_KEY_CONTENT, 114514, expect!["ok"]);
    }

    #[test]
    fn test_no_license_key_no_limit() {
        do_test("", 114514, expect!["ok"]);
    }

    #[test]
    fn test_invalid_license_key() {
        const KEY: &str = "invalid";

        do_test(KEY, 0, expect!["cannot check CPU core limit due to license key error: invalid license key: InvalidToken"]);
        do_test(KEY, 114514, expect!["cannot check CPU core limit due to license key error: invalid license key: InvalidToken"]);
    }

    #[test]
    fn test_limit() {
        const KEY: &str =
         "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
          eyJzdWIiOiJmcmVlLXRlc3QtMzIiLCJpc3MiOiJwcm9kLnJpc2luZ3dhdmUuY29tIiwidGllciI6ImZyZWUiLCJleHAiOjE4NTI1NTk5OTksImlhdCI6MTcyMzcwMTk5NCwiY3B1X2NvcmVfbGltaXQiOjMyfQ.\
          rsATtzlduLUkGQeXkOROtyGUpafdDhi18iKdYAzAldWQuO9KevNcnD8a6geCShZSGte65bI7oYtv7GHx8i66ge3B1SVsgGgYr10ebphPUNUQenYoN0mpD4Wn0prPStOgANzYZOI2ntMGAaeWStji1x67_iho6r0W9r6RX3kMvzFSbiObSIfvTdrMULeg-xeHc3bT_ErRhaXq7MAa2Oiq3lcK2sNgEvc9KYSP9YbhSik9CBkc8lcyeVoc48SSWEaBU-c8-Ge0fzjgWHI9KIsUV5Ihe66KEfs0PqdRoSWbgskYGzA3o8wHIbtJbJiPzra373kkFH9MGY0HOsw9QeJLGQ";

        do_test(KEY, 31, expect!["ok"]);
        do_test(KEY, 32, expect!["ok"]);
        do_test(KEY, 33, expect!["CPU core limit exceeded as per the license key, requesting 33 while the maximum allowed is 32"]);
    }
}
