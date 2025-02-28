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

// Tests below only work in debug mode.
#[cfg(debug_assertions)]
#[cfg(test)]
mod tests {
    use expect_test::expect;
    use thiserror_ext::AsReport as _;

    use crate::{Feature, LicenseKey, LicenseManager, TEST_PAID_LICENSE_KEY_CONTENT};

    fn do_test(key: &str, cpu_core_count: usize, expect: expect_test::Expect) {
        let manager = LicenseManager::new();
        manager.refresh(LicenseKey(key));
        manager.update_cpu_core_count(cpu_core_count);

        match Feature::TestPaid.check_available_with(&manager) {
            Ok(_) => expect.assert_eq("ok"),
            Err(error) => expect.assert_eq(&error.to_report_string()),
        }
    }

    #[test]
    fn test_no_limit() {
        do_test(TEST_PAID_LICENSE_KEY_CONTENT, 114514, expect!["ok"]);
    }

    #[test]
    fn test_invalid_license_key() {
        const KEY: &str = "invalid";

        do_test(
            KEY,
            0,
            expect![
                "feature TestPaid is not available due to license error: invalid license key: InvalidToken"
            ],
        );
        do_test(
            KEY,
            114514,
            expect![
                "feature TestPaid is not available due to license error: invalid license key: InvalidToken"
            ],
        );
    }

    #[test]
    fn test_limit() {
        const KEY: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.\
          eyJzdWIiOiJwYWlkLXRlc3QtMzIiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwidGllciI6InBhaWQiLCJleHAiOjIxNTA0OTU5OTksImlhdCI6MTczNzYxMjQ5NSwiY3B1X2NvcmVfbGltaXQiOjMyfQ.\
          SQpX2Dmon5Mb04VUbHyxsU7owJhcdLZHqUefxAXBwG5AqgKdpfS0XUePW5E4D-EfxtH_cWJiD4QDFsfdRUz88g_n_KvfNUObMW7NV5TUoRs_ImtS4ySugExNX3JzJi71QqgI8kugStQ7uOR9kZ_C-cCc_IG2CwwEmhhW1Ij0vX7qjhG5JNMit_bhxPY7Rh27ppgPTqWxJFTTsw-9B7O5WR_yIlaDjxVzk0ALm_j6DPB249gG3dkeK0rP0AK_ip2cK6iQdy8Cge7ATD6yUh4c_aR6GILDF6-vyB7QdWU6DdQS4KhdkPNWoe_Z9psotcXQJ7NhQ39hk8tdLzmTfGDDBA";

        do_test(KEY, 31, expect!["ok"]);
        do_test(KEY, 32, expect!["ok"]);
        do_test(
            KEY,
            33,
            expect![
                "feature TestPaid is not available due to license error: the license key is currently not effective because the CPU core in the cluster (33) exceeds the maximum allowed by the license key (32); consider removing some nodes or acquiring a new license key with a higher limit"
            ],
        );
    }
}
