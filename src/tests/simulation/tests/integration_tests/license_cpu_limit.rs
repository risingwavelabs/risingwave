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

use anyhow::Result;
use risingwave_common::error::AsReport;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;

/// Paid-tier key with CPU limit 20.
const KEY_20: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwYWlkLXRlc3QtMzIiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwidGllciI6InBhaWQiLCJleHAiOjIxNTA0OTU5OTksImlhdCI6MTczNzYxNTI2NywiY3B1X2NvcmVfbGltaXQiOjIwfQ.V8546BDZydv1aNk8IlVaSVlCriDtMC_75nq8CIaRPKlrltcwRJYKfK-Ru3WbKj-MDFebmW_3CqA4jR77BBtmmmtj-lPHa4qrdgrMItxm9RC_qoSU1YbI8Kb_ClYkrnFug5MAbK3wGlO8CrrjqOOt-Q5ggKChtl0uFj4zgI-S80d8Hse5LYSKHv8cU-ECKvEFe451kXE9x7nN_f8MqTSOqBwfY5o17gTD8oU3XH2k1mpesdci18kDmJPK5DeLPDYht_nRt7WGbVQvx7iiol1nzj5OBjdH_eVbX7pfk9M-JNwqZKaqfOmBbwV2F-Sf7-tK33O-XqSfXjnLAzflfjkoLQ";
/// Paid-tier key with CPU limit 100.
const KEY_100: &str = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwYWlkLXRlc3QtMzIiLCJpc3MiOiJ0ZXN0LnJpc2luZ3dhdmUuY29tIiwidGllciI6InBhaWQiLCJleHAiOjIxNTA0OTU5OTksImlhdCI6MTczNzYyMzc2MywiY3B1X2NvcmVfbGltaXQiOjEwMH0.ZGQjZa6t3va5MHMHKvgMgOXVLymEvvy1Yvd6teRUgCIF7en5BYaKXWuXwwtWLpLxr7LXyIQ3LQeDXag4k_fQOUTwV4oYTLTFVF8GcJ8JvGdTjBfjnM_2helLEhjZFgXSnhEy-xTOj5yM0BbqKCwanYlSODXQtp5owalt7a0JDwpId9_O8Pl24CjImZzPZLevJ_wSu4wv2IhVjK5QhkfBKBeaOxCeJaKfMVT5AzDQ-WwtJwahr1Dk0H2BxD6Hmp4KKBFRlVwGxq9-8uKBpbrmlClSuxPreBJ_xhP3SHtBFbVfcr38uaT_Bdh-gPRPgi-59tKOWPCY2FytO-Ls1U2l7w";

#[tokio::test]
async fn test_license_cpu_limit() -> Result<()> {
    // Now 8 * 3 = 24 cores in total.
    let mut cluster = Cluster::start(Configuration {
        compute_nodes: 3,
        compute_node_cores: 8,
        ..Default::default()
    })
    .await?;

    let mut session = cluster.start_session();

    macro_rules! set_license_key {
        ($key:expr) => {
            session
                .run(format!("ALTER SYSTEM SET license_key TO '{}';", $key))
                .await?;
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        };
    }

    macro_rules! test_paid_tier {
        () => {
            session.run("SELECT rw_test_paid_tier();").await
        };
    }

    set_license_key!("");
    let error = test_paid_tier!().unwrap_err().to_report_string();
    assert!(error.contains("feature TestPaid is not available based on your license"), "{error}");

    // Set a license key with CPU limit 100, it should work.
    set_license_key!(KEY_100);
    test_paid_tier!().unwrap().assert_result_eq("t");

    // Set a license key with CPU limit 20, which is lower than the current CPU cores.
    // Paid-tier features should not be available.
    set_license_key!(KEY_20);
    let error = test_paid_tier!().unwrap_err().to_report_string();
    assert!(
        error.contains("the license key is currently not effective"),
        "{error}"
    );

    // Kill a compute node, the total cores will be reduced to 16, which is under the limit.
    // The paid-tier features should be available again.
    cluster.simple_kill_nodes(["compute-2"]).await;
    tokio::time::sleep(std::time::Duration::from_secs(100)).await;
    test_paid_tier!().unwrap().assert_result_eq("t");

    // Add it back, will be unavailable again.
    cluster.simple_restart_nodes(["compute-2"]).await;
    tokio::time::sleep(std::time::Duration::from_secs(100)).await;
    let error = test_paid_tier!().unwrap_err().to_report_string();
    assert!(
        error.contains("the license key is currently not effective"),
        "{error}"
    );

    // Set a license key with CPU limit 100, it should work again.
    set_license_key!(KEY_100);
    test_paid_tier!().unwrap().assert_result_eq("t");

    Ok(())
}
