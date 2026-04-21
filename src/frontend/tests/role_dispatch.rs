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

use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID,
};
use risingwave_frontend::test_utils::LocalFrontend;

#[tokio::test]
async fn test_create_role_defaults_to_nologin_and_alter_role_aliases_user_flow() {
    let frontend = LocalFrontend::new(Default::default()).await;
    let session = frontend.session_ref();
    let user_info_reader = session.env().user_info_reader();

    frontend.run_sql("CREATE ROLE analytics").await.unwrap();

    let analytics = user_info_reader
        .read_guard()
        .get_user_by_name("analytics")
        .cloned()
        .unwrap();
    assert!(
        !analytics.can_login,
        "CREATE ROLE should default to NOLOGIN"
    );
    assert!(!analytics.is_super);
    assert!(!analytics.can_create_db);
    assert!(!analytics.can_create_user);

    frontend
        .run_sql("ALTER ROLE analytics RENAME TO reporting")
        .await
        .unwrap();

    let guard = user_info_reader.read_guard();
    assert!(guard.get_user_by_name("analytics").is_none());
    let reporting = guard.get_user_by_name("reporting").cloned().unwrap();
    drop(guard);

    assert!(!reporting.can_login);
}

#[tokio::test]
async fn test_create_role_with_login_preserves_login_capability() {
    let frontend = LocalFrontend::new(Default::default()).await;
    let session = frontend.session_ref();
    let user_info_reader = session.env().user_info_reader();

    frontend
        .run_sql("CREATE ROLE login_role WITH LOGIN PASSWORD 'secret'")
        .await
        .unwrap();

    let login_role = user_info_reader
        .read_guard()
        .get_user_by_name("login_role")
        .cloned()
        .unwrap();
    assert!(login_role.can_login);
}

#[tokio::test]
async fn test_grant_revoke_role_dispatches() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE role_a").await.unwrap();
    frontend
        .run_sql("CREATE USER user_b WITH PASSWORD 'secret'")
        .await
        .unwrap();

    frontend
        .run_sql("GRANT role_a TO user_b WITH ADMIN OPTION")
        .await
        .unwrap();

    frontend
        .run_sql("REVOKE ADMIN OPTION FOR role_a FROM user_b")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_set_reset_role_dispatches_with_explicit_not_implemented_errors() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE analytics").await.unwrap();

    let set_role_err = frontend
        .run_sql("SET ROLE analytics")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        set_role_err.contains("SET ROLE is parsed and dispatched"),
        "unexpected error: {set_role_err}"
    );

    let reset_role_err = frontend
        .run_sql("RESET ROLE")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        reset_role_err.contains("RESET ROLE is parsed and dispatched"),
        "unexpected error: {reset_role_err}"
    );

    let session = frontend.session_user_ref(
        DEFAULT_DATABASE_NAME.to_owned(),
        DEFAULT_SUPER_USER.to_owned(),
        DEFAULT_SUPER_USER_ID,
    );
    let local_set_role_err = frontend
        .run_sql_with_session(session, "SET LOCAL ROLE analytics")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        local_set_role_err.contains("SET ROLE is parsed and dispatched"),
        "unexpected error: {local_set_role_err}"
    );
}
