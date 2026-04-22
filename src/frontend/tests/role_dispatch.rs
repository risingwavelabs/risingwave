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

use risingwave_common::catalog::DEFAULT_DATABASE_NAME;
use risingwave_frontend::test_utils::LocalFrontend;

#[tokio::test]
async fn test_create_role_defaults_to_nologin_and_alter_role_aliases_user_flow() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE analytics").await.unwrap();

    let analytics = frontend.user_by_name("analytics").unwrap();
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

    assert!(frontend.user_by_name("analytics").is_none());
    let reporting = frontend.user_by_name("reporting").unwrap();
    assert!(!reporting.can_login);
}

#[tokio::test]
async fn test_create_role_with_login_preserves_login_capability() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend
        .run_sql("CREATE ROLE login_role WITH LOGIN PASSWORD 'secret'")
        .await
        .unwrap();

    let login_role = frontend.user_by_name("login_role").unwrap();
    assert!(login_role.can_login);
}

#[tokio::test]
async fn test_create_and_alter_role_inherit_flags() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend
        .run_sql("CREATE ROLE inherited_role WITH NOINHERIT")
        .await
        .unwrap();
    let inherited_role = frontend.user_by_name("inherited_role").unwrap();
    assert!(!inherited_role.can_inherit);

    frontend
        .run_sql("ALTER ROLE inherited_role WITH INHERIT")
        .await
        .unwrap();
    let inherited_role = frontend.user_by_name("inherited_role").unwrap();
    assert!(inherited_role.can_inherit);
}

#[tokio::test]
async fn test_drop_role_aliases_drop_user_runtime() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE doomed_role").await.unwrap();
    assert!(frontend.user_by_name("doomed_role").is_some());

    frontend.run_sql("DROP ROLE doomed_role").await.unwrap();
    assert!(frontend.user_by_name("doomed_role").is_none());
}

#[tokio::test]
async fn test_drop_role_auto_revokes_memberships() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE parent_role").await.unwrap();
    frontend.run_sql("CREATE ROLE member_role").await.unwrap();
    frontend
        .run_sql("GRANT parent_role TO member_role")
        .await
        .unwrap();

    assert_eq!(frontend.role_memberships().await.len(), 1);

    frontend.run_sql("DROP ROLE parent_role").await.unwrap();

    assert!(frontend.user_by_name("parent_role").is_none());
    assert!(frontend.role_memberships().await.is_empty());
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

    let role_a = frontend.user_by_name("role_a").unwrap();
    let user_b = frontend.user_by_name("user_b").unwrap();
    let memberships = frontend.role_memberships().await;
    assert_eq!(memberships.len(), 1);
    assert_eq!(memberships[0].role_id, role_a.id.as_raw_id());
    assert_eq!(memberships[0].member_id, user_b.id.as_raw_id());
    assert!(memberships[0].admin_option);

    frontend
        .run_sql("REVOKE ADMIN OPTION FOR role_a FROM user_b")
        .await
        .unwrap();

    let memberships = frontend.role_memberships().await;
    assert_eq!(memberships.len(), 1);
    assert!(!memberships[0].admin_option);

    frontend.run_sql("REVOKE role_a FROM user_b").await.unwrap();

    let memberships = frontend.role_memberships().await;
    assert!(memberships.is_empty());
}

#[tokio::test]
async fn test_grant_revoke_role_membership_options_dispatch() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE role_a").await.unwrap();
    frontend
        .run_sql("CREATE USER user_b WITH PASSWORD 'secret'")
        .await
        .unwrap();

    frontend
        .run_sql("GRANT role_a TO user_b WITH INHERIT FALSE, SET FALSE")
        .await
        .unwrap();

    let memberships = frontend.role_memberships().await;
    assert_eq!(memberships.len(), 1);
    assert!(!memberships[0].admin_option);
    assert!(!memberships[0].inherit_option);
    assert!(!memberships[0].set_option);

    let user_id = frontend.user_by_name("user_b").unwrap().id;
    let session = frontend.session_user_ref(
        DEFAULT_DATABASE_NAME.to_owned(),
        "user_b".to_owned(),
        user_id,
    );
    let set_role_err = frontend
        .run_sql_with_session(session.clone(), "SET ROLE role_a")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        set_role_err.contains("permission denied to set role"),
        "unexpected error: {set_role_err}"
    );

    frontend
        .run_sql("REVOKE INHERIT OPTION FOR role_a FROM user_b")
        .await
        .unwrap();
    let memberships = frontend.role_memberships().await;
    assert_eq!(memberships.len(), 1);
    assert!(!memberships[0].inherit_option);

    frontend.run_sql("REVOKE role_a FROM user_b").await.unwrap();
    assert!(frontend.role_memberships().await.is_empty());
}

#[tokio::test]
async fn test_grant_revoke_role_granted_by_current_user_only() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE role_a").await.unwrap();
    frontend
        .run_sql("CREATE USER user_b WITH PASSWORD 'secret'")
        .await
        .unwrap();

    frontend
        .run_sql("GRANT role_a TO user_b GRANTED BY root")
        .await
        .unwrap();
    assert_eq!(frontend.role_memberships().await.len(), 1);

    let err = frontend
        .run_sql("GRANT role_a TO user_b GRANTED BY postgres")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("GRANTED BY must specify the current user"),
        "unexpected error: {err}"
    );

    frontend
        .run_sql("REVOKE role_a FROM user_b GRANTED BY root")
        .await
        .unwrap();
    assert!(frontend.role_memberships().await.is_empty());
}

#[tokio::test]
async fn test_granted_by_current_role_works_but_session_user_fails_after_set_role() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE parent_role").await.unwrap();
    frontend.run_sql("CREATE ROLE grantor_role").await.unwrap();
    frontend.run_sql("CREATE ROLE grantee_role").await.unwrap();
    frontend
        .run_sql("GRANT parent_role TO grantor_role WITH ADMIN OPTION")
        .await
        .unwrap();

    let root_session = frontend.session_ref();
    frontend
        .run_sql_with_session(root_session.clone(), "SET ROLE grantor_role")
        .await
        .unwrap();

    frontend
        .run_sql_with_session(
            root_session.clone(),
            "GRANT parent_role TO grantee_role GRANTED BY current_role",
        )
        .await
        .unwrap();

    let memberships = frontend.role_memberships().await;
    assert_eq!(memberships.len(), 2);
    let membership = memberships
        .iter()
        .find(|membership| {
            membership.member_id
                == frontend
                    .user_by_name("grantee_role")
                    .unwrap()
                    .id
                    .as_raw_id()
        })
        .unwrap();
    assert_eq!(
        membership.granted_by,
        frontend
            .user_by_name("grantor_role")
            .unwrap()
            .id
            .as_raw_id()
    );

    let err = frontend
        .run_sql_with_session(
            root_session.clone(),
            "REVOKE parent_role FROM grantee_role GRANTED BY session_user",
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("GRANTED BY must specify the current user"),
        "unexpected error: {err}"
    );

    frontend
        .run_sql_with_session(
            root_session.clone(),
            "REVOKE parent_role FROM grantee_role GRANTED BY current_role",
        )
        .await
        .unwrap();

    frontend
        .run_sql_with_session(root_session, "RESET ROLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_set_reset_role_dispatches_and_changes_identity() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE analytics").await.unwrap();
    frontend
        .run_sql("CREATE USER app_user WITH PASSWORD 'secret'")
        .await
        .unwrap();
    frontend
        .run_sql("GRANT analytics TO app_user")
        .await
        .unwrap();

    let user_id = frontend.user_by_name("app_user").unwrap().id;
    let session = frontend.session_user_ref(
        DEFAULT_DATABASE_NAME.to_owned(),
        "app_user".to_owned(),
        user_id,
    );

    let before = session.auth_context();
    assert_eq!(before.session_user_name(), "app_user");
    assert_eq!(before.current_user_name(), "app_user");

    frontend
        .run_sql_with_session(session.clone(), "SET ROLE analytics")
        .await
        .unwrap();

    let after_set = session.auth_context();
    assert_eq!(after_set.session_user_name(), "app_user");
    assert_eq!(after_set.current_user_name(), "analytics");

    frontend
        .run_sql_with_session(session.clone(), "RESET ROLE")
        .await
        .unwrap();

    let after_reset = session.auth_context();
    assert_eq!(after_reset.session_user_name(), "app_user");
    assert_eq!(after_reset.current_user_name(), "app_user");

    frontend
        .run_sql_with_session(session.clone(), "SET ROLE NONE")
        .await
        .unwrap();

    let after_none = session.auth_context();
    assert_eq!(after_none.session_user_name(), "app_user");
    assert_eq!(after_none.current_user_name(), "app_user");

    frontend
        .run_sql_with_session(session.clone(), "START TRANSACTION READ ONLY")
        .await
        .unwrap();
    frontend
        .run_sql_with_session(session.clone(), "SET LOCAL ROLE analytics")
        .await
        .unwrap();
    let after_local = session.auth_context();
    assert_eq!(after_local.session_user_name(), "app_user");
    assert_eq!(after_local.current_user_name(), "analytics");
    frontend
        .run_sql_with_session(session.clone(), "COMMIT")
        .await
        .unwrap();
    let after_local_commit = session.auth_context();
    assert_eq!(after_local_commit.session_user_name(), "app_user");
    assert_eq!(after_local_commit.current_user_name(), "app_user");
}

#[tokio::test]
async fn test_set_role_requires_membership() {
    let frontend = LocalFrontend::new(Default::default()).await;

    frontend.run_sql("CREATE ROLE analytics").await.unwrap();
    frontend
        .run_sql("CREATE USER app_user_2 WITH PASSWORD 'secret'")
        .await
        .unwrap();

    let user_id = frontend.user_by_name("app_user_2").unwrap().id;
    let session = frontend.session_user_ref(
        DEFAULT_DATABASE_NAME.to_owned(),
        "app_user_2".to_owned(),
        user_id,
    );
    let set_role_err = frontend
        .run_sql_with_session(session, "SET ROLE analytics")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        set_role_err.contains("permission denied to set role"),
        "unexpected error: {set_role_err}"
    );
}
