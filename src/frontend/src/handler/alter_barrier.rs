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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DatabaseParam;
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::DatabaseId;
use crate::error::{ErrorCode, Result};
use crate::session::SessionImpl;

pub async fn handle_set_barrier_interval(
    handler_args: HandlerArgs,
    database_name: ObjectName,
    barrier_interval_ms: Option<u32>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let database_name = Binder::resolve_database_name(database_name)?;
    let database_id = check_privileges_for_alter_database(&session, &database_name)?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_barrier(
            database_id,
            DatabaseParam::BarrierIntervalMs(barrier_interval_ms),
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_DATABASE))
}

pub async fn handle_set_checkpoint_frequency(
    handler_args: HandlerArgs,
    database_name: ObjectName,
    checkpoint_frequency: Option<u64>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let database_name = Binder::resolve_database_name(database_name)?;
    let database_id = check_privileges_for_alter_database(&session, &database_name)?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_barrier(
            database_id,
            DatabaseParam::CheckpointFrequency(checkpoint_frequency),
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_DATABASE))
}

fn check_privileges_for_alter_database(session: &SessionImpl, db_name: &str) -> Result<DatabaseId> {
    let user_reader = session.env().user_info_reader().read_guard();
    let catalog_reader = session.env().catalog_reader().read_guard();
    let database = catalog_reader.get_database_by_name(db_name)?;

    // The user should be super user or owner to alter the database.
    session.check_privilege_for_drop_alter_db_schema(database)?;

    // Non-superuser owners must also have the CREATEDB privilege.
    if let Some(user) = user_reader.get_user_by_name(&session.user_name()) {
        if !user.is_super && !user.can_create_db {
            return Err(ErrorCode::PermissionDenied(
                "Non-superuser owners must also have the CREATEDB privilege".to_owned(),
            )
            .into());
        }
    } else {
        return Err(ErrorCode::PermissionDenied("Session user is invalid".to_owned()).into());
    }

    Ok(database.id())
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_alter_barrier() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        frontend.run_sql("CREATE DATABASE test_db").await.unwrap();
        {
            let reader = catalog_reader.read_guard();
            let db = reader.get_database_by_name("test_db").unwrap();
            assert!(db.barrier_interval_ms.is_none());
            assert!(db.checkpoint_frequency.is_none());
        }

        frontend
            .run_sql("ALTER DATABASE test_db SET BARRIER_INTERVAL_MS = 1000")
            .await
            .unwrap();
        {
            let reader = catalog_reader.read_guard();
            let db = reader.get_database_by_name("test_db").unwrap();
            assert_eq!(db.barrier_interval_ms, Some(1000));
            assert!(db.checkpoint_frequency.is_none());
        }

        frontend
            .run_sql("ALTER DATABASE test_db SET CHECKPOINT_FREQUENCY = 10")
            .await
            .unwrap();
        {
            let reader = catalog_reader.read_guard();
            let db = reader.get_database_by_name("test_db").unwrap();
            assert_eq!(db.barrier_interval_ms, Some(1000));
            assert_eq!(db.checkpoint_frequency, Some(10));
        }

        frontend
            .run_sql("ALTER DATABASE test_db SET BARRIER_INTERVAL_MS = DEFAULT")
            .await
            .unwrap();
        {
            let reader = catalog_reader.read_guard();
            let db = reader.get_database_by_name("test_db").unwrap();
            assert!(db.barrier_interval_ms.is_none());
            assert_eq!(db.checkpoint_frequency, Some(10));
        }

        frontend
            .run_sql("ALTER DATABASE test_db SET CHECKPOINT_FREQUENCY = DEFAULT")
            .await
            .unwrap();
        {
            let reader = catalog_reader.read_guard();
            let db = reader.get_database_by_name("test_db").unwrap();
            assert!(db.barrier_interval_ms.is_none());
            assert!(db.checkpoint_frequency.is_none());
        }
    }
}
