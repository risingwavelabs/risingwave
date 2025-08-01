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

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, FromQueryResult, Statement,
    TransactionTrait, Value,
};
use thiserror_ext::AsReport;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::time;

use crate::rpc::election::META_ELECTION_KEY;
use crate::{ElectionClient, ElectionMember, MetaResult};

pub struct SqlBackendElectionClient<T: SqlDriver> {
    id: String,
    driver: Arc<T>,
    is_leader_sender: watch::Sender<bool>,
}

impl<T: SqlDriver> SqlBackendElectionClient<T> {
    pub fn new(id: String, driver: Arc<T>) -> Self {
        let (sender, _) = watch::channel(false);
        Self {
            id,
            driver,
            is_leader_sender: sender,
        }
    }
}

#[derive(Debug, FromQueryResult)]
pub struct ElectionRow {
    service: String,
    id: String,
}

#[async_trait::async_trait]
pub trait SqlDriver: Send + Sync + 'static {
    async fn init_database(&self) -> MetaResult<()>;

    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()>;

    async fn try_campaign(&self, service_name: &str, id: &str, ttl: i64)
    -> MetaResult<ElectionRow>;
    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>>;

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>>;

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()>;

    async fn trim_candidates(&self, service_name: &str, timeout: i64) -> MetaResult<()>;
}

pub trait SqlDriverCommon {
    const ELECTION_LEADER_TABLE_NAME: &'static str = "election_leader";
    const ELECTION_MEMBER_TABLE_NAME: &'static str = "election_member";

    fn election_table_name() -> &'static str {
        Self::ELECTION_LEADER_TABLE_NAME
    }
    fn member_table_name() -> &'static str {
        Self::ELECTION_MEMBER_TABLE_NAME
    }
}

impl SqlDriverCommon for MySqlDriver {}

impl SqlDriverCommon for PostgresDriver {}

impl SqlDriverCommon for SqliteDriver {}

pub struct MySqlDriver {
    pub(crate) conn: DatabaseConnection,
}

impl MySqlDriver {
    pub fn new(conn: DatabaseConnection) -> Arc<Self> {
        Arc::new(Self { conn })
    }
}

pub struct PostgresDriver {
    pub(crate) conn: DatabaseConnection,
}

impl PostgresDriver {
    pub fn new(conn: DatabaseConnection) -> Arc<Self> {
        Arc::new(Self { conn })
    }
}

pub struct SqliteDriver {
    pub(crate) conn: DatabaseConnection,
}

impl SqliteDriver {
    pub fn new(conn: DatabaseConnection) -> Arc<Self> {
        Arc::new(Self { conn })
    }
}

#[async_trait::async_trait]
impl SqlDriver for SqliteDriver {
    async fn init_database(&self) -> MetaResult<()> {
        self.conn.execute(
            Statement::from_string(DatabaseBackend::Sqlite, format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (service VARCHAR(256), id VARCHAR(256), last_heartbeat DATETIME, PRIMARY KEY (service, id));"#,
                table = Self::member_table_name()
            ))).await?;

        self.conn.execute(
            Statement::from_string(DatabaseBackend::Sqlite, format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (service VARCHAR(256), id VARCHAR(256), last_heartbeat DATETIME, PRIMARY KEY (service));"#,
                table = Self::election_table_name()
            ))).await?;

        Ok(())
    }

    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!(
                    r#"INSERT INTO {table} (id, service, last_heartbeat)
VALUES($1, $2, CURRENT_TIMESTAMP)
ON CONFLICT (id, service)
DO
   UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat;
"#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(id), Value::from(service_name)],
            ))
            .await?;
        Ok(())
    }

    async fn try_campaign(
        &self,
        service_name: &str,
        id: &str,
        ttl: i64,
    ) -> MetaResult<ElectionRow> {
        let query_result = self.conn
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!(
                    r#"INSERT INTO {table} (service, id, last_heartbeat)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (service)
            DO UPDATE
            SET id             = CASE
                                     WHEN DATETIME({table}.last_heartbeat, '+' || $3 || ' second') < CURRENT_TIMESTAMP THEN EXCLUDED.id
                                     ELSE {table}.id
                END,
                last_heartbeat = CASE
                                     WHEN DATETIME({table}.last_heartbeat, '+' || $3 || ' seconds') < CURRENT_TIMESTAMP THEN EXCLUDED.last_heartbeat
                                     WHEN {table}.id = EXCLUDED.id THEN EXCLUDED.last_heartbeat
                                     ELSE {table}.last_heartbeat
                    END
        RETURNING service, id, last_heartbeat;
        "#,
                    table = Self::election_table_name()
                ),
                vec![Value::from(service_name), Value::from(id), Value::from(ttl)],
            ))
            .await?;

        let row = query_result
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .transpose()?;

        let row = row.ok_or_else(|| anyhow!("bad result from sqlite"))?;

        Ok(row)
    }

    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>> {
        let query_result = self
            .conn
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
                    table = Self::election_table_name()
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let row = query_result
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .transpose()?;

        Ok(row)
    }

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>> {
        let all = self
            .conn
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let rows = all
            .into_iter()
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .collect::<Result<_, sea_orm::DbErr>>()?;

        Ok(rows)
    }

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()> {
        let txn = self.conn.begin().await?;

        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!(
                r#"
            DELETE FROM {table} WHERE service = $1 AND id = $2;
            "#,
                table = Self::election_table_name()
            ),
            vec![Value::from(service_name), Value::from(id)],
        ))
        .await?;

        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!(
                r#"
            DELETE FROM {table} WHERE service = $1 AND id = $2;
            "#,
                table = Self::member_table_name()
            ),
            vec![Value::from(service_name), Value::from(id)],
        ))
        .await?;

        txn.commit().await?;

        Ok(())
    }

    async fn trim_candidates(&self, service_name: &str, timeout: i64) -> MetaResult<()> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!(
                    r#"
                    DELETE FROM {table} WHERE service = $1 AND DATETIME({table}.last_heartbeat, '+' || $2 || ' second') < CURRENT_TIMESTAMP;
                    "#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(service_name), Value::from(timeout)],
            ))
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl SqlDriver for MySqlDriver {
    async fn init_database(&self) -> MetaResult<()> {
        self.conn.execute(
            Statement::from_string(DatabaseBackend::MySql, format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (service VARCHAR(256), id VARCHAR(256), last_heartbeat DATETIME, PRIMARY KEY (service, id));"#,
                table = Self::member_table_name()
            ))).await?;

        self.conn.execute(
            Statement::from_string(DatabaseBackend::MySql, format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (service VARCHAR(256), id VARCHAR(256), last_heartbeat DATETIME, PRIMARY KEY (service));"#,
                table = Self::election_table_name()
            ))).await?;

        Ok(())
    }

    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                format!(
                    r#"INSERT INTO {table} (id, service, last_heartbeat)
        VALUES(?, ?, NOW())
        ON duplicate KEY
           UPDATE last_heartbeat = VALUES(last_heartbeat);
        "#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(id), Value::from(service_name)],
            ))
            .await?;

        Ok(())
    }

    async fn try_campaign(
        &self,
        service_name: &str,
        id: &str,
        ttl: i64,
    ) -> MetaResult<ElectionRow> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                format!(
                    r#"INSERT
            IGNORE
        INTO {table} (service, id, last_heartbeat)
        VALUES (?, ?, NOW())
        ON duplicate KEY
            UPDATE id             = if(last_heartbeat < NOW() - INTERVAL ? SECOND,
                                       VALUES(id), id),
                   last_heartbeat = if(id =
                                       VALUES(id),
                                       VALUES(last_heartbeat), last_heartbeat);"#,
                    table = Self::election_table_name()
                ),
                vec![Value::from(service_name), Value::from(id), Value::from(ttl)],
            ))
            .await?;

        let query_result = self
            .conn
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = ?;"#,
                    table = Self::election_table_name(),
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let row = query_result
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .transpose()?;

        let row = row.ok_or_else(|| anyhow!("bad result from mysql"))?;

        Ok(row)
    }

    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>> {
        let query_result = self
            .conn
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = ?;"#,
                    table = Self::election_table_name()
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let row = query_result
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .transpose()?;

        Ok(row)
    }

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>> {
        let all = self
            .conn
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = ?;"#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let rows = all
            .into_iter()
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .collect::<Result<_, sea_orm::DbErr>>()?;

        Ok(rows)
    }

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()> {
        let txn = self.conn.begin().await?;

        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::MySql,
            format!(
                r#"
            DELETE FROM {table} WHERE service = ? AND id = ?;
            "#,
                table = Self::election_table_name()
            ),
            vec![Value::from(service_name), Value::from(id)],
        ))
        .await?;

        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::MySql,
            format!(
                r#"
            DELETE FROM {table} WHERE service = ? AND id = ?;
            "#,
                table = Self::member_table_name()
            ),
            vec![Value::from(service_name), Value::from(id)],
        ))
        .await?;

        txn.commit().await?;

        Ok(())
    }

    async fn trim_candidates(&self, service_name: &str, timeout: i64) -> MetaResult<()> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                format!(
                    r#"
                    DELETE FROM {table} WHERE service = ? AND last_heartbeat < NOW() - INTERVAL ? SECOND;
                    "#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(service_name), Value::from(timeout)],
            ))
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl SqlDriver for PostgresDriver {
    async fn init_database(&self) -> MetaResult<()> {
        self.conn.execute(
            Statement::from_string(DatabaseBackend::Postgres, format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (service VARCHAR, id VARCHAR, last_heartbeat TIMESTAMPTZ, PRIMARY KEY (service, id));"#,
                table = Self::member_table_name()
            ))).await?;

        self.conn.execute(
            Statement::from_string(DatabaseBackend::Postgres, format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (service VARCHAR, id VARCHAR, last_heartbeat TIMESTAMPTZ, PRIMARY KEY (service));"#,
                table = Self::election_table_name()
            ))).await?;

        Ok(())
    }

    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                format!(
                    r#"INSERT INTO {table} (id, service, last_heartbeat)
        VALUES($1, $2, NOW())
        ON CONFLICT (id, service)
        DO
           UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat;
        "#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(id), Value::from(service_name)],
            ))
            .await?;

        Ok(())
    }

    async fn try_campaign(
        &self,
        service_name: &str,
        id: &str,
        ttl: i64,
    ) -> MetaResult<ElectionRow> {
        let query_result = self
            .conn
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                format!(
                    r#"INSERT INTO {table} (service, id, last_heartbeat)
        VALUES ($1, $2, NOW())
        ON CONFLICT (service)
            DO UPDATE
            SET id             = CASE
                                     WHEN {table}.last_heartbeat < NOW() - $3::INTERVAL THEN EXCLUDED.id
                                     ELSE {table}.id
                END,
                last_heartbeat = CASE
                                     WHEN {table}.last_heartbeat < NOW() - $3::INTERVAL THEN EXCLUDED.last_heartbeat
                                     WHEN {table}.id = EXCLUDED.id THEN EXCLUDED.last_heartbeat
                                     ELSE {table}.last_heartbeat
                    END
        RETURNING service, id, last_heartbeat;
        "#,
                    table = Self::election_table_name()
                ),
                vec![
                    Value::from(service_name),
                    Value::from(id),
                    // special handling for interval
                    Value::from(ttl.to_string()),
                ],
            ))
            .await?;

        let row = query_result
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .transpose()?;

        let row = row.ok_or_else(|| anyhow!("bad result from postgres"))?;

        Ok(row)
    }

    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>> {
        let query_result = self
            .conn
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
                    table = Self::election_table_name()
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let row = query_result
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .transpose()?;

        Ok(row)
    }

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>> {
        let all = self
            .conn
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                format!(
                    r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(service_name)],
            ))
            .await?;

        let rows = all
            .into_iter()
            .map(|query_result| ElectionRow::from_query_result(&query_result, ""))
            .collect::<Result<_, sea_orm::DbErr>>()?;

        Ok(rows)
    }

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()> {
        let txn = self.conn.begin().await?;

        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            format!(
                r#"
            DELETE FROM {table} WHERE service = $1 AND id = $2;
            "#,
                table = Self::election_table_name()
            ),
            vec![Value::from(service_name), Value::from(id)],
        ))
        .await?;

        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            format!(
                r#"
            DELETE FROM {table} WHERE service = $1 AND id = $2;
            "#,
                table = Self::member_table_name()
            ),
            vec![Value::from(service_name), Value::from(id)],
        ))
        .await?;

        txn.commit().await?;

        Ok(())
    }

    async fn trim_candidates(&self, service_name: &str, timeout: i64) -> MetaResult<()> {
        self.conn
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                format!(
                    r#"
                    DELETE FROM {table} WHERE {table}.service = $1 AND {table}.last_heartbeat < NOW() - $2::INTERVAL;
                    "#,
                    table = Self::member_table_name()
                ),
                vec![Value::from(service_name), Value::from(timeout.to_string())],
            ))
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> ElectionClient for SqlBackendElectionClient<T>
where
    T: SqlDriver + Send + Sync + 'static,
{
    async fn init(&self) -> MetaResult<()> {
        tracing::info!("initializing database for Sql backend election client");
        self.driver.init_database().await
    }

    fn id(&self) -> MetaResult<String> {
        Ok(self.id.clone())
    }

    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()> {
        let stop = stop.clone();

        let member_refresh_driver = self.driver.clone();

        let id = self.id.clone();

        let mut member_refresh_stop = stop.clone();

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = ticker.tick() => {

                        if let Err(e) = member_refresh_driver
                            .update_heartbeat(META_ELECTION_KEY, id.as_str())
                            .await {

                            tracing::debug!(error = %e.as_report(), "keep alive for member {} failed", id);
                            continue
                        }
                    }
                    _ = member_refresh_stop.changed() => {
                        return;
                    }
                }
            }
        });

        let _guard = scopeguard::guard(handle, |handle| handle.abort());

        self.is_leader_sender.send_replace(false);

        let mut timeout_ticker = time::interval(Duration::from_secs_f64(ttl as f64 / 2.0));
        timeout_ticker.reset();
        let mut stop = stop.clone();

        let mut is_leader = false;

        let mut election_ticker = time::interval(Duration::from_secs(1));

        let mut prev_leader = "".to_owned();

        loop {
            tokio::select! {
                    _ = election_ticker.tick() => {
                        let election_row = self
                            .driver
                            .try_campaign(META_ELECTION_KEY, self.id.as_str(), ttl)
                            .await?;

                        assert_eq!(election_row.service, META_ELECTION_KEY);

                        if election_row.id.eq_ignore_ascii_case(self.id.as_str()) {
                            if !is_leader{
                                self.is_leader_sender.send_replace(true);
                                is_leader = true;
                            } else {
                                self.is_leader_sender.send_replace(false);
                            }
                        } else if is_leader {
                            tracing::warn!("leader has been changed to {}", election_row.id);
                            break;
                        } else if prev_leader != election_row.id {
                            tracing::info!("leader is {}", election_row.id);
                            prev_leader.clone_from(&election_row.id)
                        }

                        timeout_ticker.reset();

                        if is_leader
                            && let Err(e) = self.driver.trim_candidates(META_ELECTION_KEY, ttl * 2).await {
                                tracing::warn!(error = %e.as_report(), "trim candidates failed");
                            }
                    }
                _ = timeout_ticker.tick() => {
                    tracing::error!("member {} election timeout", self.id);
                    break;
                }
                _ = stop.changed() => {
                    tracing::info!("stop signal received when observing");

                    if is_leader {
                        tracing::info!("leader {} resigning", self.id);
                        if let Err(e) = self.driver.resign(META_ELECTION_KEY, self.id.as_str()).await {
                            tracing::warn!(error = %e.as_report(), "resign failed");
                        }
                    }

                    return Ok(());
                }
            }
        }
        self.is_leader_sender.send_replace(false);

        return Ok(());
    }

    fn subscribe(&self) -> Receiver<bool> {
        self.is_leader_sender.subscribe()
    }

    async fn leader(&self) -> MetaResult<Option<ElectionMember>> {
        let row = self.driver.leader(META_ELECTION_KEY).await?;
        Ok(row.map(|row| ElectionMember {
            id: row.id,
            is_leader: true,
        }))
    }

    async fn get_members(&self) -> MetaResult<Vec<ElectionMember>> {
        let leader = self.leader().await?;
        let members = self.driver.candidates(META_ELECTION_KEY).await?;

        Ok(members
            .into_iter()
            .map(|row| {
                let is_leader = leader
                    .as_ref()
                    .map(|leader| leader.id.eq_ignore_ascii_case(row.id.as_str()))
                    .unwrap_or(false);

                ElectionMember {
                    id: row.id,
                    is_leader,
                }
            })
            .collect())
    }

    fn is_leader(&self) -> bool {
        *self.is_leader_sender.borrow()
    }
}

#[cfg(not(madsim))]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
    use tokio::sync::watch;

    use crate::rpc::election::sql::{SqlBackendElectionClient, SqlDriverCommon, SqliteDriver};
    use crate::{ElectionClient, MetaResult};

    async fn prepare_sqlite_env() -> MetaResult<DatabaseConnection> {
        let db: DatabaseConnection = Database::connect("sqlite::memory:").await?;

        db.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            format!("CREATE TABLE {table} (service VARCHAR(256) PRIMARY KEY, id VARCHAR(256), last_heartbeat DATETIME)",
                    table = SqliteDriver::election_table_name()),
            vec![],
        ))
            .await?;

        db.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            format!("CREATE TABLE {table} (service VARCHAR(256), id VARCHAR(256), last_heartbeat DATETIME, PRIMARY KEY (service, id))",
                    table = SqliteDriver::member_table_name()),
            vec![],
        ))
            .await?;

        Ok(db)
    }

    #[tokio::test]
    async fn test_sql_election() {
        let id = "test_id".to_owned();
        let conn = prepare_sqlite_env().await.unwrap();

        let provider = SqliteDriver { conn };
        let (sender, _) = watch::channel(false);
        let sql_election_client: Arc<dyn ElectionClient> = Arc::new(SqlBackendElectionClient {
            id,
            driver: Arc::new(provider),
            is_leader_sender: sender,
        });
        let (stop_sender, _) = watch::channel(());

        let stop_receiver = stop_sender.subscribe();

        let mut receiver = sql_election_client.subscribe();
        let client_ = sql_election_client.clone();
        tokio::spawn(async move { client_.run_once(10, stop_receiver).await.unwrap() });

        loop {
            receiver.changed().await.unwrap();
            if *receiver.borrow() {
                assert!(sql_election_client.is_leader());
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_sql_election_multi() {
        let (stop_sender, _) = watch::channel(());

        let mut clients = vec![];

        let conn = prepare_sqlite_env().await.unwrap();
        for i in 1..3 {
            let id = format!("test_id_{}", i);
            let provider = SqliteDriver { conn: conn.clone() };
            let (sender, _) = watch::channel(false);
            let sql_election_client: Arc<dyn ElectionClient> = Arc::new(SqlBackendElectionClient {
                id,
                driver: Arc::new(provider),
                is_leader_sender: sender,
            });

            let stop_receiver = stop_sender.subscribe();
            let client_ = sql_election_client.clone();
            tokio::spawn(async move { client_.run_once(10, stop_receiver).await.unwrap() });
            clients.push(sql_election_client);
        }

        let mut is_leaders = vec![];

        for client in clients {
            is_leaders.push(client.is_leader());
        }

        assert!(is_leaders.iter().filter(|&x| *x).count() <= 1);
    }
}
