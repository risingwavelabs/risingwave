// Copyright 2023 RisingWave Labs
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

use sqlx::{MySql, MySqlPool, PgPool, Postgres, Sqlite, SqlitePool};
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

#[derive(sqlx::FromRow, Debug)]
pub(crate) struct ElectionRow {
    service: String,
    id: String,
}

#[async_trait::async_trait]
pub(crate) trait SqlDriver: Send + Sync + 'static {
    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()>;

    async fn try_campaign(&self, service_name: &str, id: &str, ttl: i64)
        -> MetaResult<ElectionRow>;
    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>>;

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>>;

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()>;
}

pub(crate) trait SqlDriverCommon {
    const ELECTION_LEADER_TABLE_NAME: &'static str = "election_leader";
    const ELECTION_MEMBER_TABLE_NAME: &'static str = "election_members";

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
    pool: MySqlPool,
}

pub struct PostgresDriver {
    pool: PgPool,
}

pub struct SqliteDriver {
    pool: SqlitePool,
}

#[async_trait::async_trait]
impl SqlDriver for SqliteDriver {
    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()> {
        sqlx::query(&format!(
            r#"INSERT INTO {table} (id, service, last_heartbeat)
VALUES($1, $2, CURRENT_TIMESTAMP)
ON CONFLICT (id, service)
DO
   UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat;
"#,
            table = Self::member_table_name()
        ))
        .bind(id)
        .bind(service_name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn try_campaign(
        &self,
        service_name: &str,
        id: &str,
        ttl: i64,
    ) -> MetaResult<ElectionRow> {
        let row = sqlx::query_as::<Sqlite, ElectionRow>(&format!(
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
        ))
            .bind(service_name)
            .bind(id)
            .bind(ttl)
            .fetch_one(&self.pool)
            .await?;

        Ok(row)
    }

    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>> {
        let row = sqlx::query_as::<_, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
            table = Self::election_table_name()
        ))
        .bind(service_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>> {
        let row = sqlx::query_as::<_, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
            table = Self::member_table_name()
        ))
        .bind(service_name)
        .fetch_all(&self.pool)
        .await?;

        Ok(row)
    }

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()> {
        let mut txn = self.pool.begin().await?;
        sqlx::query(&format!(
            r#"
        DELETE FROM {table} WHERE service = $1 AND id = $2;
        "#,
            table = Self::election_table_name()
        ))
        .bind(service_name)
        .bind(id)
        .execute(&mut *txn)
        .await?;

        sqlx::query(&format!(
            r#"
        DELETE FROM {table} WHERE service = $1 AND id = $2;
        "#,
            table = Self::member_table_name()
        ))
        .bind(service_name)
        .bind(id)
        .execute(&mut *txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl SqlDriver for MySqlDriver {
    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()> {
        sqlx::query(&format!(
            r#"INSERT INTO {table} (id, service, last_heartbeat)
VALUES(?, ?, NOW())
ON duplicate KEY
   UPDATE last_heartbeat = VALUES(last_heartbeat);
"#,
            table = Self::member_table_name()
        ))
        .bind(id)
        .bind(service_name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn try_campaign(
        &self,
        service_name: &str,
        id: &str,
        ttl: i64,
    ) -> MetaResult<ElectionRow> {
        let _ = sqlx::query::<MySql>(&format!(
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
        ))
        .bind(service_name)
        .bind(id)
        .bind(ttl)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query_as::<MySql, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = ?;"#,
            table = Self::election_table_name(),
        ))
        .bind(service_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>> {
        let row = sqlx::query_as::<MySql, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = ?;"#,
            table = Self::election_table_name()
        ))
        .bind(service_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>> {
        let row = sqlx::query_as::<MySql, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = ?;"#,
            table = Self::member_table_name()
        ))
        .bind(service_name)
        .fetch_all(&self.pool)
        .await?;

        Ok(row)
    }

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()> {
        let mut txn = self.pool.begin().await?;
        sqlx::query(&format!(
            r#"
        DELETE FROM {table} WHERE service = ? AND id = ?;
        "#,
            table = Self::election_table_name()
        ))
        .bind(service_name)
        .bind(id)
        .execute(&mut *txn)
        .await?;

        sqlx::query(&format!(
            r#"
        DELETE FROM {table} WHERE service = ? AND id = ?;
        "#,
            table = Self::member_table_name()
        ))
        .bind(service_name)
        .bind(id)
        .execute(&mut *txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl SqlDriver for PostgresDriver {
    async fn update_heartbeat(&self, service_name: &str, id: &str) -> MetaResult<()> {
        sqlx::query(&format!(
            r#"INSERT INTO {table} (id, service, last_heartbeat)
VALUES($1, $2, NOW())
ON CONFLICT (id, service)
DO
   UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat;
"#,
            table = Self::member_table_name()
        ))
        .bind(id)
        .bind(service_name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn try_campaign(
        &self,
        service_name: &str,
        id: &str,
        ttl: i64,
    ) -> MetaResult<ElectionRow> {
        let row = sqlx::query_as::<Postgres, ElectionRow>(&format!(
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
        ))
            .bind(service_name)
            .bind(id)
            .bind(Duration::from_secs(ttl as u64))
            .fetch_one(&self.pool)
            .await?;

        Ok(row)
    }

    async fn leader(&self, service_name: &str) -> MetaResult<Option<ElectionRow>> {
        let row = sqlx::query_as::<Postgres, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
            table = Self::election_table_name()
        ))
        .bind(service_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    async fn candidates(&self, service_name: &str) -> MetaResult<Vec<ElectionRow>> {
        let row = sqlx::query_as::<Postgres, ElectionRow>(&format!(
            r#"SELECT service, id, last_heartbeat FROM {table} WHERE service = $1;"#,
            table = Self::member_table_name()
        ))
        .bind(service_name)
        .fetch_all(&self.pool)
        .await?;

        Ok(row)
    }

    async fn resign(&self, service_name: &str, id: &str) -> MetaResult<()> {
        let mut txn = self.pool.begin().await?;
        sqlx::query(&format!(
            r#"
        DELETE FROM {table} WHERE service = $1 AND id = $2;
        "#,
            table = Self::election_table_name()
        ))
        .bind(service_name)
        .bind(id)
        .execute(&mut *txn)
        .await?;

        sqlx::query(&format!(
            r#"
        DELETE FROM {table} WHERE service = $1 AND id = $2;
        "#,
            table = Self::member_table_name()
        ))
        .bind(service_name)
        .bind(id)
        .execute(&mut *txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> ElectionClient for SqlBackendElectionClient<T>
where
    T: SqlDriver + Send + Sync + 'static,
{
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

                            tracing::debug!("keep alive for member {} failed {}", id, e);
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
                            }
                        } else if is_leader {
                            tracing::warn!("leader has been changed to {}", election_row.id);
                            break;
                        }

                    timeout_ticker.reset();
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
                            tracing::warn!("resign failed {}", e);
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

    async fn is_leader(&self) -> bool {
        *self.is_leader_sender.borrow()
    }
}

#[cfg(not(madsim))]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::SqlitePool;
    use tokio::sync::watch;

    use crate::rpc::election::sql::{SqlBackendElectionClient, SqlDriverCommon, SqliteDriver};
    use crate::{ElectionClient, MetaResult};

    async fn prepare_sqlite_env() -> MetaResult<SqlitePool> {
        let pool = SqlitePoolOptions::new().connect("sqlite::memory:").await?;
        let _ = sqlx::query(
            &format!("CREATE TABLE {table} (service VARCHAR(256) PRIMARY KEY, id VARCHAR(256), last_heartbeat DATETIME)",
                     table = SqliteDriver::election_table_name()))
            .execute(&pool).await?;

        let _ = sqlx::query(
            &format!("CREATE TABLE {table} (service VARCHAR(256), id VARCHAR(256), last_heartbeat DATETIME, PRIMARY KEY (service, id))",
                     table = SqliteDriver::member_table_name()))
            .execute(&pool).await?;

        Ok(pool)
    }

    #[tokio::test]
    async fn test_sql_election() {
        let id = "test_id".to_string();
        let pool = prepare_sqlite_env().await.unwrap();

        let provider = SqliteDriver { pool };
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
                assert!(sql_election_client.is_leader().await);
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_sql_election_multi() {
        let (stop_sender, _) = watch::channel(());

        let mut clients = vec![];

        let pool = prepare_sqlite_env().await.unwrap();
        for i in 1..3 {
            let id = format!("test_id_{}", i);
            let provider = SqliteDriver { pool: pool.clone() };
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
            is_leaders.push(client.is_leader().await);
        }

        assert!(is_leaders.iter().filter(|&x| *x).count() <= 1);
    }
}
