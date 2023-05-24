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

use anyhow::anyhow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pg_interval::Interval;
use rust_decimal::Decimal;
use tokio::select;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

pub struct TestSuite {
    config: String,
}

macro_rules! test_eq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    return Err(anyhow!(
                        "{}:{} assertion failed: `(left == right)` \
                                (left: `{:?}`, right: `{:?}`)",
                        file!(),
                        line!(),
                        left_val,
                        right_val
                    ));
                }
            }
        }
    };
}

impl TestSuite {
    pub fn new(
        db_name: String,
        user_name: String,
        server_host: String,
        server_port: u16,
        password: String,
    ) -> Self {
        let config = if !password.is_empty() {
            format!(
                "dbname={} user={} host={} port={} password={}",
                db_name, user_name, server_host, server_port, password
            )
        } else {
            format!(
                "dbname={} user={} host={} port={}",
                db_name, user_name, server_host, server_port
            )
        };
        Self { config }
    }

    pub async fn test(&self) -> anyhow::Result<()> {
        self.binary_param_and_result().await?;
        self.dql_dml_with_param().await?;
        self.max_row().await?;
        self.multiple_on_going_portal().await?;
        self.create_with_parameter().await?;
        self.simple_cancel(false).await?;
        self.simple_cancel(true).await?;
        self.complex_cancel(false).await?;
        self.complex_cancel(true).await?;
        Ok(())
    }

    async fn create_client(&self, is_distributed: bool) -> anyhow::Result<Client> {
        let (client, connection) = tokio_postgres::connect(&self.config, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        if is_distributed {
            client.execute("set query_mode = distributed", &[]).await?;
        } else {
            client.execute("set query_mode = local", &[]).await?;
        }

        Ok(client)
    }

    pub async fn binary_param_and_result(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        for row in client.query("select $1::SMALLINT;", &[&1024_i16]).await? {
            let data: i16 = row.try_get(0)?;
            test_eq!(data, 1024);
        }

        for row in client.query("select $1::INT;", &[&144232_i32]).await? {
            let data: i32 = row.try_get(0)?;
            test_eq!(data, 144232);
        }

        for row in client.query("select $1::BIGINT;", &[&99999999_i64]).await? {
            let data: i64 = row.try_get(0)?;
            test_eq!(data, 99999999);
        }

        for row in client
            .query(
                "select $1::DECIMAL;",
                &[&Decimal::try_from(2.33454_f32).ok()],
            )
            .await?
        {
            let data: Decimal = row.try_get(0)?;
            test_eq!(data, Decimal::try_from(2.33454_f32).unwrap());
        }

        for row in client.query("select $1::BOOL;", &[&true]).await? {
            let data: bool = row.try_get(0)?;
            assert!(data);
        }

        for row in client.query("select $1::REAL;", &[&1.234234_f32]).await? {
            let data: f32 = row.try_get(0)?;
            test_eq!(data, 1.234234);
        }

        for row in client
            .query("select $1::DOUBLE PRECISION;", &[&234234.23490238483_f64])
            .await?
        {
            let data: f64 = row.try_get(0)?;
            test_eq!(data, 234234.23490238483);
        }

        for row in client
            .query(
                "select $1::date;",
                &[&NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()],
            )
            .await?
        {
            let data: NaiveDate = row.try_get(0)?;
            test_eq!(data, NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
        }

        for row in client
            .query(
                "select $1::time",
                &[&NaiveTime::from_hms_opt(10, 0, 0).unwrap()],
            )
            .await?
        {
            let data: NaiveTime = row.try_get(0)?;
            test_eq!(data, NaiveTime::from_hms_opt(10, 0, 0).unwrap());
        }

        for row in client
            .query(
                "select $1::timestamp",
                &[&NaiveDate::from_ymd_opt(2022, 1, 1)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap()],
            )
            .await?
        {
            let data: NaiveDateTime = row.try_get(0)?;
            test_eq!(
                data,
                NaiveDate::from_ymd_opt(2022, 1, 1)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap()
            );
        }

        let timestamptz = DateTime::<Utc>::from_utc(
            NaiveDate::from_ymd_opt(2022, 1, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        for row in client
            .query("select $1::timestamptz", &[&timestamptz])
            .await?
        {
            let data: DateTime<Utc> = row.try_get(0)?;
            test_eq!(data, timestamptz);
        }

        for row in client
            .query("select $1::interval", &[&Interval::new(1, 1, 24000000)])
            .await?
        {
            let data: Interval = row.try_get(0)?;
            test_eq!(data, Interval::new(1, 1, 24000000));
        }

        Ok(())
    }

    async fn dql_dml_with_param(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        client.query("create table t(id int)", &[]).await?;

        let insert_statement = client
            .prepare_typed("insert INTO t (id) VALUES ($1)", &[])
            .await?;

        for i in 0..20 {
            client.execute(&insert_statement, &[&i]).await?;
        }
        client.execute("flush", &[]).await?;

        let update_statement = client
            .prepare_typed(
                "update t set id = $1 where id < $2",
                &[Type::INT4, Type::INT4],
            )
            .await?;
        let query_statement = client
            .prepare_typed(
                "select * FROM t where id < $1 order by id ASC",
                &[Type::INT4],
            )
            .await?;
        let delete_statement = client
            .prepare_typed("delete FROM t where id < $1", &[Type::INT4])
            .await?;

        let mut i = 0;
        for row in client.query(&query_statement, &[&10_i32]).await? {
            let id: i32 = row.try_get(0)?;
            test_eq!(id, i);
            i += 1;
        }
        test_eq!(i, 10);

        client
            .execute(&update_statement, &[&100_i32, &10_i32])
            .await?;
        client.execute("flush", &[]).await?;

        let mut i = 0;
        for _ in client.query(&query_statement, &[&10_i32]).await? {
            i += 1;
        }
        test_eq!(i, 0);

        client.execute(&delete_statement, &[&20_i32]).await?;
        client.execute("flush", &[]).await?;

        let mut i = 0;
        for row in client.query(&query_statement, &[&101_i32]).await? {
            let id: i32 = row.try_get(0)?;
            test_eq!(id, 100);
            i += 1;
        }
        test_eq!(i, 10);

        client.execute("drop table t", &[]).await?;

        Ok(())
    }

    async fn max_row(&self) -> anyhow::Result<()> {
        let mut client = self.create_client(false).await?;

        client.query("create table t(id int)", &[]).await?;

        let insert_statement = client
            .prepare_typed("insert INTO t (id) VALUES ($1)", &[])
            .await?;

        for i in 0..10 {
            client.execute(&insert_statement, &[&i]).await?;
        }
        client.execute("flush", &[]).await?;

        let transaction = client.transaction().await?;
        let statement = transaction
            .prepare_typed("SELECT * FROM t order by id", &[])
            .await?;
        let portal = transaction.bind(&statement, &[]).await?;

        for t in 0..5 {
            let rows = transaction.query_portal(&portal, 1).await?;
            test_eq!(rows.len(), 1);
            let row = rows.get(0).unwrap();
            let id: i32 = row.get(0);
            test_eq!(id, t);
        }

        let mut i = 5;
        for row in transaction.query_portal(&portal, 3).await? {
            let id: i32 = row.get(0);
            test_eq!(id, i);
            i += 1;
        }
        test_eq!(i, 8);

        for row in transaction.query_portal(&portal, 5).await? {
            let id: i32 = row.get(0);
            test_eq!(id, i);
            i += 1;
        }
        test_eq!(i, 10);

        transaction.rollback().await?;

        client.execute("drop table t", &[]).await?;

        Ok(())
    }

    async fn multiple_on_going_portal(&self) -> anyhow::Result<()> {
        let mut client = self.create_client(false).await?;

        let transaction = client.transaction().await?;
        let statement = transaction
            .prepare_typed("SELECT generate_series(1,5,1)", &[])
            .await?;
        let portal_1 = transaction.bind(&statement, &[]).await?;
        let portal_2 = transaction.bind(&statement, &[]).await?;

        let rows = transaction.query_portal(&portal_1, 1).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.get(0).unwrap().get::<usize, i32>(0), 1);

        let rows = transaction.query_portal(&portal_2, 1).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.get(0).unwrap().get::<usize, i32>(0), 1);

        let rows = transaction.query_portal(&portal_2, 3).await?;
        test_eq!(rows.len(), 3);
        test_eq!(rows.get(0).unwrap().get::<usize, i32>(0), 2);
        test_eq!(rows.get(1).unwrap().get::<usize, i32>(0), 3);
        test_eq!(rows.get(2).unwrap().get::<usize, i32>(0), 4);

        let rows = transaction.query_portal(&portal_1, 1).await?;
        test_eq!(rows.len(), 1);
        test_eq!(rows.get(0).unwrap().get::<usize, i32>(0), 2);

        Ok(())
    }

    // Can't support these sql
    async fn create_with_parameter(&self) -> anyhow::Result<()> {
        let client = self.create_client(false).await?;

        test_eq!(
            client
                .query("create table t as select $1", &[])
                .await
                .is_err(),
            true
        );
        test_eq!(
            client
                .query("create view v as select $1", &[])
                .await
                .is_err(),
            true
        );
        test_eq!(
            client
                .query("create materialized view v as select $1", &[])
                .await
                .is_err(),
            true
        );

        Ok(())
    }

    async fn simple_cancel(&self, is_distributed: bool) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;

        client.execute("create table t(id int)", &[]).await?;

        let insert_statement = client
            .prepare_typed("insert INTO t (id) VALUES ($1)", &[])
            .await?;

        for i in 0..1000 {
            client.execute(&insert_statement, &[&i]).await?;
        }

        client.execute("flush", &[]).await?;

        let cancel_token = client.cancel_token();

        let query_handle = tokio::spawn(async move {
            client.query("select * from t", &[]).await.unwrap();
        });

        select! {
            _ = query_handle => {
                tracing::error!("Failed to cancel query")
            },
            _ = cancel_token.cancel_query(NoTls) => {
                tracing::trace!("Cancel query successfully")
            },
        }

        let new_client = self.create_client(is_distributed).await?;

        let rows = new_client
            .query("select * from t order by id limit 10", &[])
            .await?;

        test_eq!(rows.len(), 10);
        for (expect_id, row) in rows.iter().enumerate() {
            let id: i32 = row.get(0);
            test_eq!(id, expect_id as i32);
        }

        new_client.execute("drop table t", &[]).await?;

        Ok(())
    }

    async fn complex_cancel(&self, is_distributed: bool) -> anyhow::Result<()> {
        let client = self.create_client(is_distributed).await?;

        client
            .execute("create table t1(name varchar, id int)", &[])
            .await?;
        client
            .execute("create table t2(name varchar, id int)", &[])
            .await?;
        client
            .execute("create table t3(name varchar, id int)", &[])
            .await?;

        let insert_statement = client
            .prepare_typed("insert INTO t1 (name, id) VALUES ($1, $2)", &[])
            .await?;
        let insert_statement2 = client
            .prepare_typed("insert INTO t2 (name, id) VALUES ($1, $2)", &[])
            .await?;
        let insert_statement3 = client
            .prepare_typed("insert INTO t3 (name, id) VALUES ($1, $2)", &[])
            .await?;
        for i in 0..1000 {
            client
                .execute(&insert_statement, &[&i.to_string(), &i])
                .await?;
            client
                .execute(&insert_statement2, &[&i.to_string(), &i])
                .await?;
            client
                .execute(&insert_statement3, &[&i.to_string(), &i])
                .await?;
        }

        client.execute("flush", &[]).await?;

        client.execute("set query_mode=local", &[]).await?;

        let cancel_token = client.cancel_token();

        let query_sql = "SELECT t1.name, t2.id, t3.name
        FROM t1
        INNER JOIN (
          SELECT id, name
          FROM t2
          WHERE id IN (
            SELECT id
            FROM t1
            WHERE name LIKE '%1%'
          )
        ) AS t2 ON t1.id = t2.id
        LEFT JOIN t3 ON t2.name = t3.name
        WHERE t3.id IN (
          SELECT MAX(id)
          FROM t3
          GROUP BY name
        )
        ORDER BY t1.name ASC, t3.id DESC
        ";

        let query_handle = tokio::spawn(async move {
            client.query(query_sql, &[]).await.unwrap();
        });

        select! {
            _ = query_handle => {
                tracing::error!("Failed to cancel query")
            },
            _ = cancel_token.cancel_query(NoTls) => {
                tracing::trace!("Cancel query successfully")
            },
        }

        let new_client = self.create_client(is_distributed).await?;

        let rows = new_client
            .query(&format!("{} LIMIT 10", query_sql), &[])
            .await?;
        let expect_ans = vec![
            (1, 1, 1),
            (10, 10, 10),
            (100, 100, 100),
            (101, 101, 101),
            (102, 102, 102),
            (103, 103, 103),
            (104, 104, 104),
            (105, 105, 105),
            (106, 106, 106),
            (107, 107, 107),
        ];
        for (i, row) in rows.iter().enumerate() {
            test_eq!(
                row.get::<_, String>(0).parse::<i32>().unwrap(),
                expect_ans[i].0
            );
            test_eq!(row.get::<_, i32>(1), expect_ans[i].1);
            test_eq!(
                row.get::<_, String>(2).parse::<i32>().unwrap(),
                expect_ans[i].2
            );
        }

        new_client.execute("drop table t1", &[]).await?;
        new_client.execute("drop table t2", &[]).await?;
        new_client.execute("drop table t3", &[]).await?;
        Ok(())
    }
}
