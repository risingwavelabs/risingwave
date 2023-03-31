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
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;

use crate::opts::Opts;

pub struct TestSuite {
    config: String,
}

macro_rules! test_eq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                    return Err(anyhow!(
                        "assertion failed: `(left == right)` \
                                (left: `{:?}`, right: `{:?}`)",
                        left_val,
                        right_val
                    ));
                }
            }
        }
    };
}

impl TestSuite {
    pub fn new(opts: Opts) -> Self {
        let config = if !opts.pg_password.is_empty() {
            format!(
                "dbname={} user={} host={} port={} password={}",
                opts.pg_db_name,
                opts.pg_user_name,
                opts.pg_server_host,
                opts.pg_server_port,
                opts.pg_password
            )
        } else {
            format!(
                "dbname={} user={} host={} port={}",
                opts.pg_db_name, opts.pg_user_name, opts.pg_server_host, opts.pg_server_port
            )
        };
        Self { config }
    }

    pub async fn test(&self) -> anyhow::Result<()> {
        self.binary_param_and_result().await?;
        self.dql_dml_with_param().await?;
        self.max_row().await?;
        Ok(())
    }

    pub async fn binary_param_and_result(&self) -> anyhow::Result<()> {
        // Connect to the database.
        let (client, connection) = tokio_postgres::connect(&self.config, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

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
            .query("select $1::DECIMAL;", &[&Decimal::from_f32(2.33454_f32)])
            .await?
        {
            let data: Decimal = row.try_get(0)?;
            test_eq!(data, Decimal::from_f32(2.33454_f32).unwrap());
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
        let (client, connection) = tokio_postgres::connect(&self.config, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

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
        let (mut client, connection) = tokio_postgres::connect(&self.config, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

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
}
