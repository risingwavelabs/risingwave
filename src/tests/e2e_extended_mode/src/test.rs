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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pg_interval::Interval;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use tokio_postgres::NoTls;

use crate::opts::Opts;

pub struct TestSuite {
    config: String,
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
        self.binary_param_and_result().await
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
            assert_eq!(data, 1024);
        }

        for row in client.query("select $1::INT;", &[&144232_i32]).await? {
            let data: i32 = row.try_get(0)?;
            assert_eq!(data, 144232);
        }

        for row in client.query("select $1::BIGINT;", &[&99999999_i64]).await? {
            let data: i64 = row.try_get(0)?;
            assert_eq!(data, 99999999);
        }

        for row in client
            .query("select $1::DECIMAL;", &[&Decimal::from_f32(2.33454_f32)])
            .await?
        {
            let data: Decimal = row.try_get(0)?;
            assert_eq!(data, Decimal::from_f32(2.33454_f32).unwrap());
        }

        for row in client.query("select $1::BOOL;", &[&true]).await? {
            let data: bool = row.try_get(0)?;
            assert!(data);
        }

        for row in client.query("select $1::REAL;", &[&1.234234_f32]).await? {
            let data: f32 = row.try_get(0)?;
            assert_eq!(data, 1.234234);
        }

        // TODO(ZENOTME): After #8112, risingwave should support this case. (DOUBLE PRECISION TYPE)
        // for row in client
        //     .query("select $1::DOUBLE PRECISION;", &[&234234.23490238483_f64])
        //     .await?
        // {
        //     let data: f64 = row.try_get(0)?;
        //     assert_eq!(data, 234234.23490238483);
        // }
        for row in client
            .query("select $1::FLOAT8;", &[&234234.23490238483_f64])
            .await?
        {
            let data: f64 = row.try_get(0)?;
            assert_eq!(data, 234234.23490238483);
        }

        for row in client
            .query(
                "select $1::date;",
                &[&NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()],
            )
            .await?
        {
            let data: NaiveDate = row.try_get(0)?;
            assert_eq!(data, NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
        }

        for row in client
            .query(
                "select $1::time",
                &[&NaiveTime::from_hms_opt(10, 0, 0).unwrap()],
            )
            .await?
        {
            let data: NaiveTime = row.try_get(0)?;
            assert_eq!(data, NaiveTime::from_hms_opt(10, 0, 0).unwrap());
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
            assert_eq!(
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
            assert_eq!(data, timestamptz);
        }

        for row in client
            .query("select $1::interval", &[&Interval::new(1, 1, 24000000)])
            .await?
        {
            let data: Interval = row.try_get(0)?;
            assert_eq!(data, Interval::new(1, 1, 24000000));
        }

        Ok(())
    }
}
