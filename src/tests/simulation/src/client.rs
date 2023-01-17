// Copyright 2023 Singularity Data
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

use std::time::Duration;

use sqllogictest_engine::postgres::Postgres;
use sqllogictest_engine::DBConfig;

/// A RisingWave client for `sqllogictest`.
pub struct RisingWave {
    inner: Postgres,
    host: String,
    dbname: String,
}

impl RisingWave {
    pub async fn connect(host: String, dbname: String) -> Result<Self, tokio_postgres::Error> {
        let inner = Postgres::connect(&DBConfig {
            addrs: vec![(host.clone(), 4566)],
            db: dbname.clone(),
            user: "root".to_owned(),
            pass: "".to_owned(),
        })
        .await?;

        // For recovery.
        inner
            .pg_client()
            .simple_query("SET RW_IMPLICIT_FLUSH TO true;")
            .await?;
        inner
            .pg_client()
            .simple_query("SET CREATE_COMPACTION_GROUP_FOR_MV TO true;")
            .await?;

        Ok(RisingWave {
            inner,
            host,
            dbname,
        })
    }

    /// Returns a reference of the inner Postgres client.
    pub fn pg_client(&self) -> &tokio_postgres::Client {
        self.inner.pg_client()
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for RisingWave {
    type Error = tokio_postgres::Error;

    async fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput, Self::Error> {
        // For recovery.
        if self.pg_client().is_closed() {
            // connection error, reset the client
            *self = Self::connect(self.host.clone(), self.dbname.clone()).await?;
        }

        self.inner.run(sql).await
    }

    fn engine_name(&self) -> &str {
        "risingwave"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
