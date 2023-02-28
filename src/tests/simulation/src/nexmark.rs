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

use std::fmt::Write;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use anyhow::Result;

use crate::cluster::{Cluster, Configuration};

/// The target number of events of the three sources per second totally.
pub const THROUGHPUT: usize = 5_000;

/// Cluster for nexmark tests.
pub struct NexmarkCluster {
    pub cluster: Cluster,
}

impl NexmarkCluster {
    /// Create a cluster with nexmark sources created.
    ///
    /// If `event_num` is specified, the sources should finish in `event_num / NEXMARK_THROUGHPUT`
    /// seconds.
    pub async fn new(
        conf: Configuration,
        split_num: usize,
        event_num: Option<usize>,
        watermark: bool,
    ) -> Result<Self> {
        let mut cluster = Self {
            cluster: Cluster::start(conf).await?,
        };
        cluster
            .create_nexmark_source(split_num, event_num, watermark)
            .await?;
        Ok(cluster)
    }

    /// Run statements to create the nexmark sources.
    async fn create_nexmark_source(
        &mut self,
        split_num: usize,
        event_num: Option<usize>,
        watermark: bool,
    ) -> Result<()> {
        let watermark_column = if watermark {
            ", WATERMARK FOR date_time AS date_time - INTERVAL '4' SECOND"
        } else {
            ""
        };

        let extra_args = {
            let mut output = String::new();
            write!(
                output,
                ", nexmark.min.event.gap.in.ns = '{}'",
                Duration::from_secs(1).as_nanos() / THROUGHPUT as u128
            )?;
            write!(output, ", nexmark.split.num = '{split_num}'")?;
            if let Some(event_num) = event_num {
                write!(output, ", nexmark.event.num = '{event_num}'")?;
            }
            write!(output, ", nexmark.max.chunk.size = 256")?;
            output
        };

        self.run(format!(
            include_str!("nexmark/create_source.sql"),
            watermark_column = watermark_column,
            extra_args = extra_args
        ))
        .await?;

        Ok(())
    }
}

impl Deref for NexmarkCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl DerefMut for NexmarkCluster {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cluster
    }
}

/// Nexmark queries.
pub mod queries {
    use std::time::Duration;

    const DEFAULT_INITIAL_INTERVAL: Duration = Duration::from_secs(1);
    const DEFAULT_INITIAL_TIMEOUT: Duration = Duration::from_secs(20);

    pub mod q3 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q3.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q3 ORDER BY id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q3;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q4 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q4.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q4 ORDER BY category;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q4;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q5 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q5.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q5 ORDER BY auction;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q5;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q7 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q7.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q7 ORDER BY date_time;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q7;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q8 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q8.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q8 ORDER BY id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q8;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q9 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q9.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q9 ORDER BY id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q9;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q15 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q15.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q15 ORDER BY day ASC, total_bids DESC;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q15;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q18 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q18.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q18 ORDER BY auction, bidder, price DESC;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q18;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q101 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q101.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q101 ORDER BY auction_id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q101;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q102 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q102.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q102 ORDER BY auction_id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q102;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q103 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q103.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q103 ORDER BY auction_id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q103;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q104 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q104.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q104 ORDER BY auction_id;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q104;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q105 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q105.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q105;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q105;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q106 {
        use super::*;
        pub const CREATE: &str = include_str!("nexmark/q106.sql");
        pub const SELECT: &str = "SELECT * FROM nexmark_q106;";
        pub const DROP: &str = "DROP MATERIALIZED VIEW nexmark_q106;";
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }
}
