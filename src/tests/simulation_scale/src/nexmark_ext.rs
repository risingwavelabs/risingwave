// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Write;
use std::time::Duration;

use anyhow::Result;

use crate::cluster::Cluster;

// The target number of events of the three sources per second totally.
pub const THROUGHPUT: usize = 10_000;

impl Cluster {
    /// Run statements to create the nexmark sources.
    ///
    /// If `event_num` is specified, the sources should finish in `event_num / NEXMARK_THROUGHPUT`
    /// seconds.
    pub async fn create_nexmark_source(
        &mut self,
        split_num: usize,
        event_num: Option<usize>,
    ) -> Result<()> {
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
            output
        };

        self.run(format!(
            r#"
create source auction (
    id INTEGER,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid INTEGER,
    reserve INTEGER,
    date_time TIMESTAMP,
    expires TIMESTAMP,
    seller INTEGER,
    category INTEGER)
with (
    connector = 'nexmark',
    nexmark.table.type = 'Auction'
    {extra_args}
) row format JSON;
"#,
        ))
        .await?;

        self.run(format!(
            r#"
create source bid (
    auction INTEGER,
    bidder INTEGER,
    price INTEGER,
    "date_time" TIMESTAMP)
with (
    connector = 'nexmark',
    nexmark.table.type = 'Bid'
    {extra_args}
) row format JSON;
"#,
        ))
        .await?;

        self.run(format!(
            r#"
create source person (
    id INTEGER,
    name VARCHAR,
    email_address VARCHAR,
    credit_card VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP)
with (
    connector = 'nexmark',
    nexmark.table.type = 'Person'
    {extra_args}
) row format JSON;
"#,
        ))
        .await?;

        Ok(())
    }
}
