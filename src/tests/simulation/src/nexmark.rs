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
    ) -> Result<Self> {
        let mut cluster = Self {
            cluster: Cluster::start(conf).await?,
        };
        cluster.create_nexmark_source(split_num, event_num).await?;
        Ok(cluster)
    }

    /// Run statements to create the nexmark sources.
    async fn create_nexmark_source(
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
            write!(output, ", nexmark.max.chunk.size = 256")?;
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
// TODO: import the query from external files and avoid duplicating the queries in the code.
pub mod queries {
    use std::time::Duration;

    const DEFAULT_INITIAL_INTERVAL: Duration = Duration::from_secs(1);
    const DEFAULT_INITIAL_TIMEOUT: Duration = Duration::from_secs(20);

    pub mod q3 {
        //! Covers hash inner join.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q3
AS
SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'or' OR P.state = 'id' OR P.state = 'ca');
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q3 ORDER BY id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q3;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q4 {
        //! Covers hash inner join and hash aggregation.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q4
AS
SELECT
    Q.category,
    AVG(Q.final) as avg
FROM (
    SELECT
        MAX(B.price) AS final,A.category
    FROM
        auction A,
        bid B
    WHERE
        A.id = B.auction AND
        B.date_time BETWEEN A.date_time AND A.expires
    GROUP BY
        A.id,A.category
    ) Q
GROUP BY
    Q.category;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q4 ORDER BY category;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q4;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q5 {
        //! Covers self-join.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q5
AS
SELECT AuctionBids.auction, AuctionBids.num FROM (
  SELECT
    bid.auction,
    count(*) AS num,
    window_start AS starttime
  FROM
    HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
  GROUP BY
    window_start,
    bid.auction
) AS AuctionBids
JOIN (
  SELECT
    max(CountBids.num) AS maxn,
    CountBids.starttime_c
  FROM (
    SELECT
      count(*) AS num,
      window_start AS starttime_c
    FROM HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
    GROUP BY
      bid.auction,
      window_start
  ) AS CountBids
  GROUP BY
    CountBids.starttime_c
) AS MaxBids
ON AuctionBids.starttime = MaxBids.starttime_c AND AuctionBids.num >= MaxBids.maxn;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q5 ORDER BY auction;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q5;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q7 {
        //! Covers self-join.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q7
AS
SELECT
  B.auction,
  B.price,
  B.bidder,
  B.date_time
FROM
  bid B
JOIN (
  SELECT
    MAX(price) AS maxprice,
    window_end as date_time
  FROM
    TUMBLE(bid, date_time, INTERVAL '10' SECOND)
  GROUP BY
    window_end
) B1 ON B.price = B1.maxprice
WHERE
  B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND
  AND B1.date_time;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q7 ORDER BY date_time;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q7;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q8 {
        //! Covers self-join.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q8
AS
SELECT
  P.id,
  P.name,
  P.starttime
FROM (
  SELECT
    id,
    name,
    window_start AS starttime,
    window_end AS endtime
  FROM
    TUMBLE(person, date_time, INTERVAL '10' SECOND)
  GROUP BY
    id,
    name,
    window_start,
    window_end
) P
JOIN (
  SELECT
    seller,
    window_start AS starttime,
    window_end AS endtime
  FROM
    TUMBLE(auction, date_time, INTERVAL '10' SECOND)
  GROUP BY
    seller,
    window_start,
    window_end
) A ON P.id = A.seller
  AND P.starttime = A.starttime
  AND P.endtime = A.endtime;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q8 ORDER BY id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q8;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q9 {
        //! Covers group top-n.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q9
AS
SELECT
  id, item_name, description, initial_bid, reserve, date_time, expires, seller, category,
  auction, bidder, price, bid_date_time
FROM (
  SELECT A.*, B.auction, B.bidder, B.price, B.date_time AS bid_date_time,
    ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.date_time ASC) AS rownum
  FROM auction A, bid B
  WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
)
WHERE rownum <= 1;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q9 ORDER BY id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q9;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q101 {
        //! A self-made query that covers outer join.
        //!
        //! Monitor ongoing auctions and track the current highest bid for each one in real-time. If
        //! the auction has no bids, the highest bid will be NULL.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q101
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    b.max_price AS current_highest_bid
FROM auction a
LEFT OUTER JOIN (
    SELECT
        b1.auction,
        MAX(b1.price) max_price
    FROM bid b1
    GROUP BY b1.auction
) b ON a.id = b.auction;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q101 ORDER BY auction_id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q101;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q102 {
        //! A self-made query that covers dynamic filter and simple aggregation.
        //!
        //! Show the auctions whose count of bids is greater than the overall average count of bids
        //! per auction.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q102
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    COUNT(b.auction) AS bid_count
FROM auction a
JOIN bid b ON a.id = b.auction
GROUP BY a.id, a.item_name
HAVING COUNT(b.auction) >= (
    SELECT COUNT(*) / COUNT(DISTINCT auction) FROM bid
);
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q102 ORDER BY auction_id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q102;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q103 {
        //! A self-made query that covers semi join.
        //!
        //! Show the auctions that have at least 20 bids.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q103
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name
FROM auction a
WHERE a.id IN (
    SELECT b.auction FROM bid b
    GROUP BY b.auction
    HAVING COUNT(*) >= 20
);
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q103 ORDER BY auction_id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q103;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q104 {
        //! A self-made query that covers anti join.
        //!
        //! This is the same as q103, which shows the auctions that have at least 20 bids.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q104
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name
FROM auction a
WHERE a.id NOT IN (
    SELECT b.auction FROM bid b
    GROUP BY b.auction
    HAVING COUNT(*) < 20
);
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q104 ORDER BY auction_id;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q104;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }

    pub mod q105 {
        //! A self-made query that covers singleton top-n (and local-phase group top-n).
        //!
        //! Show the top 1000 auctions by the number of bids.

        use super::*;
        pub const CREATE: &str = r#"
CREATE MATERIALIZED VIEW nexmark_q105
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    COUNT(b.auction) AS bid_count
FROM auction a
JOIN bid b ON a.id = b.auction
GROUP BY a.id, a.item_name
ORDER BY bid_count DESC
LIMIT 1000;
"#;
        pub const SELECT: &str = r#"
SELECT * FROM nexmark_q105;
"#;
        pub const DROP: &str = r#"
DROP MATERIALIZED VIEW nexmark_q105;
"#;
        pub const INITIAL_INTERVAL: Duration = DEFAULT_INITIAL_INTERVAL;
        pub const INITIAL_TIMEOUT: Duration = DEFAULT_INITIAL_TIMEOUT;
    }
}
