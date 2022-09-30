use std::fmt::Write;

use anyhow::Result;

use crate::cluster::Cluster;

impl Cluster {
    /// Run statements to create the nexmark sources.
    pub async fn create_nexmark_source(
        &mut self,
        split_num: usize,
        event_num: Option<usize>,
    ) -> Result<()> {
        let extra_args = {
            let mut output = String::new();
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
    nexmark.table.type = 'Auction',
    nexmark.min.event.gap.in.ns = '100000'
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
    nexmark.table.type = 'Bid',
    nexmark.min.event.gap.in.ns = '100000'
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
    nexmark.table.type = 'Person',
    nexmark.min.event.gap.in.ns = '100000'
    {extra_args}
) row format JSON;
"#,
        ))
        .await?;

        Ok(())
    }
}
