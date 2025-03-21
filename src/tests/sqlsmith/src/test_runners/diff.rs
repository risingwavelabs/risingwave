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

//! Provides E2E Test runner functionality.

use anyhow::bail;
use itertools::Itertools;
use rand::Rng;
use similar::{ChangeTag, TextDiff};
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::test_runners::utils::{
    Result, create_base_tables, create_mviews, drop_mview_table, drop_tables, format_drop_mview,
    generate_rng, populate_tables, run_query, run_query_inner, set_variable, update_base_tables,
};
use crate::{Table, differential_sql_gen};

/// Differential testing for batch and stream
pub async fn run_differential_testing(
    client: &Client,
    testdata: &str,
    count: usize,
    seed: Option<u64>,
) -> Result<()> {
    let mut rng = generate_rng(seed);

    set_variable(client, "RW_IMPLICIT_FLUSH", "TRUE").await;
    set_variable(client, "QUERY_MODE", "DISTRIBUTED").await;
    tracing::info!("Set session variables");

    let base_tables = create_base_tables(testdata, client).await.unwrap();

    let rows_per_table = 50;
    let inserts = populate_tables(client, &mut rng, base_tables.clone(), rows_per_table).await;
    tracing::info!("Populated base tables");

    let (tables, mviews) = create_mviews(&mut rng, base_tables.clone(), client)
        .await
        .unwrap();
    tracing::info!("Created tables");

    // Generate an update for some inserts, on the corresponding table.
    update_base_tables(client, &mut rng, &base_tables, &inserts).await;
    tracing::info!("Ran updates");

    for i in 0..count {
        diff_stream_and_batch(&mut rng, tables.clone(), client, i).await?
    }

    drop_tables(&mviews, testdata, client).await;
    tracing::info!("[EXECUTION SUCCESS]");
    Ok(())
}

/// Create the tables defined in testdata, along with some mviews.
/// Just test number of rows for now.
/// TODO(kwannoel): Test row contents as well. That requires us to run a batch query
/// with `select * ORDER BY <all columns>`.
async fn diff_stream_and_batch(
    rng: &mut impl Rng,
    mvs_and_base_tables: Vec<Table>,
    client: &Client,
    i: usize,
) -> Result<()> {
    // Generate some mviews
    let mview_name = format!("stream_{}", i);
    let (batch, stream, table) = differential_sql_gen(rng, mvs_and_base_tables, &mview_name)?;
    diff_stream_and_batch_with_sqls(client, i, &batch, &stream, &mview_name, &table).await
}

async fn diff_stream_and_batch_with_sqls(
    client: &Client,
    i: usize,
    batch: &str,
    stream: &str,
    mview_name: &str,
    table: &Table,
) -> Result<()> {
    tracing::info!("[RUN CREATE MVIEW id={}]: {}", i, stream);
    let skip_count = run_query(12, client, stream).await?;
    if skip_count > 0 {
        tracing::info!("[RUN DROP MVIEW id={}]: {}", i, &format_drop_mview(table));
        drop_mview_table(table, client).await;
        return Ok(());
    }

    let select = format!("SELECT * FROM {}", &mview_name);
    tracing::info!("[RUN SELECT * FROM MVIEW id={}]: {}", i, select);
    let (skip_count, stream_result) = run_query_inner(12, client, &select, true).await?;
    if skip_count > 0 {
        bail!("SQL should not fail: {:?}", select)
    }

    tracing::info!("[RUN - BATCH QUERY id={}]: {}", i, &batch);
    let (skip_count, batch_result) = run_query_inner(12, client, batch, true).await?;
    if skip_count > 0 {
        tracing::info!(
            "[DIFF - DROP MVIEW id={}]: {}",
            i,
            &format_drop_mview(table)
        );
        drop_mview_table(table, client).await;
        return Ok(());
    }
    let n_stream_rows = stream_result.len();
    let n_batch_rows = batch_result.len();
    let formatted_stream_rows = format_rows(&batch_result);
    let formatted_batch_rows = format_rows(&stream_result);
    tracing::debug!(
        "[COMPARE - STREAM_FORMATTED_ROW id={}]: {formatted_stream_rows}",
        i,
    );
    tracing::debug!(
        "[COMPARE - BATCH_FORMATTED_ROW id={}]: {formatted_batch_rows}",
        i,
    );

    let diff = TextDiff::from_lines(&formatted_batch_rows, &formatted_stream_rows);

    let diff: String = diff
        .iter_all_changes()
        .filter_map(|change| match change.tag() {
            ChangeTag::Delete => Some(format!("-{}", change)),
            ChangeTag::Insert => Some(format!("+{}", change)),
            ChangeTag::Equal => None,
        })
        .collect();

    if diff.is_empty() {
        tracing::info!("[RUN DROP MVIEW id={}]: {}", i, format_drop_mview(table));
        tracing::info!("[PASSED DIFF id={}, rows_compared={n_stream_rows}]", i);

        drop_mview_table(table, client).await;
        Ok(())
    } else {
        bail!(
            "
Different results for batch and stream:

BATCH SQL:
{batch}

STREAM SQL:
{stream}

SELECT FROM STREAM SQL:
{select}

BATCH_ROW_LEN:
{n_batch_rows}

STREAM_ROW_LEN:
{n_stream_rows}

BATCH_ROWS:
{formatted_batch_rows}

STREAM_ROWS:
{formatted_stream_rows}

ROW DIFF (+/-):
{diff}
",
        )
    }
}

/// Format + sort rows so they can be diffed.
fn format_rows(rows: &[SimpleQueryMessage]) -> String {
    rows.iter()
        .filter_map(|r| match r {
            SimpleQueryMessage::Row(row) => {
                let n_cols = row.columns().len();
                let formatted_row: String = (0..n_cols)
                    .map(|i| {
                        format!(
                            "{:#?}",
                            match row.get(i) {
                                Some(s) => s,
                                _ => "NULL",
                            }
                        )
                    })
                    .join(", ");
                Some(formatted_row)
            }
            SimpleQueryMessage::CommandComplete(_n_rows) => None,
            _ => unreachable!(),
        })
        .sorted()
        .join("\n")
}
