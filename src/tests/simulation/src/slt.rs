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

use std::cmp::min;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use itertools::Itertools;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use sqllogictest::{Condition, ParallelTestError, QueryExpect, Record, StatementExpect};

use crate::client::RisingWave;
use crate::cluster::{Cluster, KillOpts};
use crate::utils::TimedExt;

// retry a maximum times until it succeed
const MAX_RETRY: usize = 10;

fn is_create_table_as(sql: &str) -> bool {
    let parts: Vec<String> = sql.split_whitespace().map(|s| s.to_lowercase()).collect();

    parts.len() >= 4 && parts[0] == "create" && parts[1] == "table" && parts[3] == "as"
}

fn is_sink_into_table(sql: &str) -> bool {
    let parts: Vec<String> = sql.split_whitespace().map(|s| s.to_lowercase()).collect();

    parts.len() >= 4 && parts[0] == "create" && parts[1] == "sink" && parts[3] == "into"
}

#[derive(Debug, PartialEq, Eq)]
enum SqlCmd {
    /// Other create statements.
    Create {
        is_create_table_as: bool,
    },
    /// Create sink.
    CreateSink {
        is_sink_into_table: bool,
    },
    /// Create Materialized views
    CreateMaterializedView {
        name: String,
    },
    /// Set background ddl
    SetBackgroundDdl {
        enable: bool,
    },
    Drop,
    Dml,
    Flush,
    Alter,
    Others,
}

impl SqlCmd {
    fn allow_kill(&self) -> bool {
        matches!(
            self,
            SqlCmd::Create {
                // `create table as` is also not atomic in our system.
                is_create_table_as: false,
                ..
            } | SqlCmd::CreateSink {
                is_sink_into_table: false,
            } | SqlCmd::CreateMaterializedView { .. }
                | SqlCmd::Drop
        )
        // We won't kill during insert/update/delete/alter since the atomicity is not guaranteed.
        // TODO: For `SqlCmd::Alter`, since table fragment and catalog commit for table schema change
        // are not transactional, we can't kill during `alter table add/drop columns` for now, will
        // remove it until transactional commit of table fragment and catalog is supported.
    }

    fn is_create(&self) -> bool {
        matches!(
            self,
            SqlCmd::Create { .. }
                | SqlCmd::CreateSink { .. }
                | SqlCmd::CreateMaterializedView { .. }
        )
    }
}

fn extract_sql_command(sql: &str) -> SqlCmd {
    let sql = sql.to_lowercase();
    let tokens = sql.split_whitespace();
    let mut tokens = tokens.multipeek();
    let first_token = tokens.next().unwrap_or("");

    match first_token {
        // NOTE(kwannoel):
        // It's entirely possible for a malformed command to be parsed as `SqlCmd::Create`.
        // BUT an error should be expected for such a test.
        // So we don't need to handle this case.
        // Eventually if there are too many edge cases, we can opt to use our parser.
        "create" => {
            let result: Option<SqlCmd> = try {
                match tokens.next()? {
                    "materialized" => {
                        // view
                        tokens.next()?;

                        // if not exists | name
                        let next = *tokens.peek()?;
                        if "if" == next
                            && let Some("not") = tokens.peek().cloned()
                            && let Some("exists") = tokens.peek().cloned()
                        {
                            tokens.next();
                            tokens.next();
                            tokens.next();
                            let name = tokens.next()?.to_owned();
                            SqlCmd::CreateMaterializedView { name }
                        } else {
                            let next = next.to_owned();
                            let mut name = next.split("("); // handle mv(col_name ...) pattern
                            let name = name.next().expect("MV should have name").to_owned();
                            SqlCmd::CreateMaterializedView { name }
                        }
                    }
                    "sink" => SqlCmd::CreateSink {
                        is_sink_into_table: is_sink_into_table(&sql),
                    },
                    _ => SqlCmd::Create {
                        is_create_table_as: is_create_table_as(&sql),
                    },
                }
            };
            result.unwrap_or(SqlCmd::Others)
        }
        "set" => {
            if sql.contains("background_ddl") {
                let enable = sql.contains("true");
                SqlCmd::SetBackgroundDdl { enable }
            } else {
                SqlCmd::Others
            }
        }
        "drop" => SqlCmd::Drop,
        "insert" | "update" | "delete" => SqlCmd::Dml,
        "flush" => SqlCmd::Flush,
        "alter" => SqlCmd::Alter,
        _ => SqlCmd::Others,
    }
}

const KILL_IGNORE_FILES: &[&str] = &[
    // TPCH queries are too slow for recovery.
    "tpch_snapshot.slt",
    "tpch_upstream.slt",
    // Drop is not retryable in search path test.
    "search_path.slt",
    // Transaction statements are not retryable.
    "transaction/now.slt",
    "transaction/read_only_multi_conn.slt",
    "transaction/read_only.slt",
    "transaction/tolerance.slt",
    "transaction/cursor.slt",
    "transaction/cursor_multi_conn.slt",
];

/// Wait for background mv to finish creating
async fn wait_background_mv_finished(mview_name: &str) -> Result<()> {
    let Ok(rw) = RisingWave::connect("frontend".into(), "dev".into()).await else {
        bail!("failed to connect to frontend for {mview_name}");
    };
    let client = rw.pg_client();
    if client.simple_query("WAIT;").await.is_err() {
        bail!("failed to wait for background mv to finish creating for {mview_name}");
    }

    let Ok(result) = client
        .query(
            "select count(*) from pg_matviews where matviewname=$1;",
            &[&mview_name],
        )
        .await
    else {
        bail!("failed to query pg_matviews for {mview_name}");
    };

    match result[0].try_get::<_, i64>(0) {
        Ok(1) => Ok(()),
        r => bail!("expected 1 row in pg_matviews, got {r:#?} instead for {mview_name}"),
    }
}

pub struct Opts {
    pub kill_opts: KillOpts,
    /// Probability of `background_ddl` being set to true per ddl record.
    pub background_ddl_rate: f64,
    /// Set vnode count (`STREAMING_MAX_PARALLELISM`) to random value before running DDL.
    pub random_vnode_count: bool,
}

/// Run the sqllogictest files in `glob`.
pub async fn run_slt_task(
    cluster: Arc<Cluster>,
    glob: &str,
    Opts {
        kill_opts,
        background_ddl_rate,
        random_vnode_count,
    }: Opts,
) {
    tracing::info!("background_ddl_rate: {}", background_ddl_rate);
    let seed = std::env::var("MADSIM_TEST_SEED")
        .unwrap_or("0".to_owned())
        .parse::<u64>()
        .unwrap();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let kill = kill_opts.kill_compute
        || kill_opts.kill_meta
        || kill_opts.kill_frontend
        || kill_opts.kill_compactor;
    let files = glob::glob(glob).expect("failed to read glob pattern");
    for file in files {
        // use a session per file
        let mut tester =
            sqllogictest::Runner::new(|| RisingWave::connect("frontend".into(), "dev".into()));
        tester.add_label("madsim");

        let file = file.unwrap();
        let path = file.as_path();
        println!("{}", path.display());
        if kill && KILL_IGNORE_FILES.iter().any(|s| path.ends_with(s)) {
            continue;
        }
        // XXX: hack for kafka source test
        let tempfile = (path.ends_with("kafka.slt") || path.ends_with("kafka_batch.slt"))
            .then(|| hack_kafka_test(path));
        let path = tempfile.as_ref().map(|p| p.path()).unwrap_or(path);

        // NOTE(kwannoel): For background ddl
        let mut background_ddl_enabled = false;

        // If background ddl is set to true within the test case, prevent random setting of background_ddl to true.
        // We can revert it back to false only if we encounter a record that sets background_ddl to false.
        let mut manual_background_ddl_enabled = false;

        let records = sqllogictest::parse_file(path).expect("failed to parse file");
        let random_vnode_count = random_vnode_count
            // Skip using random vnode count if the test case cares about parallelism, including
            // setting parallelism manually or checking the parallelism with system tables.
            && records.iter().all(|record| {
                if let Record::Statement { sql, .. } | Record::Query { sql, .. } = record
                    && sql.to_lowercase().contains("parallelism")
                {
                    println!("[RANDOM VNODE COUNT] skip: {}", path.display());
                    false
                } else {
                    true
                }
            });

        for record in records {
            // uncomment to print metrics for task counts
            // let metrics = madsim::runtime::Handle::current().metrics();
            // println!("{:#?}", metrics);
            // println!("{}", metrics.num_tasks_by_node_by_spawn());
            if let sqllogictest::Record::Halt { .. } = record {
                break;
            }

            let cmd = match &record {
                sqllogictest::Record::Statement {
                    sql, conditions, ..
                }
                | sqllogictest::Record::Query {
                    sql, conditions, ..
                } if conditions
                    .iter()
                    .all(|c| !matches!(c, Condition::SkipIf{ label } if label == "madsim"))
                    && !conditions
                        .iter()
                        .any(|c| matches!(c, Condition::OnlyIf{ label} if label != "madsim" )) =>
                {
                    extract_sql_command(sql)
                }
                _ => SqlCmd::Others,
            };

            // For normal records.
            if !kill {
                // Set random vnode count if needed.
                if random_vnode_count
                    && cmd.is_create()
                    && let Record::Statement {
                        loc,
                        conditions,
                        connection,
                        ..
                    } = &record
                {
                    let vnode_count = (2..=64) // small
                        .chain(224..=288) // normal
                        .chain(992..=1056) // 1024 affects row id gen behavior
                        .choose(&mut thread_rng())
                        .unwrap();
                    let sql = format!("SET STREAMING_MAX_PARALLELISM = {vnode_count};");
                    println!("[RANDOM VNODE COUNT] set: {vnode_count}");
                    let set_random_vnode_count = Record::Statement {
                        loc: loc.clone(),
                        conditions: conditions.clone(),
                        connection: connection.clone(),
                        sql,
                        expected: StatementExpect::Ok,
                    };
                    tester.run_async(set_random_vnode_count).await.unwrap();
                    println!("[RANDOM VNODE COUNT] run: {record}");
                }

                match tester
                    .run_async(record.clone())
                    .timed(|_res, elapsed| {
                        tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                    })
                    .await
                {
                    Ok(_) => continue,
                    Err(e) => panic!("{}", e),
                }
            }

            // For kill enabled.
            tracing::debug!(?cmd, "Running");

            if let SqlCmd::SetBackgroundDdl { enable } = cmd {
                manual_background_ddl_enabled = enable;
                background_ddl_enabled = enable;
            }

            // For each background ddl compatible statement, provide a chance for background_ddl=true.
            if let Record::Statement {
                loc,
                conditions,
                connection,
                ..
            } = &record
                && !manual_background_ddl_enabled
            {
                let enable_random_background_ddl =
                    matches!(cmd, SqlCmd::CreateMaterializedView { .. })
                        && conditions.iter().all(|c| {
                            *c != Condition::SkipIf {
                                label: "madsim".to_owned(),
                            }
                        })
                        && background_ddl_rate > 0.0;
                let background_ddl_setting =
                    enable_random_background_ddl && rng.gen_bool(background_ddl_rate);
                let set_background_ddl = Record::Statement {
                    loc: loc.clone(),
                    conditions: conditions.clone(),
                    connection: connection.clone(),
                    expected: StatementExpect::Ok,
                    sql: format!("SET BACKGROUND_DDL={background_ddl_setting};"),
                };
                tester.run_async(set_background_ddl).await.unwrap();
                tracing::debug!("SET BACKGROUND_DDL={background_ddl_setting};");
                background_ddl_enabled = background_ddl_setting;
            };

            if !cmd.allow_kill() {
                for i in 0usize.. {
                    let delay = Duration::from_secs(1 << i);
                    if let Err(err) = tester
                        .run_async(record.clone())
                        .timed(|_res, elapsed| {
                            tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                        })
                        .await
                    {
                        let err_string = err.to_string();
                        // cluster could be still under recovering if killed before, retry if
                        // meets `no reader for dml in table with id {}`.
                        let allowed_errs = [
                            "no reader for dml in table",
                            "error reading a body from connection: broken pipe",
                            "failed to inject barrier",
                            "get error from control stream",
                            "cluster is under recovering",
                        ];
                        let should_retry = i < MAX_RETRY
                            && allowed_errs
                                .iter()
                                .any(|allowed_err| err_string.contains(allowed_err));
                        if !should_retry {
                            panic!("{}", err);
                        }
                        tracing::error!("failed to run test: {err}\nretry after {delay:?}");
                    } else {
                        break;
                    }
                    tokio::time::sleep(delay).await;
                }
                continue;
            }

            let should_kill = thread_rng().gen_bool(kill_opts.kill_rate as f64);
            // spawn a background task to kill nodes
            let handle = if should_kill {
                let cluster = cluster.clone();
                let opts = kill_opts;
                Some(tokio::spawn(async move {
                    let t = thread_rng().gen_range(Duration::default()..Duration::from_secs(1));
                    tokio::time::sleep(t).await;
                    cluster.kill_node(&opts).await;
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }))
            } else {
                None
            };

            for i in 0usize.. {
                tracing::debug!(iteration = i, "retry count");
                let delay = Duration::from_secs(min(1 << i, 10));
                if i > 0 {
                    tokio::time::sleep(delay).await;
                }
                match tester
                    .run_async(record.clone())
                    .timed(|_res, elapsed| {
                        tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                    })
                    .await
                {
                    Ok(_) => {
                        // For background ddl
                        if let SqlCmd::CreateMaterializedView { ref name } = cmd
                            && background_ddl_enabled
                            && !matches!(
                                record,
                                Record::Statement {
                                    expected: StatementExpect::Error(_),
                                    ..
                                } | Record::Query {
                                    expected: QueryExpect::Error(_),
                                    ..
                                }
                            )
                        {
                            tracing::debug!(iteration = i, name, "Retry for background ddl");
                            match wait_background_mv_finished(name).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        iteration = i,
                                        "Record with background_ddl {:?} finished",
                                        record
                                    );
                                    break;
                                }
                                Err(err) => {
                                    tracing::error!(
                                        iteration = i,
                                        ?err,
                                        "failed to wait for background mv to finish creating"
                                    );
                                    if i >= MAX_RETRY {
                                        panic!("failed to run test after retry {i} times, error={err:#?}");
                                    }
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        match cmd {
                            // allow 'table exists' error when retry CREATE statement
                            SqlCmd::Create {
                                is_create_table_as: false,
                            }
                            | SqlCmd::CreateSink {
                                is_sink_into_table: false,
                            } if i != 0
                                // It should not be a gRPC request to meta error,
                                // otherwise it means that the catalog is not yet populated to fe.
                                && !e.to_string().contains("gRPC request to meta service failed")
                                && e.to_string().contains("exists")
                                && e.to_string().contains("Catalog error") =>
                            {
                                tracing::debug!(?cmd, ?e, "already exists");
                                break;
                            }
                            SqlCmd::CreateMaterializedView { ref name }
                                if i != 0
                                // It should not be a gRPC request to meta error,
                                // otherwise it means that the catalog is not yet populated to fe.
                                && !e.to_string().contains("gRPC request to meta service failed")
                                && e.to_string().contains("exists")
                                && e.to_string().contains("Catalog error") =>
                            {
                                if background_ddl_enabled {
                                    match wait_background_mv_finished(name).await {
                                        Ok(_) => {
                                            tracing::debug!(
                                                iteration = i,
                                                "Record with background_ddl {:?} finished",
                                                record
                                            );
                                            break;
                                        }
                                        Err(err) => {
                                            tracing::error!(
                                                iteration = i,
                                                ?err,
                                                "failed to wait for background mv to finish creating"
                                            );
                                            if i >= MAX_RETRY {
                                                panic!("failed to run test after retry {i} times, error={err:#?}");
                                            }
                                            continue;
                                        }
                                    }
                                }
                                tracing::debug!(?cmd, "already dropped");
                                break;
                            }
                            // allow 'not found' error when retry DROP statement
                            SqlCmd::Drop
                                if i != 0
                                    && e.to_string().contains("not found")
                                    && e.to_string().contains("Catalog error") =>
                            {
                                tracing::debug!(?cmd, "already dropped");
                                break;
                            }

                            // Keep i >= MAX_RETRY for other errors. Since these errors indicate that the MV might not yet be created.
                            _ if i >= MAX_RETRY => {
                                panic!("failed to run test after retry {i} times: {e}")
                            }
                            SqlCmd::CreateMaterializedView { ref name }
                                if i != 0
                                    && e.to_string().contains("table is in creating procedure")
                                    && background_ddl_enabled =>
                            {
                                tracing::debug!(iteration = i, name, "Retry for background ddl");
                                match wait_background_mv_finished(name).await {
                                    Ok(_) => {
                                        tracing::debug!(
                                            iteration = i,
                                            "Record with background_ddl {:?} finished",
                                            record
                                        );
                                        break;
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            iteration = i,
                                            ?err,
                                            "failed to wait for background mv to finish creating"
                                        );
                                        if i >= MAX_RETRY {
                                            panic!("failed to run test after retry {i} times, error={err:#?}");
                                        }
                                        continue;
                                    }
                                }
                            }
                            _ => tracing::error!(
                                iteration = i,
                                "failed to run test: {e}\nretry after {delay:?}"
                            ),
                        }
                    }
                }
            }
            if let SqlCmd::SetBackgroundDdl { enable } = cmd {
                background_ddl_enabled = enable;
            };
            if let Some(handle) = handle {
                handle.await.unwrap();
            }
        }
    }
}

pub async fn run_parallel_slt_task(glob: &str, jobs: usize) -> Result<(), ParallelTestError> {
    let mut tester =
        sqllogictest::Runner::new(|| RisingWave::connect("frontend".into(), "dev".into()));
    tester.add_label("madsim");

    tester
        .run_parallel_async(
            glob,
            vec!["frontend".into()],
            |host, dbname| async move { RisingWave::connect(host, dbname).await.unwrap() },
            jobs,
        )
        .await
        .map_err(|e| panic!("{e}"))
}

/// Replace some strings in kafka.slt and write to a new temp file.
fn hack_kafka_test(path: &Path) -> tempfile::NamedTempFile {
    let content = std::fs::read_to_string(path).expect("failed to read file");
    let simple_avsc_full_path =
        std::fs::canonicalize("src/connector/src/test_data/simple-schema.avsc")
            .expect("failed to get schema path");
    let complex_avsc_full_path =
        std::fs::canonicalize("src/connector/src/test_data/complex-schema.avsc")
            .expect("failed to get schema path");
    let json_schema_full_path =
        std::fs::canonicalize("src/connector/src/test_data/complex-schema.json")
            .expect("failed to get schema path");
    let content = content
        .replace("127.0.0.1:29092", "192.168.11.1:29092")
        .replace("localhost:29092", "192.168.11.1:29092")
        .replace(
            "/risingwave/avro-simple-schema.avsc",
            simple_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/avro-complex-schema.avsc",
            complex_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/json-complex-schema",
            json_schema_full_path.to_str().unwrap(),
        );
    let file = tempfile::NamedTempFile::new().expect("failed to create temp file");
    std::fs::write(file.path(), content).expect("failed to write file");
    println!("created a temp file for kafka test: {:?}", file.path());
    file
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_is_create_table_as() {
        assert!(is_create_table_as("     create     table xx  as select 1;"));
        assert!(!is_create_table_as(
            "     create table xx not  as select 1;"
        ));
        assert!(!is_create_table_as("     create view xx as select 1;"));
    }

    #[test]
    fn test_extract_sql_command() {
        check(
            extract_sql_command("create  table  t as select 1;"),
            expect![[r#"
                Create {
                    is_create_table_as: true,
                }"#]],
        );
        check(
            extract_sql_command("  create table  t (a int);"),
            expect![[r#"
                Create {
                    is_create_table_as: false,
                }"#]],
        );
        check(
            extract_sql_command(" create materialized   view  m_1 as select 1;"),
            expect![[r#"
                CreateMaterializedView {
                    name: "m_1",
                }"#]],
        );
        check(
            extract_sql_command("set background_ddl= true;"),
            expect![[r#"
                SetBackgroundDdl {
                    enable: true,
                }"#]],
        );
        check(
            extract_sql_command("SET BACKGROUND_DDL=true;"),
            expect![[r#"
                SetBackgroundDdl {
                    enable: true,
                }"#]],
        );
        check(
            extract_sql_command("CREATE MATERIALIZED VIEW if not exists m_1 as select 1;"),
            expect![[r#"
                CreateMaterializedView {
                    name: "m_1",
                }"#]],
        )
    }
}
