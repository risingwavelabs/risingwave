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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rand::{thread_rng, Rng};
use sqllogictest::ParallelTestError;

use crate::client::RisingWave;
use crate::cluster::{Cluster, KillOpts};

fn is_create_table_as(sql: &str) -> bool {
    let parts: Vec<String> = sql
        .trim_start()
        .split_whitespace()
        .map(|s| s.to_lowercase())
        .collect();

    parts.len() >= 4 && parts[0] == "create" && parts[1] == "table" && parts[3] == "as"
}

#[derive(PartialEq, Eq)]
enum SqlCmd {
    Create { is_create_table_as: bool },
    Drop,
    Dml,
    Flush,
    Others,
}

impl SqlCmd {
    // We won't kill during insert/update/delete since the atomicity is not guaranteed.
    // Notice that `create table as` is also not atomic in our system.
    fn ignore_kill(&self) -> bool {
        matches!(
            self,
            SqlCmd::Dml
                | SqlCmd::Create {
                    is_create_table_as: true
                }
        )
    }
}

fn extract_sql_command(sql: &str) -> SqlCmd {
    let cmd = sql
        .trim_start()
        .split_once(' ')
        .unwrap_or_default()
        .0
        .to_lowercase();
    match cmd.as_str() {
        "create" => SqlCmd::Create {
            is_create_table_as: is_create_table_as(sql),
        },
        "drop" => SqlCmd::Drop,
        "insert" | "update" | "delete" => SqlCmd::Dml,
        "flush" => SqlCmd::Flush,
        _ => SqlCmd::Others,
    }
}

const KILL_IGNORE_FILES: &[&str] = &[
    // TPCH queries are too slow for recovery.
    "tpch_snapshot.slt",
    "tpch_upstream.slt",
    // We already have visibility_all cases.
    "visibility_checkpoint.slt",
    // This depends on session config.
    "session_timezone.slt",
];

/// Run the sqllogictest files in `glob`.
pub async fn run_slt_task(cluster: Arc<Cluster>, glob: &str, opts: &KillOpts) {
    let kill = opts.kill_compute || opts.kill_meta || opts.kill_frontend || opts.kill_compactor;
    let files = glob::glob(glob).expect("failed to read glob pattern");
    for file in files {
        // use a session per file
        let risingwave = RisingWave::connect("frontend".into(), "dev".into())
            .await
            .unwrap();
        let mut tester = sqllogictest::Runner::new(risingwave);

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
        for record in sqllogictest::parse_file(path).expect("failed to parse file") {
            if let sqllogictest::Record::Halt { .. } = record {
                break;
            }

            // For normal records.
            if !kill {
                match tester.run_async(record).await {
                    Ok(_) => continue,
                    Err(e) => panic!("{}", e),
                }
            }

            // For kill enabled.
            let cmd = match &record {
                sqllogictest::Record::Statement { sql, .. }
                | sqllogictest::Record::Query { sql, .. } => extract_sql_command(sql),
                _ => SqlCmd::Others,
            };

            if cmd.ignore_kill() {
                for i in 0usize.. {
                    let delay = Duration::from_secs(1 << i);
                    if let Err(err) = tester.run_async(record.clone()).await {
                        // cluster could be still under recovering if killed before, retry if
                        // meets `no reader for dml in table with id {}`.
                        let should_retry =
                            err.to_string().contains("no reader for dml in table") && i < 5;
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

            let should_kill = thread_rng().gen_ratio((opts.kill_rate * 1000.0) as u32, 1000);
            // spawn a background task to kill nodes
            let handle = if should_kill {
                let cluster = cluster.clone();
                let opts = *opts;
                Some(tokio::spawn(async move {
                    let t = thread_rng().gen_range(Duration::default()..Duration::from_secs(1));
                    tokio::time::sleep(t).await;
                    cluster.kill_node(&opts).await;
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }))
            } else {
                None
            };
            // retry up to 5 times until it succeed
            for i in 0usize.. {
                let delay = Duration::from_secs(1 << i);
                match tester.run_async(record.clone()).await {
                    Ok(_) => break,
                    // allow 'table exists' error when retry CREATE statement
                    Err(e)
                        if matches!(
                            cmd,
                            SqlCmd::Create {
                                is_create_table_as: false
                            }
                        ) && i != 0
                            && e.to_string().contains("exists")
                            && e.to_string().contains("Catalog error") =>
                    {
                        break
                    }
                    // allow 'not found' error when retry DROP statement
                    Err(e)
                        if cmd == SqlCmd::Drop
                            && i != 0
                            && e.to_string().contains("not found")
                            && e.to_string().contains("Catalog error") =>
                    {
                        break
                    }
                    Err(e) if i >= 5 => panic!("failed to run test after retry {i} times: {e}"),
                    Err(e) => tracing::error!("failed to run test: {e}\nretry after {delay:?}"),
                }
                tokio::time::sleep(delay).await;
            }
            if let Some(handle) = handle {
                handle.await.unwrap();
            }
        }
    }
}

pub async fn run_parallel_slt_task(glob: &str, jobs: usize) -> Result<(), ParallelTestError> {
    let db = RisingWave::connect("frontend".into(), "dev".into())
        .await
        .unwrap();
    let mut tester = sqllogictest::Runner::new(db);
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
    let proto_full_path = std::fs::canonicalize("src/connector/src/test_data/complex-schema")
        .expect("failed to get schema path");
    let content = content
        .replace("127.0.0.1:29092", "192.168.11.1:29092")
        .replace(
            "/risingwave/avro-simple-schema.avsc",
            simple_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/avro-complex-schema.avsc",
            complex_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/proto-complex-schema",
            proto_full_path.to_str().unwrap(),
        );
    let file = tempfile::NamedTempFile::new().expect("failed to create temp file");
    std::fs::write(file.path(), content).expect("failed to write file");
    println!("created a temp file for kafka test: {:?}", file.path());
    file
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_create_table_as() {
        assert!(is_create_table_as("     create     table xx  as select 1;"));
        assert!(!is_create_table_as(
            "     create table xx not  as select 1;"
        ));
        assert!(!is_create_table_as("     create view xx as select 1;"));
    }
}
