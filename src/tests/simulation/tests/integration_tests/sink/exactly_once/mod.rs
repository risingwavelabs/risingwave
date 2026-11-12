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

#[cfg(madsim)]
mod utils;

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_simulation::cluster::{Cluster, KillOpts};
use tokio::time::sleep;

use crate::sink::exactly_once::utils::SimulationTestIcebergExactlyOnceSink;
use crate::sink::utils::{
    CREATE_SINK, CREATE_SOURCE, DROP_SINK, DROP_SOURCE, SimulationTestSource,
    start_sink_test_cluster,
};
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

#[tokio::test]
async fn test_exactly_once_sink_basic() -> Result<()> {
    let err_rate_list = vec![0.1, 0.1, 0.1, 0.1, 0.1];
    test_exactly_once_sink_inner(err_rate_list).await
}

#[tokio::test]
async fn test_exactly_once_sink_corner_case() -> Result<()> {
    // If, during a re-commit, a new snapshot_id is generated instead of using the previously persisted snapshot_id, it can lead to duplicate data when consecutive "23" appears in err_events.
    let err_rate_list = vec![0.0, 0.0, 0.3, 0.3, 0.0];
    test_exactly_once_sink_inner(err_rate_list).await
}

async fn test_exactly_once_sink_inner(err_rate_list: Vec<f64>) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestIcebergExactlyOnceSink::register_new_with_err_rate(err_rate_list);
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 20);
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;

    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;

    let count = test_source.id_list.len();

    test_sink.wait_initial_parallelism(6).await?;

    let internal_tables = session.run("show internal tables").await?;

    let table_name_prefix = "public.__internal_test_sink_";

    let sink_internal_table_name: String = TryInto::<[&str; 1]>::try_into(
        internal_tables
            .split("\n")
            .filter(|line| {
                line.contains(table_name_prefix)
                    && line
                        .strip_prefix(table_name_prefix)
                        .unwrap()
                        .contains("sink")
            })
            .collect_vec(),
    )
    .unwrap()[0]
        .to_string();

    let _result = session
        .run(format!(
            "select * from {} limit 10",
            sink_internal_table_name
        ))
        .await?;

    let _result = session
        .run(format!(
            "select * from {} where kv_log_store_vnode = 0 limit 10",
            sink_internal_table_name
        ))
        .await?;

    test_sink
        .store
        .wait_for_count_and_check_duplicate(test_source.id_list.len())
        .await?;

    let result: String = session.run("select * from rw_sink_decouple").await?;
    let [_, is_sink_decouple_str, vnode_count_str] =
        TryInto::<[&str; 3]>::try_into(result.split(" ").collect_vec()).unwrap();
    assert_eq!(is_sink_decouple_str, "t");

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert_eq!(
        source_parallelism,
        test_source.create_stream_count.load(Relaxed)
    );
    println!(
        "total id nums in external sink is {:?}, test_source.id_list len is {:?}",
        test_sink.store.count_total_keys(),
        test_source.id_list.len()
    );
    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    test_sink.store.check_simple_result(&test_source.id_list)?;
    assert!(test_sink.store.checkpoint_count() > 0);

    Ok(())
}

const CREATE_MV: &str = "create materialized view test_mv as select id, name from test_source";
const CREATE_SINK_FROM_MV: &str =
    "create sink test_sink from test_mv with (connector = 'test', type = 'upsert')";
const REPLACE_SINK_FROM_MV: &str =
    "replace sink test_sink from test_mv with (connector = 'test', type = 'upsert')";

/// `REPLACE SINK` on an exactly-once sink while its commits fail now and then, both before and
/// after they reach the external sink.
#[tokio::test]
async fn test_exactly_once_sink_replace() -> Result<()> {
    test_exactly_once_sink_replace_inner(vec![0.0, 0.0, 0.2, 0.2, 0.0], false).await
}

/// `REPLACE SINK` on an exactly-once sink whose commits the external sink refuses, followed by
/// a restart of the meta node while they are still refused. Only the old sink is refused, so
/// the new sink could commit at any time if nothing held it back.
#[tokio::test]
async fn test_exactly_once_sink_replace_while_old_sink_cannot_commit() -> Result<()> {
    test_exactly_once_sink_replace_inner(vec![0.0; 5], true).await
}

async fn test_exactly_once_sink_replace_inner(
    err_rate_list: Vec<f64>,
    old_sink_cannot_commit: bool,
) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let source_parallelism = 6;

    let test_sink = SimulationTestIcebergExactlyOnceSink::register_new_with_err_rate(err_rate_list);
    // Slow enough that the source is still producing when the sink is replaced.
    let test_source = SimulationTestSource::register_new(source_parallelism, 0..100000, 0.2, 5);
    let count = test_source.id_list.len();
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_MV).await?;
    session.run(CREATE_SINK_FROM_MV).await?;
    test_sink.wait_initial_parallelism(6).await?;

    // Let the old sink commit part of the data.
    while test_sink.store.id_count() < count / 10 {
        sleep(Duration::from_secs(1)).await;
    }
    let old_sink_id = sink_id(&mut session).await?;

    if old_sink_cannot_commit {
        test_sink.store.set_commits_refused(old_sink_id, true);
        // Let the old sink acknowledge epochs that it cannot commit.
        sleep(Duration::from_secs(5)).await;
    }
    let count_before_replace = test_sink.store.id_count();
    assert!(count_before_replace < count);

    // The replacement does not wait for the external sink.
    tokio::time::timeout(Duration::from_secs(300), session.run(REPLACE_SINK_FROM_MV))
        .await
        .map_err(|_| anyhow::anyhow!("REPLACE SINK waited for the external sink"))??;
    let new_sink_id = sink_id(&mut session).await?;
    assert!(new_sink_id != old_sink_id);

    if old_sink_cannot_commit {
        // The epochs that the old sink could not commit were kept for the new sink.
        assert!(replaced_sink_pending_epoch_count(&cluster, old_sink_id, new_sink_id).await? > 0);

        // The new sink takes data and its own commits would succeed, but it does not commit
        // ahead of the old sink.
        sleep(Duration::from_secs(10)).await;
        assert_eq!(test_sink.store.id_count(), count_before_replace);

        // The same holds after the meta node restarts.
        cluster.simple_kill_nodes(["meta-1"]).await;
        sleep(Duration::from_secs(5)).await;
        cluster.simple_restart_nodes(["meta-1"]).await;
        sleep(Duration::from_secs(30)).await;
        assert_eq!(test_sink.store.id_count(), count_before_replace);
        assert!(replaced_sink_pending_epoch_count(&cluster, old_sink_id, new_sink_id).await? > 0);

        test_sink.store.set_commits_refused(old_sink_id, false);
    }

    test_sink
        .store
        .wait_for_count_and_check_duplicate(count)
        .await?;
    test_sink.store.check_simple_result(&test_source.id_list)?;

    // Every commit of the old sink reached the external sink before the first commit of the
    // new sink.
    assert_eq!(
        test_sink.store.committed_sink_id_runs(),
        vec![old_sink_id, new_sink_id]
    );
    assert_eq!(
        replaced_sink_pending_epoch_count(&cluster, old_sink_id, new_sink_id).await?,
        0
    );

    let mut session = cluster.start_session();
    run_with_retry(&mut session, DROP_SINK).await?;
    session.run("drop materialized view test_mv").await?;
    session.run(DROP_SOURCE).await?;
    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));

    Ok(())
}

async fn sink_id(session: &mut risingwave_simulation::cluster::Session) -> Result<SinkId> {
    let id = session
        .run("select id from rw_catalog.rw_sinks where name = 'test_sink'")
        .await?;
    Ok(SinkId::from(id.trim().parse::<u32>()?))
}

/// The number of epochs of the replaced sink that still wait for their commit, read from the
/// meta store.
async fn replaced_sink_pending_epoch_count(
    cluster: &Cluster,
    old_sink_id: SinkId,
    new_sink_id: SinkId,
) -> Result<u64> {
    use risingwave_meta_model::replaced_sink_pending_state;
    use sea_orm::{ColumnTrait, Database, EntityTrait, PaginatorTrait, QueryFilter};

    let url = format!("sqlite://{}?mode=ro", cluster.meta_sqlite_path().display());
    let db = Database::connect(&url).await?;
    let count = replaced_sink_pending_state::Entity::find()
        .filter(replaced_sink_pending_state::Column::SinkId.eq(old_sink_id))
        .filter(replaced_sink_pending_state::Column::SuccessorSinkId.eq(new_sink_id))
        .count(&db)
        .await?;
    Ok(count)
}

/// Run a statement that may arrive while the meta node is still coming back.
async fn run_with_retry(
    session: &mut risingwave_simulation::cluster::Session,
    sql: &str,
) -> Result<()> {
    let mut last_err = None;
    for _ in 0..30 {
        match session.run(sql).await {
            Ok(_) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
        sleep(Duration::from_secs(2)).await;
    }
    Err(anyhow::anyhow!(
        "failed to run `{sql}`: {:?}",
        last_err.expect("at least one attempt")
    ))
}
