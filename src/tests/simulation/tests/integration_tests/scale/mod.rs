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

mod adaptive_strategy;
mod alter_fragment;
mod auto_parallelism;
mod backfill_parallelism;
mod background_ddl;
mod cascade_materialized_view;
mod dynamic_filter;
mod isolation;
mod nexmark_chaos;
mod nexmark_q4;
mod nexmark_source;
mod no_shuffle;
mod parallelism_exceeds_cores;
mod resource_group;
mod schedulability;
mod shared_source;
mod singleton_migration;
mod sink;
mod streaming_parallelism;
mod table;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};

const SET_TABLE_STRATEGY_DEFAULT: &str = "set streaming_parallelism_strategy_for_table = 'DEFAULT'";
const SET_SOURCE_STRATEGY_DEFAULT: &str =
    "set streaming_parallelism_strategy_for_source = 'DEFAULT'";
const SET_MV_STRATEGY_DEFAULT: &str =
    "set streaming_parallelism_strategy_for_materialized_view = 'DEFAULT'";

fn with_per_session_query(mut config: Configuration, query: &str) -> Configuration {
    let mut per_session_queries = (*config.per_session_queries).clone();
    per_session_queries.push(query.to_string());
    config.per_session_queries = per_session_queries.into();
    config
}

pub fn with_default_table_strategy(config: Configuration) -> Configuration {
    with_per_session_query(config, SET_TABLE_STRATEGY_DEFAULT)
}

pub async fn start_scale_session(cluster: &mut Cluster) -> Result<Session> {
    let mut session = cluster.start_session();
    session.run(SET_TABLE_STRATEGY_DEFAULT).await?;
    Ok(session)
}

pub async fn start_scale_session_with_source_default(cluster: &mut Cluster) -> Result<Session> {
    let mut session = start_scale_session(cluster).await?;
    session.run(SET_SOURCE_STRATEGY_DEFAULT).await?;
    Ok(session)
}

pub async fn set_default_materialized_view_strategy(session: &mut Session) -> Result<()> {
    session.run(SET_MV_STRATEGY_DEFAULT).await?;
    Ok(())
}
