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

use anyhow::Result;
use risingwave_common::hash::VirtualNode;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;

#[tokio::test]
async fn test_fragment_parallelism_can_exceed_physical_cores() -> Result<()> {
    let configuration = Configuration::for_scale();
    let total_cores = configuration.total_streaming_cores() as usize;
    let mut cluster = Cluster::start(configuration).await?;
    let max_parallelism = VirtualNode::COUNT_FOR_COMPAT;

    // Expect creating a job with parallelism larger than max_parallelism to be rejected.
    let mut session = cluster.start_session();

    session
        .run(format!(
            "set streaming_parallelism = {}",
            max_parallelism + 1
        ))
        .await?;

    assert!(session.run("create table t (v int);").await.is_err());

    let new_parallelism = total_cores + 1;

    session
        .run(format!("set streaming_parallelism = {new_parallelism}"))
        .await?;

    // With a value within max_parallelism, creation succeeds; we will bump further afterward.
    session.run("create table t (v int);").await?;

    let fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    let fragment_id = fragment.id();
    assert_eq!(fragment.inner.actors.len(), new_parallelism);

    let new_parallelism = total_cores + 2;

    // Raise table parallelism beyond physical cores; fragment actor count should follow.
    session
        .run(&format!(
            "alter table t set parallelism = {new_parallelism};"
        ))
        .await?;

    let updated_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    assert_eq!(updated_fragment.inner.actors.len(), new_parallelism);

    let new_parallelism = total_cores + 3;

    // Alter fragment directly to an even higher parallelism and verify actor count.
    session
        .run(&format!(
            "alter fragment {fragment_id} set parallelism = {new_parallelism};"
        ))
        .await?;

    let updated_fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;
    assert_eq!(updated_fragment.inner.actors.len(), new_parallelism);

    Ok(())
}
