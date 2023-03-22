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

#![cfg(madsim)]

use anyhow::Result;
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;

#[madsim::test]
async fn test_delta_join() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("set rw_implicit_flush = true;").await?;
    session
        .run("set rw_streaming_enable_delta_join = true;")
        .await?;

    session
        .run("create table a (a1 int primary key, a2 int);")
        .await?;
    session
        .run("create table b (b1 int primary key, b2 int);")
        .await?;
    let [t1, t2]: [_; 2] = cluster
        .locate_fragments([identity_contains("materialize")])
        .await?
        .try_into()
        .unwrap();

    session
        .run("create materialized view v as select * from a join b on a.a1 = b.b1;")
        .await?;
    let lookup_fragments = cluster
        .locate_fragments([identity_contains("lookup")])
        .await?;
    assert_eq!(lookup_fragments.len(), 2, "failed to plan delta join");
    let union_fragment = cluster
        .locate_one_fragment([identity_contains("union")])
        .await?;

    let mut test_times = 0;
    macro_rules! test_works {
        () => {
            let keys = || (0..100).map(|i| test_times * 100 + i);

            for key in keys() {
                session
                    .run(format!("insert into a values ({key}, 233)"))
                    .await?;
                session
                    .run(format!("insert into b values ({key}, 666)"))
                    .await?;
            }
            session.run("flush").await?;

            let result = keys()
                .rev()
                .map(|key| format!("{key} 233 {key} 666"))
                .join("\n");

            session
                .run("select * from v order by a1 desc limit 100;")
                .await?
                .assert_result_eq(result);

            #[allow(unused_assignments)]
            test_times += 1;
        };
    }

    test_works!();

    // Scale-in one side
    cluster.reschedule(format!("{}-[0]", t1.id())).await?;
    test_works!();

    // Scale-in both sides together
    cluster
        .reschedule(format!("{}-[2];{}-[0,2]", t1.id(), t2.id()))
        .await?;
    test_works!();

    // Scale-out one side
    cluster.reschedule(format!("{}+[0]", t2.id())).await?;
    test_works!();

    // Scale-out both sides together
    cluster
        .reschedule(format!("{}+[0,2];{}+[2]", t1.id(), t2.id()))
        .await?;
    test_works!();

    // Scale-in join with union
    cluster
        .reschedule(format!("{}-[5];{}-[5]", t1.id(), union_fragment.id()))
        .await?;
    test_works!();

    let result = cluster
        .reschedule(format!("{}-[0]", lookup_fragments[0].id()))
        .await;
    assert!(
        result.is_err(),
        "directly scale-in lookup (downstream) should fail"
    );

    Ok(())
}
