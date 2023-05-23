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

use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use itertools::Itertools;
use madsim::rand::thread_rng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::Rng;
use risingwave_common::hash::ParallelUnitId;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::PbFragment;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::stream_plan::StreamNode;

use self::predicate::BoxedPredicate;
use crate::cluster::Cluster;

/// Predicates used for locating fragments.
pub mod predicate {
    use risingwave_pb::stream_plan::stream_node::NodeBody;
    use risingwave_pb::stream_plan::DispatcherType;

    use super::*;

    trait Predicate = Fn(&PbFragment) -> bool + Send + 'static;
    pub type BoxedPredicate = Box<dyn Predicate>;

    fn root(fragment: &PbFragment) -> &StreamNode {
        fragment.actors.first().unwrap().nodes.as_ref().unwrap()
    }

    fn count(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> usize {
        let child = root.input.iter().map(|n| count(n, p)).sum::<usize>();
        child + if p(root) { 1 } else { 0 }
    }

    fn any(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> bool {
        p(root) || root.input.iter().any(|n| any(n, p))
    }

    fn all(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> bool {
        p(root) && root.input.iter().all(|n| all(n, p))
    }

    /// There're exactly `n` operators whose identity contains `s` in the fragment.
    pub fn identity_contains_n(n: usize, s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &PbFragment| {
            count(root(f), &|n| {
                n.identity.to_lowercase().contains(&s.to_lowercase())
            }) == n
        };
        Box::new(p)
    }

    /// There exists operators whose identity contains `s` in the fragment.
    pub fn identity_contains(s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &PbFragment| {
            any(root(f), &|n| {
                n.identity.to_lowercase().contains(&s.to_lowercase())
            })
        };
        Box::new(p)
    }

    /// There does not exist any operator whose identity contains `s` in the fragment.
    pub fn no_identity_contains(s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &PbFragment| {
            all(root(f), &|n| {
                !n.identity.to_lowercase().contains(&s.to_lowercase())
            })
        };
        Box::new(p)
    }

    /// There're `n` upstream fragments of the fragment.
    pub fn upstream_fragment_count(n: usize) -> BoxedPredicate {
        let p = move |f: &PbFragment| f.upstream_fragment_ids.len() == n;
        Box::new(p)
    }

    /// The fragment is able to be rescheduled. Used for locating random fragment.
    pub fn can_reschedule() -> BoxedPredicate {
        let p = |f: &PbFragment| {
            // The rescheduling of no-shuffle downstreams must be derived from the most upstream
            // fragment. So if a fragment has no-shuffle upstreams, it cannot be rescheduled.
            !any(root(f), &|n| {
                let Some(NodeBody::Merge(merge)) = &n.node_body else { return false };
                merge.upstream_dispatcher_type() == DispatcherType::NoShuffle
            })
        };
        Box::new(p)
    }

    /// The fragment with the given id.
    pub fn id(id: u32) -> BoxedPredicate {
        let p = move |f: &PbFragment| f.fragment_id == id;
        Box::new(p)
    }
}

#[derive(Debug)]
pub struct Fragment {
    pub inner: risingwave_pb::meta::table_fragments::Fragment,

    r: Arc<GetClusterInfoResponse>,
}

impl Fragment {
    /// The fragment id.
    pub fn id(&self) -> u32 {
        self.inner.fragment_id
    }

    /// Generate a reschedule plan for the fragment.
    pub fn reschedule(
        &self,
        remove: impl AsRef<[ParallelUnitId]>,
        add: impl AsRef<[ParallelUnitId]>,
    ) -> String {
        let remove = remove.as_ref();
        let add = add.as_ref();

        let mut f = String::new();
        write!(f, "{}", self.id()).unwrap();
        if !remove.is_empty() {
            write!(f, " -{:?}", remove).unwrap();
        }
        if !add.is_empty() {
            write!(f, " +{:?}", add).unwrap();
        }
        f
    }

    /// Generate a random reschedule plan for the fragment.
    ///
    /// Consumes `self` as the actor info will be stale after rescheduling.
    pub fn random_reschedule(self) -> String {
        let (all_parallel_units, current_parallel_units) = self.parallel_unit_usage();

        let rng = &mut thread_rng();
        let target_parallel_unit_count = match self.inner.distribution_type() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => 1,
            FragmentDistributionType::Hash => rng.gen_range(1..=all_parallel_units.len()),
        };
        let target_parallel_units: HashSet<_> = all_parallel_units
            .choose_multiple(rng, target_parallel_unit_count)
            .copied()
            .collect();

        let remove = current_parallel_units
            .difference(&target_parallel_units)
            .copied()
            .collect_vec();
        let add = target_parallel_units
            .difference(&current_parallel_units)
            .copied()
            .collect_vec();

        self.reschedule(remove, add)
    }

    pub fn parallel_unit_usage(&self) -> (Vec<ParallelUnitId>, HashSet<ParallelUnitId>) {
        let actor_to_parallel_unit: HashMap<_, _> = self
            .r
            .table_fragments
            .iter()
            .flat_map(|tf| {
                tf.actor_status.iter().map(|(&actor_id, status)| {
                    (
                        actor_id,
                        status.get_parallel_unit().unwrap().id as ParallelUnitId,
                    )
                })
            })
            .collect();

        let all_parallel_units = self
            .r
            .worker_nodes
            .iter()
            .flat_map(|n| n.parallel_units.iter())
            .map(|p| p.id as ParallelUnitId)
            .collect_vec();
        let current_parallel_units: HashSet<_> = self
            .inner
            .actors
            .iter()
            .map(|a| actor_to_parallel_unit[&a.actor_id] as ParallelUnitId)
            .collect();

        (all_parallel_units, current_parallel_units)
    }
}

impl Cluster {
    /// Locate fragments that satisfy all the predicates.
    pub async fn locate_fragments(
        &mut self,
        predicates: impl IntoIterator<Item = BoxedPredicate>,
    ) -> Result<Vec<Fragment>> {
        let predicates = predicates.into_iter().collect_vec();

        let fragments = self
            .ctl
            .spawn(async move {
                let r: Arc<_> = risingwave_ctl::cmd_impl::meta::get_cluster_info(
                    &risingwave_ctl::common::CtlContext::default(),
                )
                .await?
                .into();

                let mut results = vec![];
                for tf in &r.table_fragments {
                    for f in tf.fragments.values() {
                        let selected = predicates.iter().all(|p| p(f));
                        if selected {
                            results.push(Fragment {
                                inner: f.clone(),
                                r: r.clone(),
                            });
                        }
                    }
                }

                Ok::<_, anyhow::Error>(results)
            })
            .await??;

        Ok(fragments)
    }

    /// Locate exactly one fragment that satisfies all the predicates.
    pub async fn locate_one_fragment(
        &mut self,
        predicates: impl IntoIterator<Item = BoxedPredicate>,
    ) -> Result<Fragment> {
        let [fragment]: [_; 1] = self
            .locate_fragments(predicates)
            .await?
            .try_into()
            .map_err(|fs| anyhow!("not exactly one fragment: {fs:#?}"))?;
        Ok(fragment)
    }

    /// Locate a random fragment that is reschedulable.
    pub async fn locate_random_fragment(&mut self) -> Result<Fragment> {
        self.locate_fragments([predicate::can_reschedule()])
            .await?
            .into_iter()
            .choose(&mut thread_rng())
            .ok_or_else(|| anyhow!("no reschedulable fragment"))
    }

    /// Locate some random fragments that are reschedulable.
    pub async fn locate_random_fragments(&mut self) -> Result<Vec<Fragment>> {
        let fragments = self.locate_fragments([predicate::can_reschedule()]).await?;
        let len = thread_rng().gen_range(1..=fragments.len());
        let selected = fragments
            .into_iter()
            .choose_multiple(&mut thread_rng(), len);
        Ok(selected)
    }

    /// Locate a fragment with the given id.
    pub async fn locate_fragment_by_id(&mut self, id: u32) -> Result<Fragment> {
        self.locate_one_fragment([predicate::id(id)]).await
    }

    /// retrieve cluster info from meta node.
    pub async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        self.ctl
            .spawn(async move {
                risingwave_ctl::cmd_impl::meta::get_cluster_info(
                    &risingwave_ctl::common::CtlContext::default(),
                )
                .await
            })
            .await?
    }

    /// mark node running at addr for deletion. Node will no longer accept new fragments
    pub async fn unregister_worker_node(&self, addr: HostAddr) -> Result<()> {
        let _ = self
            .ctl
            .spawn(async move {
                risingwave_ctl::cmd_impl::meta::unregister_worker_node(
                    &risingwave_ctl::common::CtlContext::default(),
                    addr,
                )
                .await
            })
            .await?;
        Ok(())
    }

    /// Reschedule with the given `plan`. Check the document of
    /// [`risingwave_ctl::cmd_impl::meta::reschedule`] for more details.
    pub async fn reschedule(&mut self, plan: impl Into<String>) -> Result<()> {
        let plan = plan.into();

        self.ctl
            .spawn(async move {
                let opts = risingwave_ctl::CliOpts::parse_from([
                    "ctl",
                    "meta",
                    "reschedule",
                    "--plan",
                    plan.as_ref(),
                ]);
                risingwave_ctl::start(opts).await
            })
            .await??;

        Ok(())
    }
}
