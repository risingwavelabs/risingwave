use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use itertools::Itertools;
use risingwave_pb::meta::table_fragments::Fragment as ProstFragment;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::stream_plan::StreamNode;

use self::predicate::BoxedPredicate;
use crate::cluster::Cluster;

/// Predicates used for locating fragments.
pub mod predicate {
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;

    use super::*;

    trait Predicate = Fn(&ProstFragment) -> bool + Send + 'static;
    pub type BoxedPredicate = Box<dyn Predicate>;

    fn root(fragment: &ProstFragment) -> &StreamNode {
        fragment.actors.first().unwrap().nodes.as_ref().unwrap()
    }

    fn count(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> usize {
        let child = root.input.iter().map(|n| count(n, p)).sum::<usize>();
        child + if p(root) { 1 } else { 0 }
    }

    fn any(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> bool {
        p(root) || root.input.iter().any(|n| any(n, p))
    }

    /// There're exactly `n` operators whose identity contains `s` in the fragment.
    pub fn identity_contains_n(n: usize, s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &ProstFragment| {
            count(root(f), &|n| n.identity.to_lowercase().contains(&s)) == n
        };
        Box::new(p)
    }

    /// There exists operators whose identity contains `s` in the fragment.
    pub fn identity_contains(s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &ProstFragment| any(root(f), &|n| n.identity.to_lowercase().contains(&s));
        Box::new(p)
    }

    /// There're `n` upstream fragments of the fragment.
    pub fn upstream_fragment_count(n: usize) -> BoxedPredicate {
        let p = move |f: &ProstFragment| f.upstream_fragment_ids.len() == n;
        Box::new(p)
    }

    /// The fragment is able to be scaled. Used for locating random fragment.
    pub fn can_scale() -> BoxedPredicate {
        let p = |f: &ProstFragment| {
            // TODO: singleton fragments are also able to be migrated, may add a `can_migrate`
            // predicate.
            let distributed = f.distribution_type() == FragmentDistributionType::Hash;

            // TODO: remove below after we support scaling them.
            let has_downstream_mv = identity_contains("materialize")(f)
                && !f.actors.first().unwrap().dispatcher.is_empty();
            let has_source = identity_contains("source")(f);
            let has_chain = identity_contains("chain")(f);

            distributed && !(has_downstream_mv || has_source || has_chain)
        };
        Box::new(p)
    }
}

#[derive(Debug)]
pub struct Fragment {
    pub inner: risingwave_pb::meta::table_fragments::Fragment,

    // TODO: generate random valid plan based on the complete cluster info.
    #[expect(dead_code)]
    r: Arc<GetClusterInfoResponse>,
}

impl Fragment {
    /// The fragment id.
    pub fn id(&self) -> u32 {
        self.inner.fragment_id
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
                let r: Arc<_> = risingwave_ctl::cmd_impl::meta::get_cluster_info()
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
            .map_err(|fs| anyhow!("not exactly one fragment: {fs:?}"))?;
        Ok(fragment)
    }

    /// Reschedule with the given `plan`. Check the document of
    /// [`risingwave_ctl::cmd_impl::meta::reschedule`] for more details.
    pub async fn reschedule(&mut self, plan: impl Into<String>) -> Result<()> {
        let plan: String = plan.into();
        self.ctl
            .spawn(async move {
                let opts = risingwave_ctl::CliOpts::parse_from([
                    "ctl",
                    "meta",
                    "reschedule",
                    "--plan",
                    &plan,
                ]);
                risingwave_ctl::start(opts).await
            })
            .await??;

        Ok(())
    }
}
