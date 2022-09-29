use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use itertools::Itertools;
use risingwave_pb::meta::table_fragments::Fragment as ProstFragment;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::stream_plan::StreamNode;

use self::predicates::BoxedPredicate;
use crate::cluster::Cluster;

pub mod predicates {
    use super::*;

    trait Predicate = Fn(&ProstFragment) -> bool + Send + 'static;
    pub type BoxedPredicate = Box<dyn Predicate>;

    fn root(fragment: &ProstFragment) -> &StreamNode {
        fragment.actors.first().unwrap().nodes.as_ref().unwrap()
    }

    fn count(root: &StreamNode, p: impl Fn(&StreamNode) -> bool) -> usize {
        let child = root.input.iter().filter(|n| p(n)).count();
        child + if p(root) { 1 } else { 0 }
    }

    fn any(root: &StreamNode, p: impl Fn(&StreamNode) -> bool) -> bool {
        p(root) || root.input.iter().any(p)
    }

    pub fn identity_contains_n(n: usize, s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &ProstFragment| {
            count(root(f), |n| n.identity.to_lowercase().contains(&s)) == n
        };
        Box::new(p)
    }

    pub fn identity_contains(s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &ProstFragment| any(root(f), |n| n.identity.to_lowercase().contains(&s));
        Box::new(p)
    }
}

#[derive(Debug)]
pub struct Fragment {
    pub inner: risingwave_pb::meta::table_fragments::Fragment,

    #[expect(dead_code)]
    r: Arc<GetClusterInfoResponse>,
}

impl Fragment {
    pub fn id(&self) -> u32 {
        self.inner.fragment_id
    }
}

impl Cluster {
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

    pub async fn locate_one_fragment(
        &mut self,
        predicates: impl IntoIterator<Item = BoxedPredicate>,
    ) -> Result<Fragment> {
        let [fragment]: [_; 1] = self
            .locate_fragments(predicates)
            .await?
            .try_into()
            .unwrap_or_else(|fs| panic!("not exactly one fragment: {fs:?}"));
        Ok(fragment)
    }

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
