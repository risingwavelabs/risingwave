use std::collections::HashSet;
use std::iter::once;

use tokio::sync::oneshot;

use crate::executor::Barrier;

pub(super) enum ManagedBarrierState {
    Pending {
        last_epoch: Option<u64>,
    },

    Stashed {
        epoch: u64,

        collected_actors: HashSet<u32>,
    },

    Issued {
        epoch: u64,

        remaining_actors: HashSet<u32>,

        collect_notifier: oneshot::Sender<()>,
    },
}

impl ManagedBarrierState {
    fn may_notify(&mut self) {
        let (epoch, to_notify) = match self {
            ManagedBarrierState::Issued {
                epoch,
                remaining_actors,
                ..
            } => (*epoch, remaining_actors.is_empty()),

            _ => unreachable!(),
        };

        if to_notify {
            let state = std::mem::replace(
                self,
                Self::Pending {
                    last_epoch: Some(epoch),
                },
            );

            match state {
                ManagedBarrierState::Issued {
                    collect_notifier, ..
                } => {
                    if collect_notifier.send(()).is_err() {
                        warn!("failed to notify barrier collection: rx is dropped")
                    }
                }

                _ => unreachable!(),
            }
        }
    }

    pub(super) fn collect(&mut self, actor_id: u32, barrier: &Barrier) {
        match self {
            ManagedBarrierState::Pending { last_epoch } => {
                if let Some(last_epoch) = *last_epoch {
                    assert_eq!(barrier.epoch.prev, last_epoch)
                }

                *self = Self::Stashed {
                    epoch: barrier.epoch.curr,
                    collected_actors: once(actor_id).collect(),
                }
            }

            ManagedBarrierState::Stashed {
                epoch,
                collected_actors,
            } => {
                assert_eq!(barrier.epoch.curr, *epoch);

                let new = collected_actors.insert(actor_id);
                assert!(new);
            }

            ManagedBarrierState::Issued {
                epoch,
                remaining_actors,
                ..
            } => {
                assert_eq!(barrier.epoch.curr, *epoch);

                let exist = remaining_actors.remove(&actor_id);
                assert!(exist);
                self.may_notify();
            }
        }
    }

    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: impl IntoIterator<Item = u32>,
        collect_notifier: oneshot::Sender<()>,
    ) {
        match self {
            ManagedBarrierState::Pending { .. } => {
                let remaining_actors = actor_ids_to_collect.into_iter().collect();

                *self = Self::Issued {
                    epoch: barrier.epoch.curr,
                    remaining_actors,
                    collect_notifier,
                };
                self.may_notify();
            }

            ManagedBarrierState::Stashed {
                epoch,
                collected_actors,
            } => {
                assert_eq!(barrier.epoch.curr, *epoch);

                let remaining_actors = actor_ids_to_collect
                    .into_iter()
                    .filter(|a| collected_actors.contains(a))
                    .collect();

                *self = Self::Issued {
                    epoch: barrier.epoch.curr,
                    remaining_actors,
                    collect_notifier,
                };
                self.may_notify();
            }

            ManagedBarrierState::Issued { .. } => panic!("barrier state has already been `Issued`"),
        }
    }
}
