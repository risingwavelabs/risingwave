use std::collections::VecDeque;
use std::iter::once;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use risingwave_pb::hummock::HummockSnapshot;
use tokio::sync::{oneshot, watch, RwLock};

use super::notifier::Notifier;
use super::{Command, Scheduled};
use crate::hummock::HummockManagerRef;
use crate::manager::META_NODE_ID;
use crate::storage::MetaStore;
use crate::MetaResult;

/// A buffer or queue for scheduling barriers.
///
/// We manually implement one here instead of using channels since we may need to update the front
/// of the queue to add some notifiers for instant flushes.
struct Inner {
    buffer: RwLock<VecDeque<Scheduled>>,

    /// When `buffer` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,
}

#[derive(Clone)]
pub struct BarrierScheduler<S: MetaStore> {
    inner: Arc<Inner>,

    hummock_manager: HummockManagerRef<S>,
}

impl<S: MetaStore> BarrierScheduler<S> {
    pub fn new(hummock_manager: HummockManagerRef<S>) -> (Self, ScheduledBarriers) {
        let inner = Arc::new(Inner {
            buffer: RwLock::new(VecDeque::new()),
            changed_tx: watch::channel(()).0,
        });

        (
            Self {
                inner: inner.clone(),
                hummock_manager,
            },
            ScheduledBarriers { inner },
        )
    }

    /// Push a scheduled barrier into the buffer.
    async fn push(&self, scheduleds: impl IntoIterator<Item = Scheduled>) {
        let mut buffer = self.inner.buffer.write().await;
        for scheduled in scheduleds {
            buffer.push_back(scheduled);
            if buffer.len() == 1 {
                self.inner.changed_tx.send(()).ok();
            }
        }
    }

    /// Attach `new_notifiers` to the very first scheduled barrier. If there's no one scheduled, a
    /// default checkpoint barrier will be created.
    async fn attach_notifiers(&self, new_notifiers: impl IntoIterator<Item = Notifier>) {
        let mut buffer = self.inner.buffer.write().await;
        match buffer.front_mut() {
            Some((_, notifiers)) => notifiers.extend(new_notifiers),
            None => {
                // If no command scheduled, create periodic checkpoint barrier by default.
                buffer.push_back((Command::checkpoint(), new_notifiers.into_iter().collect()));
                if buffer.len() == 1 {
                    self.inner.changed_tx.send(()).ok();
                }
            }
        }
    }

    /// Wait for the next barrier to collect. Note that the barrier flowing in our stream graph is
    /// ignored, if exists.
    async fn wait_for_next_barrier_to_collect(&self) -> MetaResult<()> {
        let (tx, rx) = oneshot::channel();
        let notifier = Notifier {
            collected: Some(tx),
            ..Default::default()
        };
        self.attach_notifiers(once(notifier)).await;
        rx.await.unwrap()
    }

    /// Run multiple commands and return when they're all completely finished. It's ensured that
    /// multiple commands is executed continuously and atomically.
    pub async fn run_multiple_commands(&self, commands: Vec<Command>) -> MetaResult<()> {
        struct Context {
            collect_rx: oneshot::Receiver<MetaResult<()>>,
            finish_rx: oneshot::Receiver<()>,
            is_create_mv: bool,
        }

        let mut contexts = Vec::with_capacity(commands.len());
        let mut scheduleds = Vec::with_capacity(commands.len());

        for command in commands {
            let (collect_tx, collect_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = oneshot::channel();
            let is_create_mv = matches!(command, Command::CreateMaterializedView { .. });

            contexts.push(Context {
                collect_rx,
                finish_rx,
                is_create_mv,
            });
            scheduleds.push((
                command,
                once(Notifier {
                    collected: Some(collect_tx),
                    finished: Some(finish_tx),
                    ..Default::default()
                })
                .collect(),
            ));
        }

        self.push(scheduleds).await;

        for Context {
            collect_rx,
            finish_rx,
            is_create_mv,
        } in contexts
        {
            collect_rx.await.unwrap()?; // Throw the error if it occurs when collecting this barrier.

            // TODO: refactor this
            if is_create_mv {
                // The snapshot ingestion may last for several epochs, we should pin the epoch here.
                // TODO: this should be done in `post_collect`
                let _snapshot = self.hummock_manager.pin_snapshot(META_NODE_ID).await?;
                finish_rx.await.unwrap(); // Wait for this command to be finished.
                self.hummock_manager.unpin_snapshot(META_NODE_ID).await?;
            } else {
                finish_rx.await.unwrap(); // Wait for this command to be finished.
            }
        }

        Ok(())
    }

    /// Run a command and return when it's completely finished.
    pub async fn run_command(&self, command: Command) -> MetaResult<()> {
        self.run_multiple_commands(vec![command]).await
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self) -> MetaResult<HummockSnapshot> {
        let start = Instant::now();

        tracing::debug!("start barrier flush");
        self.wait_for_next_barrier_to_collect().await?;

        let elapsed = Instant::now().duration_since(start);
        tracing::debug!("barrier flushed in {:?}", elapsed);

        let snapshot = self.hummock_manager.get_last_epoch()?;
        Ok(snapshot)
    }
}

/// A buffer or queue for scheduling barriers.
///
/// We manually implement one here instead of using channels since we may need to update the front
/// of the queue to add some notifiers for instant flushes.
pub struct ScheduledBarriers {
    inner: Arc<Inner>,
}

impl ScheduledBarriers {
    /// Pop a scheduled barrier from the buffer, or a default checkpoint barrier if not exists.
    pub(super) async fn pop_or_default(&self) -> Scheduled {
        let mut buffer = self.inner.buffer.write().await;

        // If no command scheduled, create periodic checkpoint barrier by default.
        buffer
            .pop_front()
            .unwrap_or_else(|| (Command::checkpoint(), Default::default()))
    }

    /// Wait for at least one scheduled barrier in the buffer.
    pub(super) async fn wait_one(&self) {
        let buffer = self.inner.buffer.read().await;
        if buffer.len() > 0 {
            return;
        }
        let mut rx = self.inner.changed_tx.subscribe();
        drop(buffer);

        rx.changed().await.unwrap();
    }

    /// Clear all buffered scheduled barriers, and notify their subscribers with failed as aborted.
    pub(super) async fn abort(&self) {
        let mut buffer = self.inner.buffer.write().await;
        while let Some((_, notifiers)) = buffer.pop_front() {
            notifiers.into_iter().for_each(|notify| {
                notify.notify_collection_failed(anyhow!("Scheduled barrier abort.").into())
            })
        }
    }
}
