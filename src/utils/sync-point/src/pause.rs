// Copyright 2026 RisingWave Labs
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

use std::cell::RefCell;
use std::future::Future;

use tokio::sync::oneshot;

use crate::SyncPoint;

tokio::task_local! {
    static PAUSE: RefCell<Option<Pause>>;
}

/// Pauses a scoped future once at the named sync point.
pub struct Pause {
    point: SyncPoint,
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl Pause {
    /// Returns the pause and the handles used to wait for its arrival and resume it.
    /// Dropping the resume sender also releases the pause.
    pub fn at(point: SyncPoint) -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, wait_reached) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        (
            Self {
                point,
                reached,
                resume,
            },
            wait_reached,
            release,
        )
    }

    /// Uses this pause in place of the global registry while polling the future.
    /// Independently spawned tasks require their own scope.
    pub async fn scope<F: Future>(self, future: F) -> F::Output {
        PAUSE.scope(RefCell::new(Some(self)), future).await
    }
}

/// Returns whether the point was reached within a pause scope.
pub(super) async fn on(point: SyncPoint) -> bool {
    let Ok(pause) = PAUSE.try_with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|pause| pause.point == point) {
            slot.take()
        } else {
            None
        }
    }) else {
        return false;
    };

    if let Some(Pause {
        reached, resume, ..
    }) = pause
    {
        let _ = reached.send(());
        let _ = resume.await;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::Pause;

    #[tokio::test]
    async fn scoped_pauses_are_isolated_and_release_on_drop() {
        const POINT: &str = "PAUSE_SCOPE_TEST";
        let legacy_called = Arc::new(AtomicBool::new(false));
        crate::reset();
        crate::hook(POINT, {
            let legacy_called = legacy_called.clone();
            move || {
                assert!(!legacy_called.swap(true, Ordering::Relaxed));
                async {}
            }
        });

        let (first, first_reached, resume_first) = Pause::at(POINT);
        let (second, second_reached, resume_second) = Pause::at(POINT);
        let first_task = tokio::spawn(first.scope(async {
            tokio::spawn(crate::on(POINT)).await.unwrap();
            let mut other = std::pin::pin!(crate::on("OTHER_POINT"));
            assert!(
                futures_util::poll!(other.as_mut()).is_ready(),
                "paused at a different sync point"
            );
            crate::on(POINT).await;
            crate::on(POINT).await;
        }));
        let second_task = tokio::spawn(second.scope(crate::on(POINT)));

        timeout(Duration::from_secs(10), async {
            first_reached.await.unwrap();
            second_reached.await.unwrap();
            assert!(legacy_called.load(Ordering::Relaxed));

            resume_first.send(()).unwrap();
            first_task.await.unwrap();
            assert!(!second_task.is_finished());

            drop(resume_second);
            second_task.await.unwrap();
        })
        .await
        .unwrap();

        crate::remove_action(POINT);
    }
}
