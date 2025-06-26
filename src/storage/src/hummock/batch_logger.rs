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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::Level;

struct State<T> {
    data: Vec<T>,
    updated: Instant,
}

struct Inner<T> {
    level: Level,
    interval: Duration,
    batch: usize,
    msg: String,
    state: Mutex<State<T>>,
}

pub struct BatchLogger<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Debug for BatchLogger<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchLogger").finish()
    }
}

impl<T> Clone for BatchLogger<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> BatchLogger<T> {
    pub fn new(level: Level, msg: &str, interval: Duration, batch: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                level,
                interval,
                batch,
                msg: msg.to_owned(),
                state: Mutex::new(State {
                    data: vec![],
                    updated: Instant::now(),
                }),
            }),
        }
    }

    pub fn log(&self, data: T)
    where
        T: Debug,
    {
        let mut state = self.inner.state.lock();
        if state.updated.elapsed() >= self.inner.interval || state.data.len() >= self.inner.batch {
            match self.inner.level {
                Level::ERROR => tracing::error!(data = ?state.data, "{msg}", msg = self.inner.msg),
                Level::WARN => tracing::warn!(data = ?state.data, "{msg}", msg = self.inner.msg),
                Level::INFO => tracing::info!(data = ?state.data, "{msg}", msg = self.inner.msg),
                Level::DEBUG => tracing::debug!(data = ?state.data, "{msg}", msg = self.inner.msg),
                Level::TRACE => tracing::trace!(data = ?state.data, "{msg}", msg = self.inner.msg),
            }
            state.data.clear();
            state.updated = Instant::now();
        }
        state.data.push(data);
    }
}
