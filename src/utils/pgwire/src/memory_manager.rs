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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pg_message::ServerThrottleReason;

/// `MessageMemoryManager` tracks memory usage of frontend messages and provides hint of whether this message would cause memory constraint violation.
///
/// For any message of size n,
/// - If n <= `min_filter_bytes`, the message won't add to the memory constraint, because the message is too small that it can always be accepted.
/// - If n > `max_filter_bytes`, the message won't add to the memory constraint, because the message is too large that it's better to be rejected immediately.
/// - Otherwise, the message will add size n to the memory constraint. See `MessageMemoryManager::add`.
pub struct MessageMemoryManager {
    pub current_running_bytes: AtomicU64,
    pub max_running_bytes: u64,
    pub min_filter_bytes: u64,
    pub max_filter_bytes: u64,
}

pub type MessageMemoryManagerRef = Arc<MessageMemoryManager>;

impl MessageMemoryManager {
    pub fn new(max_running_bytes: u64, min_bytes: u64, max_bytes: u64) -> Self {
        Self {
            current_running_bytes: AtomicU64::new(0),
            max_running_bytes,
            min_filter_bytes: min_bytes,
            max_filter_bytes: max_bytes,
        }
    }

    /// Returns a `ServerThrottleReason` indicating whether any memory constraint has been violated.
    pub fn add(
        self: &MessageMemoryManagerRef,
        bytes: u64,
    ) -> (Option<ServerThrottleReason>, Option<MessageMemoryGuard>) {
        if bytes > self.max_filter_bytes {
            return (Some(ServerThrottleReason::TooLargeMessage), None);
        }
        if bytes <= self.min_filter_bytes {
            return (None, None);
        }
        let prev = self
            .current_running_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        let guard: MessageMemoryGuard = MessageMemoryGuard {
            bytes,
            manager: self.clone(),
        };
        // Always permit at least one entry, regardless of its size.
        let reason = if prev != 0 && prev + bytes > self.max_running_bytes {
            Some(ServerThrottleReason::TooManyMemoryUsage)
        } else {
            None
        };
        (reason, Some(guard))
    }

    fn sub(&self, bytes: u64) {
        self.current_running_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub struct MessageMemoryGuard {
    bytes: u64,
    manager: MessageMemoryManagerRef,
}

impl Drop for MessageMemoryGuard {
    fn drop(&mut self) {
        self.manager.sub(self.bytes);
    }
}
