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

use datafusion::execution::memory_pool::{
    MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use datafusion_common::{DataFusionError, Result as DFResult, resources_datafusion_err};
use prometheus::core::Atomic;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use thiserror_ext::AsReport;

pub struct RwMemoryPool {
    ctx: MemoryContext,
}

impl RwMemoryPool {
    pub fn new(parent: MemoryContext) -> Self {
        let counter = TrAdderAtomic::new(0);
        let ctx = MemoryContext::new(Some(parent), counter);
        Self { ctx }
    }

    fn available(&self) -> usize {
        let limit = self.ctx.mem_limit().try_into().unwrap_or(usize::MAX);
        let used = self.reserved();
        limit.saturating_sub(used)
    }
}

impl std::fmt::Debug for RwMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RwMemoryPool")
    }
}

impl MemoryPool for RwMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let success = self.ctx.add(additional as i64);
        if !success {
            tracing::warn!(
                error = %insufficient_capacity_err(
                    reservation,
                    additional,
                    self.available()
                ).as_report()
            )
        }
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.ctx.add(-(shrink as i64));
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        if self.ctx.add(additional as i64) {
            Ok(())
        } else {
            Err(insufficient_capacity_err(
                reservation,
                additional,
                self.available(),
            ))
        }
    }

    fn reserved(&self) -> usize {
        let bytes_used = self.ctx.get_bytes_used();
        if bytes_used <= 0 {
            0
        } else {
            bytes_used.try_into().unwrap_or(usize::MAX)
        }
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.ctx.mem_limit() as usize)
    }
}

fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    resources_datafusion_err!(
        "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total pool",
        human_readable_size(additional),
        reservation.consumer().name(),
        human_readable_size(reservation.size()),
        human_readable_size(available)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::MemoryConsumer;
    use risingwave_common::metrics::TrAdderAtomic;

    use super::*;

    fn create_pool(limit: u64) -> Arc<dyn MemoryPool> {
        let parent = MemoryContext::root(TrAdderAtomic::new(0), limit);
        Arc::new(RwMemoryPool::new(parent))
    }

    #[test]
    fn test_basic_grow_and_shrink() {
        let pool = create_pool(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Grow within limit
        reservation.grow(512);
        assert_eq!(pool.reserved(), 512);

        // Shrink
        reservation.shrink(256);
        assert_eq!(pool.reserved(), 256);

        // Drop releases all
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_try_grow_within_limit() {
        let pool = create_pool(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Should succeed
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);

        // Should succeed (total = 1024 = limit)
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 1024);
    }

    #[test]
    fn test_try_grow_exceeds_limit() {
        let pool = create_pool(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Fill up to limit
        reservation.grow(1024);

        // Should fail - exceeds limit
        let result = reservation.try_grow(1);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to allocate additional"),
            "Unexpected error: {}",
            err_msg
        );

        // Pool should NOT have grown past the limit
        assert_eq!(pool.reserved(), 1024);
    }

    #[test]
    fn test_multiple_consumers_share_limit() {
        let pool = create_pool(1024);

        let c1 = MemoryConsumer::new("consumer1");
        let mut r1 = c1.register(&pool);

        let c2 = MemoryConsumer::new("consumer2");
        let mut r2 = c2.register(&pool);

        // Consumer 1 takes 600
        assert!(r1.try_grow(600).is_ok());

        // Consumer 2 takes 400
        assert!(r2.try_grow(400).is_ok());

        // Total = 1000, consumer 2 tries to take 100 more → total would be 1100 > 1024
        let result = r2.try_grow(100);
        assert!(result.is_err());

        assert_eq!(pool.reserved(), 1000);
    }

    #[test]
    fn test_memory_limit_reports_correctly() {
        let pool = create_pool(2048);
        match pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, 2048),
            MemoryLimit::Infinite => panic!("Expected finite memory limit"),
            _ => panic!("Unexpected memory limit variant"),
        }
    }

    #[test]
    fn test_parent_context_tracks_usage() {
        let parent = MemoryContext::root(TrAdderAtomic::new(0), 4096);
        let pool: Arc<dyn MemoryPool> = Arc::new(RwMemoryPool::new(parent.clone()));

        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        reservation.grow(1000);

        // Parent context should also see the usage
        assert_eq!(parent.get_bytes_used(), 1000);

        drop(reservation);
        assert_eq!(parent.get_bytes_used(), 0);
    }
}
