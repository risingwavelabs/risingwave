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

//! # Memory pool design for DataFusion execution
//!
//! This module implements a shared memory pool used by DataFusion operators during query
//! execution. The pool is split into two logical parts to handle different operator needs:
//!
//! ## Spillable vs unspillable consumers
//!
//! - **Spillable** operators (e.g. sort, hash join) can spill intermediate data to disk when
//!   memory is low. They use `spillable_ctx` and may be denied allocation when the pool is
//!   full, at which point they can spill and retry or reduce in-memory usage.
//!
//! - **Unspillable** operators have no spill path; they must get memory or the query fails.
//!   They use `unspillable_ctx`. If the pool is exhausted by spillable usage, unspillable
//!   allocations would fail and cause unnecessary errors.
//!
//! ## Reserved headroom for unspillable
//!
//! We reserve a fraction of the pool (`UNSPILLABLE_MEM_RESERVED_RATIO`, 10%) as headroom for
//! unspillable consumers. When a spillable consumer calls `try_grow`, we require that
//! `additional + unspillable_mem_reserved` bytes are available before allowing the allocation.
//! After the allocation we only charge `additional`; the "reserved" is not actually used but
//! ensures that much space remains available for unspillable operators. Thus spillable
//! usage cannot starve unspillable usage, and queries that mix both operator types behave
//! predictably under memory pressure.

use datafusion::execution::memory_pool::{
    MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use datafusion_common::{DataFusionError, Result as DFResult, resources_datafusion_err};
use prometheus::core::Atomic;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use thiserror_ext::AsReport;

const UNSPILLABLE_MEM_RESERVED_RATIO: f64 = 0.1;

pub struct RwMemoryPool {
    spillable_ctx: MemoryContext,
    unspillable_ctx: MemoryContext,
    unspillable_mem_reserved: i64,
}

impl RwMemoryPool {
    pub fn new(parent: MemoryContext) -> Self {
        let unspillable_mem_reserved =
            (UNSPILLABLE_MEM_RESERVED_RATIO * parent.mem_limit() as f64) as i64;
        let spillable_ctx = MemoryContext::new(Some(parent.clone()), TrAdderAtomic::new(0));
        let unspillable_ctx = MemoryContext::new(Some(parent), TrAdderAtomic::new(0));
        Self {
            spillable_ctx,
            unspillable_ctx,
            unspillable_mem_reserved,
        }
    }
}

impl std::fmt::Debug for RwMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RwMemoryPool")
    }
}

impl MemoryPool for RwMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let success = match reservation.consumer().can_spill() {
            true => self.spillable_ctx.add(additional as i64),
            false => self.unspillable_ctx.add(additional as i64),
        };
        if !success {
            tracing::warn!(
                error = %insufficient_capacity_err(
                    reservation,
                    additional
                ).as_report()
            )
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        match reservation.consumer().can_spill() {
            true => self.spillable_ctx.add(-(shrink as i64)),
            false => self.unspillable_ctx.add(-(shrink as i64)),
        };
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        let mut success = false;
        if reservation.consumer().can_spill() {
            if self
                .spillable_ctx
                .add(additional as i64 + self.unspillable_mem_reserved)
            {
                self.spillable_ctx.add(-self.unspillable_mem_reserved);
                success = true;
            }
        } else if self.unspillable_ctx.add(additional as i64) {
            success = true;
        }

        if success {
            Ok(())
        } else {
            Err(insufficient_capacity_err(reservation, additional))
        }
    }

    fn reserved(&self) -> usize {
        let bytes_used =
            self.spillable_ctx.get_bytes_used() + self.unspillable_ctx.get_bytes_used();
        if bytes_used <= 0 {
            0
        } else {
            bytes_used.try_into().unwrap_or(usize::MAX)
        }
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.spillable_ctx.mem_limit() as usize)
    }
}

// The message is a bit different from datafusion's because it's hard to calculate the
// available memory for risingwave memory pool. Risingwave memory pool is a chained memory pool
// and different parents will maintain memory usage and limit independently.
fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
) -> DataFusionError {
    resources_datafusion_err!(
        "Failed to allocate additional {} for {} with {} already allocated for this reservation",
        human_readable_size(additional),
        reservation.consumer().name(),
        human_readable_size(reservation.size())
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

    fn unspillable_consumer(name: &str) -> MemoryConsumer {
        MemoryConsumer::new(name)
    }

    fn spillable_consumer(name: &str) -> MemoryConsumer {
        MemoryConsumer::new(name).with_can_spill(true)
    }

    #[test]
    fn test_basic_grow_and_shrink() {
        let pool = create_pool(1024);
        let consumer = unspillable_consumer("test");
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
    fn test_try_grow_within_limit_unspillable() {
        let pool = create_pool(1024);
        let consumer = unspillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        // Unspillable can use full limit
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);

        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 1024);
    }

    #[test]
    fn test_try_grow_within_limit_spillable() {
        // Spillable is limited by (limit - 10% reserved for unspillable).
        // With limit 1024, reserved = 102, so spillable can use at most 922.
        let pool = create_pool(1024);
        let consumer = spillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);

        // 512 + 410 = 922 <= 922 (spillable budget)
        assert!(reservation.try_grow(410).is_ok());
        assert_eq!(pool.reserved(), 922);

        // One more byte would exceed spillable budget (922 + 1 + 102 > 1024)
        assert!(reservation.try_grow(1).is_err());
    }

    #[test]
    fn test_try_grow_exceeds_limit() {
        let pool = create_pool(1024);
        let consumer = unspillable_consumer("test");
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

        assert_eq!(pool.reserved(), 1024);
    }

    #[test]
    fn test_multiple_consumers_share_limit() {
        let pool = create_pool(1024);

        let c1 = unspillable_consumer("consumer1");
        let mut r1 = c1.register(&pool);

        let c2 = unspillable_consumer("consumer2");
        let mut r2 = c2.register(&pool);

        assert!(r1.try_grow(600).is_ok());
        assert!(r2.try_grow(400).is_ok());

        let result = r2.try_grow(100);
        assert!(result.is_err());
        assert_eq!(pool.reserved(), 1000);
    }

    #[test]
    fn test_spillable_and_unspillable_share_pool() {
        // Spillable reserves 10% for unspillable; both share the same parent limit.
        // try_grow(spillable) ensures (additional + reserved) is available, so with unspillable
        // already using 102, spillable can add at most 1024 - 102 - 102 = 820.
        let pool = create_pool(1024); // unspillable_mem_reserved = 102

        let mut r_unspill = unspillable_consumer("unspillable").register(&pool);
        let mut r_spill = spillable_consumer("spillable").register(&pool);

        // Unspillable takes the reserved 102
        assert!(r_unspill.try_grow(102).is_ok());
        assert_eq!(pool.reserved(), 102);

        // Spillable can add at most 820 (leaving 102 headroom for unspillable)
        assert!(r_spill.try_grow(820).is_ok());
        assert_eq!(pool.reserved(), 922);

        // Unspillable can use the remaining 102 (the reserved headroom)
        assert!(r_unspill.try_grow(102).is_ok());
        assert_eq!(pool.reserved(), 1024);

        // Pool is full
        assert!(r_spill.try_grow(1).is_err());
        assert!(r_unspill.try_grow(1).is_err());
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

        let consumer = unspillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        reservation.grow(1000);
        assert_eq!(parent.get_bytes_used(), 1000);

        drop(reservation);
        assert_eq!(parent.get_bytes_used(), 0);
    }
}
