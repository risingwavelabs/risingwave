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

//! # DataFusion memory pool
//!
//! This module implements [`RwMemoryPool`], a DataFusion [`MemoryPool`] that routes allocations
//! through RisingWave's [`MemoryContext`] hierarchy.
//!
//! **Scope:** This pool is used exclusively by the DataFusion batch engine for Iceberg queries.
//! The RisingWave native batch engine has its own memory management and does **not** use this pool.
//!
//! ## Spillable vs unspillable
//!
//! - **Spillable** operators (e.g. sort, hash join) can spill to disk when memory is low.
//!   Their allocations go through `spillable_ctx`.
//! - **Unspillable** operators must get memory or the query fails.
//!   Their allocations go through `unspillable_ctx`.
//!
//! ## Headroom via `MemoryContext` hierarchy
//!
//! To prevent spillable operators from starving unspillable ones, a three-level context tree is
//! used:
//!
//! ```text
//! batch_mem_context (limit = L)                          ← FrontendEnv.mem_context (shared)
//! ├── df_spillable_budget_ctx (limit = L × 0.9)          ← FrontendEnv, DataFusion-only
//! │   ├── pool_A.spillable_ctx                            ← per-query RwMemoryPool
//! │   └── pool_B.spillable_ctx
//! ├── pool_A.unspillable_ctx
//! └── pool_B.unspillable_ctx
//! ```
//!
//! [`create_df_spillable_budget_ctx`] creates the shared intermediate node. Its limit
//! (`L × (1 − DF_UNSPILLABLE_MEM_RESERVED_RATIO)`) caps total DataFusion spillable usage,
//! guaranteeing at least 10 % headroom for unspillable operators. Because the limit is enforced
//! structurally in the context tree, concurrent `try_grow` calls from different pools do not
//! suffer from spurious failures.

use datafusion::execution::memory_pool::{
    MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use datafusion_common::{DataFusionError, Result as DFResult, resources_datafusion_err};
use prometheus::core::Atomic;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use thiserror_ext::AsReport;

/// Fraction of batch memory reserved exclusively for unspillable DataFusion operators.
/// Spillable operators are collectively capped at `(1 - ratio) * batch_limit`.
pub(crate) const DF_UNSPILLABLE_MEM_RESERVED_RATIO: f64 = 0.1;

/// Create the shared spillable-budget context for the DataFusion engine.
///
/// This context sits between the root `batch_mem_context` and all per-query
/// spillable contexts, capping total spillable usage at
/// `batch_mem_context.limit * (1 - DF_UNSPILLABLE_MEM_RESERVED_RATIO)`.
/// It must be created once and shared across all `RwMemoryPool` instances.
pub(crate) fn create_df_spillable_budget_ctx(batch_mem_context: &MemoryContext) -> MemoryContext {
    let limit = batch_mem_context.mem_limit();
    let spillable_limit = (limit as f64 * (1.0 - DF_UNSPILLABLE_MEM_RESERVED_RATIO)) as u64;
    MemoryContext::new_with_mem_limit(
        Some(batch_mem_context.clone()),
        TrAdderAtomic::new(0),
        spillable_limit,
    )
}

pub struct RwMemoryPool {
    spillable_ctx: MemoryContext,
    unspillable_ctx: MemoryContext,
}

impl RwMemoryPool {
    pub fn new(batch_mem_context: MemoryContext, df_spillable_budget: MemoryContext) -> Self {
        let spillable_ctx = MemoryContext::new(Some(df_spillable_budget), TrAdderAtomic::new(0));
        let unspillable_ctx = MemoryContext::new(Some(batch_mem_context), TrAdderAtomic::new(0));
        Self {
            spillable_ctx,
            unspillable_ctx,
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
        let success = match reservation.consumer().can_spill() {
            true => self.spillable_ctx.add(additional as i64),
            false => self.unspillable_ctx.add(additional as i64),
        };
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
        MemoryLimit::Finite(self.unspillable_ctx.mem_limit() as usize)
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

    const TEST_RESERVED_RATIO: f64 = 0.1;

    /// Build the three-level context tree used in production and return (pool, parent).
    fn create_pool(limit: u64) -> (Arc<dyn MemoryPool>, MemoryContext) {
        let parent = MemoryContext::root(TrAdderAtomic::new(0), limit);
        let spillable_budget = MemoryContext::new_with_mem_limit(
            Some(parent.clone()),
            TrAdderAtomic::new(0),
            (limit as f64 * (1.0 - TEST_RESERVED_RATIO)) as u64,
        );
        (
            Arc::new(RwMemoryPool::new(parent.clone(), spillable_budget)),
            parent,
        )
    }

    /// Build two pools sharing the same parent (simulates concurrent queries).
    fn create_two_pools(limit: u64) -> (Arc<dyn MemoryPool>, Arc<dyn MemoryPool>, MemoryContext) {
        let parent = MemoryContext::root(TrAdderAtomic::new(0), limit);
        let spillable_budget = MemoryContext::new_with_mem_limit(
            Some(parent.clone()),
            TrAdderAtomic::new(0),
            (limit as f64 * (1.0 - TEST_RESERVED_RATIO)) as u64,
        );
        let pool_a = Arc::new(RwMemoryPool::new(parent.clone(), spillable_budget.clone()))
            as Arc<dyn MemoryPool>;
        let pool_b =
            Arc::new(RwMemoryPool::new(parent.clone(), spillable_budget)) as Arc<dyn MemoryPool>;
        (pool_a, pool_b, parent)
    }

    fn unspillable_consumer(name: &str) -> MemoryConsumer {
        MemoryConsumer::new(name)
    }

    fn spillable_consumer(name: &str) -> MemoryConsumer {
        MemoryConsumer::new(name).with_can_spill(true)
    }

    #[test]
    fn test_basic_grow_and_shrink() {
        let (pool, _) = create_pool(1024);
        let consumer = unspillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        reservation.grow(512);
        assert_eq!(pool.reserved(), 512);

        reservation.shrink(256);
        assert_eq!(pool.reserved(), 256);

        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_try_grow_within_limit_unspillable() {
        let (pool, _) = create_pool(1024);
        let consumer = unspillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);

        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 1024);
    }

    #[test]
    fn test_try_grow_within_limit_spillable() {
        // limit=1024, spillable budget = floor(1024 * 0.9) = 921
        let (pool, _) = create_pool(1024);
        let consumer = spillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);

        // 512 + 409 = 921 = spillable budget
        assert!(reservation.try_grow(409).is_ok());
        assert_eq!(pool.reserved(), 921);

        // One more byte exceeds spillable budget (921 + 1 > 921)
        assert!(reservation.try_grow(1).is_err());
    }

    #[test]
    fn test_try_grow_exceeds_limit() {
        let (pool, _) = create_pool(1024);
        let consumer = unspillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        reservation.grow(1024);

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
        let (pool, _) = create_pool(1024);

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
        // limit=1024, spillable budget = 921
        let (pool, _) = create_pool(1024);

        let mut r_unspill = unspillable_consumer("unspillable").register(&pool);
        let mut r_spill = spillable_consumer("spillable").register(&pool);

        // Unspillable takes 103 (the reserved headroom is 1024 - 921 = 103)
        assert!(r_unspill.try_grow(103).is_ok());
        assert_eq!(pool.reserved(), 103);

        // Spillable takes remaining budget: parent has 103 used, spillable_budget has 0 used.
        // spillable_budget limit = 921, parent limit = 1024.
        // spillable can add up to min(921, 1024-103) = 921
        assert!(r_spill.try_grow(921).is_ok());
        assert_eq!(pool.reserved(), 1024);

        // Pool is full
        assert!(r_spill.try_grow(1).is_err());
        assert!(r_unspill.try_grow(1).is_err());
    }

    #[test]
    fn test_two_pools_spillable_share_budget() {
        // Two pools share the same spillable_budget_ctx. Total spillable across both is capped.
        // limit=1000, spillable budget = floor(1000 * 0.9) = 900
        let (pool_a, pool_b, parent) = create_two_pools(1000);

        let mut r_a = spillable_consumer("a").register(&pool_a);
        let mut r_b = spillable_consumer("b").register(&pool_b);

        // Pool A takes 450
        assert!(r_a.try_grow(450).is_ok());
        // Pool B takes 450
        assert!(r_b.try_grow(450).is_ok());
        assert_eq!(parent.get_bytes_used(), 900);

        // Both at 450, total spillable = 900 = budget. One more byte fails.
        assert!(r_a.try_grow(1).is_err());
        assert!(r_b.try_grow(1).is_err());

        // But unspillable can still use the remaining 100
        let mut r_unspill = unspillable_consumer("unspill").register(&pool_a);
        assert!(r_unspill.try_grow(100).is_ok());
        assert_eq!(parent.get_bytes_used(), 1000);
    }

    #[test]
    fn test_memory_limit_reports_correctly() {
        let (pool, _) = create_pool(2048);
        match pool.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, 2048),
            MemoryLimit::Infinite => panic!("Expected finite memory limit"),
            _ => panic!("Unexpected memory limit variant"),
        }
    }

    #[test]
    fn test_parent_context_tracks_usage() {
        let (pool, parent) = create_pool(4096);

        let consumer = unspillable_consumer("test");
        let mut reservation = consumer.register(&pool);

        reservation.grow(1000);
        assert_eq!(parent.get_bytes_used(), 1000);

        drop(reservation);
        assert_eq!(parent.get_bytes_used(), 0);
    }
}
