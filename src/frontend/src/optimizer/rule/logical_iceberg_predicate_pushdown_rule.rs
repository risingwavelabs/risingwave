//  Copyright 2025 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use super::prelude::*;
use crate::optimizer::plan_node::{LogicalFilter, LogicalIcebergScan, LogicalImmIcebergScan, PlanTreeNodeUnary};

/// NOTE(kwannoel): We do predicate pushdown to the iceberg-sdk here.
/// zone-map is used to evaluate predicates on iceberg tables.
/// Without zone-map, iceberg-sdk will still apply the predicate on its own.
/// See: <https://github.com/apache/iceberg-rust/blob/5c1a9e68da346819072a15327080a498ad91c488/crates/iceberg/src/arrow/reader.rs#L229-L235>.
pub struct LogicalIcebergPredicatePushDownRule {}

impl Rule<Logical> for LogicalIcebergPredicatePushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let input = filter.input();
        let scan: &LogicalImmIcebergScan = input.as_logical_imm_iceberg_scan()?;
        // NOTE(kwannoel): We only fill iceberg predicate here.
        assert!(scan.predicate().always_true());

        let predicate = filter.predicate().clone();
        // set the scan predicate to the pushdownable part
        let scan = scan.clone_with_predicate(predicate);
        Some(scan.into())
    }
}

impl LogicalIcebergPredicatePushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(LogicalIcebergPredicatePushDownRule {})
    }
}
