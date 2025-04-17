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

use itertools::Itertools;

use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::plan_node::{BatchLimit, BatchSeqScan, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};

pub struct BatchPushLimitToScanRule {}

impl Rule for BatchPushLimitToScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let limit: &BatchLimit = plan.as_batch_limit()?;
        let limit_input = limit.input();
        let scan: &BatchSeqScan = limit_input.as_batch_seq_scan()?;
        if scan.limit().is_some() {
            return None;
        }
        let pushed_limit = limit.limit() + limit.offset();
        let new_scan = BatchSeqScan::new_with_dist(
            scan.core().clone(),
            scan.base.distribution().clone(),
            scan.scan_ranges().iter().cloned().collect_vec(),
            Some(pushed_limit),
        );
        Some(limit.clone_with_input(new_scan.into()).into())
    }
}

impl BatchPushLimitToScanRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchPushLimitToScanRule {})
    }
}
