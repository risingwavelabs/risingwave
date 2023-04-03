//  Copyright 2023 RisingWave Labs
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

use paste::paste;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalLimit, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;

const _LEAF_OR_BREAKERS: &[&str] = &[
    "as_logical_scan",
    "as_logical_filter",
    "as_logical_top_n",
    "as_logical_join",
    "as_logical_multi_join",
    "as_logical_values",
];

macro_rules! cannot_push_down {
    ($plan:ident, $type:literal) => {
        paste! {match $plan.[<as_logical_ $type>]() {
                Some(_) => true,
                None => false,
            }
        }
    };
}

macro_rules! try_match {
    ($plan:ident, $macro:ident) => {
        if ($macro!($plan, "scan")) {
            true
        } else if ($macro!($plan, "filter")) {
            true
        } else if ($macro!($plan, "top_n")) {
            true
        } else if ($macro!($plan, "join")) {
            true
        } else if ($macro!($plan, "multi_join")) {
            true
        } else if ($macro!($plan, "values")) {
            true
        } else if ($macro!($plan, "now")) {
            true
        } else if ($macro!($plan, "source")) {
            true
        } else {
            false
        }
    };
}

pub struct LimitPushDownRule {}

impl Rule for LimitPushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let limit: &LogicalLimit = plan.as_logical_limit()?;
        let input = limit.input();
        if try_match!(input, cannot_push_down) {
            return None;
        }
        if input.inputs().len() > 1 {
            return None;
        }
        let logical_limit = limit.clone_with_input(input.inputs()[0].clone());
        let new_input = input.clone_with_inputs(&[logical_limit.into()]);
        Some(limit.clone_with_input(new_input).into())
    }
}

impl LimitPushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(LimitPushDownRule {})
    }
}
