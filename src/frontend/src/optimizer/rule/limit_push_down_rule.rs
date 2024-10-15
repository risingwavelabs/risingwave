//  Copyright 2024 RisingWave Labs
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

use super::{BoxedRule, Result, Rule};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::PlanRef;

pub struct LimitPushDownRule {}

impl Rule for LimitPushDownRule {
    fn apply(&self, plan: PlanRef) -> Result<Option<PlanRef>> {
        let limit = plan.as_logical_limit();
        if limit.is_none() {
            return Ok(None);
        }
        let limit = limit.unwrap();

        let input = limit.input();
        let project = input.as_logical_project().to_owned();
        if project.is_none() {
            return Ok(None);
        }
        let project = project.unwrap();

        let input = project.input();
        let logical_limit = limit.clone_with_input(input);
        Ok(Some(project.clone_with_input(logical_limit.into()).into()))
    }
}

impl LimitPushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(LimitPushDownRule {})
    }
}
