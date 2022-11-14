// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use itertools::Itertools;

use crate::TestCase;

pub fn resolve_testcase_id(testcases: Vec<TestCase>) -> Result<Vec<TestCase>> {
    let mut testcases_with_ids = HashMap::new();
    for testcase in &testcases {
        if let Some(id) = &testcase.id {
            testcases_with_ids.insert(id.clone(), testcase.clone());
        }
    }

    testcases
        .into_iter()
        .map(|testcase| {
            let before_statements = if let Some(before) = &testcase.before {
                Some(
                    before
                        .iter()
                        .map(|id| {
                            testcases_with_ids
                                .get(id)
                                .map(|case| case.sql.clone())
                                .ok_or_else(|| anyhow!("failed to resolve {}: not found", id))
                        })
                        .try_collect()?,
                )
            } else {
                None
            };

            Ok(TestCase {
                before_statements,
                ..testcase
            })
        })
        .try_collect()
}
