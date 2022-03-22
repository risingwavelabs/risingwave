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
