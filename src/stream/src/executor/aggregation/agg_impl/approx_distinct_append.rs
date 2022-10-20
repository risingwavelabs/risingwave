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

use risingwave_common::bail;

use super::approx_distinct_utils::{RegisterBucket, StreamingApproxDistinct};
use crate::executor::StreamExecutorResult;

#[derive(Clone, Debug)]
pub(super) struct AppendOnlyRegisterBucket {
    max: u8,
}

impl RegisterBucket for AppendOnlyRegisterBucket {
    fn new() -> Self {
        Self { max: 0 }
    }

    fn update_bucket(&mut self, index: usize, is_insert: bool) -> StreamExecutorResult<()> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        if !is_insert {
            bail!("HyperLogLog: Deletion in append-only bucket");
        }

        if index as u8 > self.max {
            self.max = index as u8;
        }

        Ok(())
    }

    fn get_max(&self) -> StreamExecutorResult<u8> {
        Ok(self.max)
    }
}

#[derive(Clone, Debug, Default)]
pub struct AppendOnlyStreamingApproxDistinct {
    // TODO(yuchao): The state may need to be stored in state table to allow correct recovery.
    registers: Vec<AppendOnlyRegisterBucket>,
    initial_count: i64,
}

impl StreamingApproxDistinct for AppendOnlyStreamingApproxDistinct {
    type Bucket = AppendOnlyRegisterBucket;

    fn with_i64(registers_num: u32, initial_count: i64) -> Self {
        Self {
            registers: vec![AppendOnlyRegisterBucket::new(); registers_num as usize],
            initial_count,
        }
    }

    fn get_initial_count(&self) -> i64 {
        self.initial_count
    }

    fn reset_buckets(&mut self, registers_num: u32) {
        self.registers = vec![AppendOnlyRegisterBucket::new(); registers_num as usize];
    }

    fn registers(&self) -> &[AppendOnlyRegisterBucket] {
        &self.registers
    }

    fn registers_mut(&mut self) -> &mut [AppendOnlyRegisterBucket] {
        &mut self.registers
    }
}
