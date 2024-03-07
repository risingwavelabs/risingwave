// Copyright 2024 RisingWave Labs
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

use std::collections::HashSet;
use std::time::{Duration, SystemTime};

const ERROR_SUPPRESSOR_RESET_DURATION: Duration = Duration::from_millis(60 * 60 * 1000); // 1h

#[derive(Debug)]
pub struct ErrorSuppressor {
    max_unique: usize,
    unique: HashSet<String>,
    last_reset_time: SystemTime,
}

impl ErrorSuppressor {
    pub fn new(max_unique: usize) -> Self {
        Self {
            max_unique,
            last_reset_time: SystemTime::now(),
            unique: Default::default(),
        }
    }

    pub fn suppress_error(&mut self, error: &str) -> bool {
        self.try_reset();
        if self.unique.contains(error) {
            false
        } else if self.unique.len() < self.max_unique {
            self.unique.insert(error.to_string());
            false
        } else {
            // We have exceeded the capacity.
            true
        }
    }

    pub fn max(&self) -> usize {
        self.max_unique
    }

    fn try_reset(&mut self) {
        if self.last_reset_time.elapsed().unwrap() >= ERROR_SUPPRESSOR_RESET_DURATION {
            *self = Self::new(self.max_unique)
        }
    }
}
