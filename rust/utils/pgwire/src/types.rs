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
//
use std::ops::Index;

/// A row of data returned from the database by a query.
#[derive(Debug)]
// NOTE: Since we only support simple query protocol, the values are represented as strings.
pub struct Row(Vec<Option<String>>);

impl Row {
    /// Create a row from values.
    pub fn new(row: Vec<Option<String>>) -> Self {
        Self(row)
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the row contains no values. Required by clippy.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the values.
    pub fn values(&self) -> &[Option<String>] {
        &self.0
    }
}

impl Index<usize> for Row {
    type Output = Option<String>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}
