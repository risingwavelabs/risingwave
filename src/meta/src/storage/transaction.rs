// Copyright 2023 RisingWave Labs
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

use crate::storage::{ColumnFamily, Key, Value};

/// A `Transaction` executes several writes(aka. operations) to meta store atomically with optional
/// preconditions checked. It executes as follow:
/// 1. If all `preconditions` are valid, all `operations` are executed; Otherwise no operation
/// is executed.
/// 2. Upon `commit` the transaction, the `TransactionAbort` error will be returned if
/// any precondition was not met in previous step.
#[derive(Default)]
pub struct Transaction {
    preconditions: Vec<Precondition>,
    operations: Vec<Operation>,
}

impl Transaction {
    /// Check whether the key exists.
    ///
    /// The check call will never failed, instead, it will only fail on commit.
    #[inline(always)]
    pub fn check_exists(&mut self, cf: ColumnFamily, key: Key) {
        self.add_precondition(Precondition::KeyExists { cf, key })
    }

    /// Check whether the key exists.
    ///
    /// The check call will never failed, instead, it will only fail on commit.
    #[inline(always)]
    pub fn check_equal(&mut self, cf: ColumnFamily, key: Key, value: Value) {
        self.add_precondition(Precondition::KeyEqual { cf, key, value })
    }

    /// Put the key/value pair if the preconditions satisfied.
    #[inline(always)]
    pub fn put(&mut self, cf: ColumnFamily, key: Key, value: Value) {
        self.add_operation(Operation::Put { cf, key, value })
    }

    /// Delete the key if the preconditions satisfied.
    #[inline(always)]
    pub fn delete(&mut self, cf: ColumnFamily, key: Key) {
        self.add_operation(Operation::Delete { cf, key })
    }

    #[inline(always)]
    fn add_precondition(&mut self, precondition: Precondition) {
        self.preconditions.push(precondition)
    }

    #[inline(always)]
    fn add_operation(&mut self, operation: Operation) {
        self.operations.push(operation)
    }

    /// Add a batch of preconditions.
    #[inline(always)]
    pub fn add_preconditions(&mut self, mut preconditions: impl AsMut<Vec<Precondition>>) {
        self.preconditions.append(preconditions.as_mut());
    }

    /// Add a batch of operations.
    #[inline(always)]
    pub fn add_operations(&mut self, mut operations: impl AsMut<Vec<Operation>>) {
        self.operations.append(operations.as_mut());
    }

    pub(super) fn into_parts(self) -> (Vec<Precondition>, Vec<Operation>) {
        (self.preconditions, self.operations)
    }

    #[cfg(test)]
    pub fn get_operations(&self) -> &Vec<Operation> {
        &self.operations
    }
}

pub enum Operation {
    /// `put` key value pairs.
    Put {
        cf: ColumnFamily,
        key: Key,
        value: Value,
    },
    /// `delete` key value pairs.
    Delete { cf: ColumnFamily, key: Key },
}

/// Preconditions are checked in the beginning of a transaction
pub enum Precondition {
    KeyExists {
        cf: ColumnFamily,
        key: Key,
    },
    KeyEqual {
        cf: ColumnFamily,
        key: Key,
        value: Value,
    },
}
