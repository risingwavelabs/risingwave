use crate::storage::{Key, KeyValueVersion, Value};

/// A `Transaction` executes several writes(aka. operations) to meta store atomically with optional
/// preconditions checked. It executes as follow:
/// 1. If all `preconditions` are valid, all `operations` are executed; Otherwise no operation
/// is executed.
/// 2. Upon `commit` the transaction, the `TransactionAbort` error will be returned if
/// any precondition was not met in previous step.
pub struct Transaction {
    preconditions: Vec<Precondition>,
    operations: Vec<Operation>,
}

impl Transaction {
    pub fn new() -> Transaction {
        Transaction {
            preconditions: vec![],
            operations: vec![],
        }
    }
    pub fn add_preconditions(&mut self, mut preconditions: impl AsMut<Vec<Precondition>>) {
        self.preconditions.append(preconditions.as_mut());
    }
    pub fn add_operations(&mut self, mut operations: impl AsMut<Vec<Operation>>) {
        self.operations.append(operations.as_mut());
    }
    pub fn preconditions(&self) -> &[Precondition] {
        &self.preconditions
    }
    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }
}

pub enum Operation {
    /// `put` key value pairs.
    /// If `WithVersion` is not specified, a default global version is used.
    Put(Key, Value, Option<KeyValueVersion>),
    /// `delete` key value pairs.
    /// If `WithVersion` is not specified, all versions of this `Key` are matched and deleted.
    /// Otherwise, only specific version of this `Key` is deleted.
    Delete(Key, Option<KeyValueVersion>),
}

/// Preconditions are checked in the beginning of a transaction
pub enum Precondition {
    #[allow(dead_code)]
    KeyExists {
        key: Key,
        /// If version is None, a default global version is used.
        version: Option<KeyValueVersion>,
    },
}
