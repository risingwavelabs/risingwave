use crate::storage::{Key, KeyValueVersion, OperationOption, Value};

/// A `Transaction` executes several writes(aka. operations) to meta store atomically with optional
/// preconditions checked. It executes as follow:
/// 1. If all `preconditions` are valid, all `operations` are executed; Otherwise no operation
/// is executed.
/// 2. Upon `commit` the transaction, the `TransactionAbort` error will be returned if
/// any precondition was not met in previous step.
pub trait Transaction: Send + Sync + 'static {
    fn add_preconditions(&mut self, preconditions: Vec<Precondition>);
    fn add_operations(&mut self, operations: Vec<Operation>);
    fn commit(&self) -> Result<(), crate::storage::Error>;
}

pub enum Operation {
    /// `put` key value pairs.
    /// If `WithVersion` is not specified, a default global version is used.
    Put(Key, Value, Vec<OperationOption>),
    /// `delete` key value pairs based on `OperationOption`s.
    /// If `WithVersion` is not specified, all versions of this `Key` are matched and deleted.
    /// Otherwise, only specific version of this `Key` is deleted.
    Delete(Key, Vec<OperationOption>),
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
