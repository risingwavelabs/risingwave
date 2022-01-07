use crate::manager::Epoch;
use crate::storage::OperationOption;

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
    // key, value, options
    Put(Vec<u8>, Vec<u8>, Vec<OperationOption>),
    // key, options
    Delete(Vec<u8>, Vec<OperationOption>),
}

/// Preconditions are checked in the beginning of a transaction
pub enum Precondition {
    KeyExists {
        key: Vec<u8>,
        version: Option<Epoch>,
    },
}
