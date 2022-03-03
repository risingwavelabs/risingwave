mod etcd_meta_store;
mod mem_meta_store;
mod meta_store;
#[cfg(test)]
mod tests;
mod transaction;

pub type ColumnFamily = String;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub use mem_meta_store::*;
pub use meta_store::*;
pub use transaction::*;
