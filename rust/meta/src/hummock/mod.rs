mod hummock_client;
mod hummock_manager;
#[cfg(test)]
mod tests;

pub use hummock_client::*;
pub use hummock_manager::*;

pub type HummockTTL = u64;
pub type HummockRefCount = u64;
pub type HummockVersionId = u64;
pub type HummockContextId = i32;
