#[derive(Default)]
pub struct StoreLocalMetrics {
    pub cache_data_block_miss: u64,
    pub cache_data_block_total: u64,
    pub cache_meta_block_miss: u64,
    pub cache_meta_block_total: u64,

    pub scan_read_bytes: u64,

    // include multiple versions of one key.
    pub scan_key_count: u64,
    pub processed_key_count: u64,
}
