use risingwave_common::config::StorageConfig;

pub fn default_config_for_test() -> StorageConfig {
    StorageConfig {
        sstable_size: 256 * (1 << 20),
        block_size: 64 * (1 << 10),
        bloom_false_positive: 0.1,
        data_directory: "hummock_001".to_string(),
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        async_checkpoint_enabled: true,
    }
}
