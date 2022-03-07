use itertools::Itertools;
use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo};
use risingwave_storage::hummock::key::key_with_epoch;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    HummockEpoch, HummockSSTableId, SSTableBuilderOptions, SstableBuilder,
};

pub fn generate_test_tables(epoch: u64, table_id: &mut u64) -> Vec<SstableInfo> {
    // Tables to add
    let opt = SSTableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 4096,
        table_capacity: 0,
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    };

    let mut tables = vec![];
    for i in 0..2 {
        let mut b = SstableBuilder::new(opt.clone());
        let kv_pairs = vec![
            (i, HummockValue::Put(b"test".as_slice())),
            (i * 10, HummockValue::Put(b"test".as_slice())),
        ];
        for kv in kv_pairs {
            b.add(&iterator_test_key_of_epoch(*table_id, kv.0, epoch), kv.1);
        }
        let (_data, meta) = b.finish();
        tables.push(SstableInfo {
            id: *table_id,
            key_range: Some(KeyRange {
                left: meta.smallest_key,
                right: meta.largest_key,
                inf: false,
            }),
        });
        (*table_id) += 1;
    }
    tables
}

/// Generate keys like `001_key_test_00002` with timestamp `epoch`.
pub fn iterator_test_key_of_epoch(table: u64, idx: usize, ts: HummockEpoch) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_epoch(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        ts,
    )
}

pub fn get_sorted_sstable_ids(sstables: &[SstableInfo]) -> Vec<HummockSSTableId> {
    sstables.iter().map(|table| table.id).sorted().collect_vec()
}

pub fn get_sorted_committed_sstable_ids(hummock_version: &HummockVersion) -> Vec<HummockSSTableId> {
    hummock_version
        .levels
        .iter()
        .flat_map(|level| level.table_ids.clone())
        .sorted()
        .collect_vec()
}
