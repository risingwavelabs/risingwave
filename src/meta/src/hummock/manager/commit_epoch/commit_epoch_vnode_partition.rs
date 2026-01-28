use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockSstableId, KeyComparator};

pub fn build_vnode_partition_boundary_keys(
    table_id: TableId,
    vnode_count: usize,
    partition_count: usize,
) -> Vec<Bytes> {
    debug_assert!(partition_count > 0);
    debug_assert!(partition_count <= vnode_count);

    if partition_count <= 1 {
        return vec![];
    }

    // Keep the same partitioning rule with `MultiBuilder`:
    // - First `partition_count - remainder` partitions have `basic` vnodes.
    // - Last `remainder` partitions have `basic + 1` vnodes.
    let basic = vnode_count / partition_count;
    let remainder = vnode_count % partition_count;
    let small_partitions = partition_count - remainder;

    let mut boundary_keys = Vec::with_capacity(partition_count.saturating_sub(1));
    let mut start = 0usize;
    for idx in 0..partition_count {
        let size = if idx < small_partitions {
            basic
        } else {
            basic + 1
        };
        if idx > 0 {
            boundary_keys.push(vnode_boundary_full_key(table_id, start));
        }
        start += size;
    }
    debug_assert_eq!(start, vnode_count);
    boundary_keys
}

fn vnode_boundary_full_key(table_id: TableId, vnode: usize) -> Bytes {
    FullKey::new(
        table_id,
        TableKey(VirtualNode::from_index(vnode).to_be_bytes().to_vec()),
        u64::MAX,
    )
    .encode()
    .into()
}

fn boundary_in_key_range(boundary: &[u8], key_range: &KeyRange) -> bool {
    if key_range.left.is_empty() || key_range.right.is_empty() {
        return false;
    }

    // Require boundary > left to avoid generating empty left range.
    if !KeyComparator::compare_encoded_full_key(boundary, &key_range.left).is_gt() {
        return false;
    }

    // Require boundary < right. `boundary == right` doesn't indicate cross-partition and would
    // create a tiny tail range.
    KeyComparator::compare_encoded_full_key(boundary, &key_range.right).is_lt()
}

pub fn count_boundaries_in_key_range(key_range: &KeyRange, boundary_keys: &[Bytes]) -> usize {
    boundary_keys
        .iter()
        .filter(|b| boundary_in_key_range(b.as_ref(), key_range))
        .count()
}

pub fn split_sst_by_boundary_keys(
    sst: SstableInfo,
    boundary_keys: &[Bytes],
    new_sst_id: &mut HummockSstableId,
) -> Vec<SstableInfo> {
    let key_range = &sst.key_range;
    let split_points: Vec<_> = boundary_keys
        .iter()
        .filter(|b| boundary_in_key_range(b.as_ref(), key_range))
        .cloned()
        .collect();

    if split_points.is_empty() {
        return vec![sst];
    }

    let piece_count = split_points.len() + 1;
    let sizes = allocate_sstable_split_sizes(sst.sst_size, piece_count);
    debug_assert_eq!(sizes.len(), piece_count);

    let mut pieces = Vec::with_capacity(piece_count);

    // First piece reuses the original `sst_id` (this SST is newly added in this commit anyway).
    let base = sst.get_inner();
    let mut first = base.clone();
    first.key_range = KeyRange {
        left: key_range.left.clone(),
        right: split_points[0].clone(),
        right_exclusive: true,
    };
    first.sst_size = sizes[0];
    pieces.push(first.into());

    for i in 0..split_points.len() {
        let left = split_points[i].clone();
        let (right, right_exclusive) = if i + 1 < split_points.len() {
            (split_points[i + 1].clone(), true)
        } else {
            (key_range.right.clone(), key_range.right_exclusive)
        };

        let mut inner = base.clone();
        inner.sst_id = *new_sst_id;
        *new_sst_id += 1;
        inner.key_range = KeyRange {
            left,
            right,
            right_exclusive,
        };
        inner.sst_size = sizes[i + 1];
        pieces.push(inner.into());
    }

    pieces
}

fn allocate_sstable_split_sizes(total: u64, parts: usize) -> Vec<u64> {
    if parts == 0 {
        return vec![];
    }
    let base = total / parts as u64;
    let remainder = (total % parts as u64) as usize;
    (0..parts)
        .map(|i| std::cmp::max(1, base + if i < remainder { 1 } else { 0 }))
        .collect()
}

pub fn build_nonoverlapping_layers(mut ssts: Vec<SstableInfo>) -> Vec<Vec<SstableInfo>> {
    ssts.sort_by(|a, b| a.key_range.cmp(&b.key_range));

    let mut layers: Vec<Vec<SstableInfo>> = vec![];
    let mut last_ranges: Vec<KeyRange> = vec![];

    for sst in ssts {
        let sst_key_range = sst.key_range.clone();
        let target_layer_idx = last_ranges
            .iter()
            .position(|last| !last.sstable_overlap(&sst_key_range));
        if let Some(idx) = target_layer_idx {
            layers[idx].push(sst);
            last_ranges[idx] = sst_key_range;
        } else {
            last_ranges.push(sst_key_range);
            layers.push(vec![sst]);
        }
    }

    debug_assert!(
        layers
            .iter()
            .all(|layer| risingwave_hummock_sdk::can_concat(layer))
    );
    layers
}

pub fn sort_layers_by_max_epoch_asc(layers: &mut Vec<Vec<SstableInfo>>) {
    layers.sort_by_key(|layer| layer.iter().map(|sst| sst.max_epoch).max().unwrap_or(0));
}

pub fn chunk_nonoverlapping_layer_by_size(
    layer: Vec<SstableInfo>,
    sub_level_size_limit: u64,
) -> Vec<Vec<SstableInfo>> {
    let mut out = vec![];
    let mut current = vec![];
    let mut accumulated_size = 0u64;

    for sst in layer {
        accumulated_size += sst.sst_size;
        current.push(sst);
        if accumulated_size > sub_level_size_limit {
            out.push(current);
            current = vec![];
            accumulated_size = 0;
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;

    use super::*;

    fn fk(table_id: TableId, vnode: usize) -> Bytes {
        FullKey::new(
            table_id,
            TableKey(VirtualNode::from_index(vnode).to_be_bytes().to_vec()),
            u64::MAX,
        )
        .encode()
        .into()
    }

    #[test]
    fn test_build_vnode_partition_boundary_keys() {
        let table_id = TableId::new(1);
        let keys = build_vnode_partition_boundary_keys(table_id, 256, 8);
        assert_eq!(keys.len(), 7);
        assert_eq!(keys[0], fk(table_id, 32));
        assert_eq!(keys[6], fk(table_id, 224));
    }

    #[test]
    fn test_count_boundaries_in_key_range() {
        let table_id = TableId::new(1);
        let keys = build_vnode_partition_boundary_keys(table_id, 256, 8);
        let kr = KeyRange {
            left: fk(table_id, 0),
            right: fk(table_id, 64),
            right_exclusive: true,
        };
        assert_eq!(count_boundaries_in_key_range(&kr, &keys), 1);
    }

    #[test]
    fn test_split_sst_by_boundary_keys() {
        let table_id = TableId::new(1);
        let keys = build_vnode_partition_boundary_keys(table_id, 256, 8);
        let sst: SstableInfo = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 10.into(),
            key_range: KeyRange {
                left: fk(table_id, 0),
                right: fk(table_id, 64),
                right_exclusive: true,
            },
            file_size: 10,
            table_ids: vec![table_id],
            meta_offset: 0,
            stale_key_count: 0,
            total_key_count: 0,
            min_epoch: 1,
            max_epoch: 1,
            uncompressed_file_size: 10,
            range_tombstone_count: 0,
            bloom_filter_kind: Default::default(),
            sst_size: 10,
        }
        .into();

        let mut new_sst_id: HummockSstableId = 100u64.into();
        let pieces = split_sst_by_boundary_keys(sst, &keys, &mut new_sst_id);
        assert_eq!(pieces.len(), 2);
        let sst_id_10: HummockSstableId = 10u64.into();
        let sst_id_100: HummockSstableId = 100u64.into();
        assert_eq!(pieces[0].sst_id, sst_id_10);
        assert_eq!(pieces[0].key_range.left, fk(table_id, 0));
        assert_eq!(pieces[0].key_range.right, fk(table_id, 32));
        assert!(pieces[0].key_range.right_exclusive);
        assert_eq!(pieces[1].sst_id, sst_id_100);
        assert_eq!(pieces[1].key_range.left, fk(table_id, 32));
        assert_eq!(pieces[1].key_range.right, fk(table_id, 64));
        assert!(pieces[1].key_range.right_exclusive);
    }

    #[test]
    fn test_build_nonoverlapping_layers() {
        let table_id = TableId::new(1);
        let make_sst = |sst_id: u64, left_v: usize, right_v: usize| {
            SstableInfoInner {
                object_id: sst_id.into(),
                sst_id: sst_id.into(),
                key_range: KeyRange {
                    left: fk(table_id, left_v),
                    right: fk(table_id, right_v),
                    right_exclusive: true,
                },
                file_size: 10,
                table_ids: vec![table_id],
                meta_offset: 0,
                stale_key_count: 0,
                total_key_count: 0,
                min_epoch: 1,
                max_epoch: 1,
                uncompressed_file_size: 10,
                range_tombstone_count: 0,
                bloom_filter_kind: Default::default(),
                sst_size: 10,
            }
            .into()
        };

        let ssts = vec![
            make_sst(1, 0, 64),
            make_sst(2, 32, 96),
            make_sst(3, 96, 128),
        ];
        let layers = build_nonoverlapping_layers(ssts);
        assert_eq!(layers.len(), 2);
        for layer in layers {
            assert!(risingwave_hummock_sdk::can_concat(&layer));
        }
    }
}
