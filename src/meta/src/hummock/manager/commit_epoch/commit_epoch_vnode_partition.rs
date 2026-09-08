use bytes::Bytes;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_at_vnode_boundary;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockSstableId, KeyComparator};

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
    let mut remaining = sst;
    for (idx, split_key) in split_points.into_iter().enumerate() {
        let right_size = sizes[idx + 1..].iter().sum();
        let (left, right) =
            split_sst_at_vnode_boundary(remaining, new_sst_id, split_key, sizes[idx], right_size);
        pieces.push(left.expect("vnode boundary must leave a non-empty left SST"));
        remaining = right.expect("vnode boundary must leave a non-empty right SST");
    }
    pieces.push(remaining);

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

/// Build layers from SSTs in uploader order (newest first), returning oldest first for commit.
/// Overlapping older SSTs must be below every overlapping newer SST. Place each SST at the
/// shallowest depth satisfying that constraint; disjoint SSTs can still share a layer.
pub fn build_nonoverlapping_layers(ssts: Vec<SstableInfo>) -> Vec<Vec<SstableInfo>> {
    // Layer indices are read depths here: newer layers have smaller indices.
    let mut layers: Vec<Vec<SstableInfo>> = vec![];

    for sst in ssts {
        let target_layer_idx = layers
            .iter()
            .rposition(|layer| {
                layer
                    .iter()
                    .any(|newer| newer.key_range.sstable_overlap(&sst.key_range))
            })
            .map_or(0, |idx| idx + 1);
        if target_layer_idx == layers.len() {
            layers.push(vec![]);
        }
        layers[target_layer_idx].push(sst);
    }

    for layer in &mut layers {
        layer.sort_by(|a, b| a.key_range.cmp(&b.key_range));
    }

    debug_assert!(
        layers
            .iter()
            .all(|layer| risingwave_hummock_sdk::can_concat(layer))
    );
    // Hummock stores L0 sub-levels oldest first and reads them in reverse order.
    layers.reverse();
    layers
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
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_hummock_sdk::vnode_partition::build_vnode_partition_boundary_keys;

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
            filter_type: Default::default(),
            filter_layout: Default::default(),
            vnode_statistics: None,
            sst_size: 10,
        }
        .into();

        let mut new_sst_id: HummockSstableId = 100u64.into();
        let pieces = split_sst_by_boundary_keys(sst, &keys, &mut new_sst_id);
        assert_eq!(pieces.len(), 2);
        let sst_id_100: HummockSstableId = 100u64.into();
        let sst_id_101: HummockSstableId = 101u64.into();
        let sst_id_102: HummockSstableId = 102u64.into();
        assert_eq!(pieces[0].sst_id, sst_id_101);
        assert_eq!(pieces[0].key_range.left, fk(table_id, 0));
        assert_eq!(pieces[0].key_range.right, fk(table_id, 32));
        assert!(pieces[0].key_range.right_exclusive);
        assert_eq!(pieces[1].sst_id, sst_id_100);
        assert_eq!(pieces[1].key_range.left, fk(table_id, 32));
        assert_eq!(pieces[1].key_range.right, fk(table_id, 64));
        assert!(pieces[1].key_range.right_exclusive);
        assert_eq!(pieces[0].table_ids, vec![table_id]);
        assert_eq!(pieces[1].table_ids, vec![table_id]);
        assert_eq!(new_sst_id, sst_id_102);
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
                filter_type: Default::default(),
                filter_layout: Default::default(),
                vnode_statistics: None,
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

    fn ordered_sst(id: u64, left: usize, right: usize, epoch: u64) -> SstableInfo {
        let epoch = test_epoch(epoch);
        let table_id = TableId::new(1);
        let key = |vnode| {
            FullKey::new(
                table_id,
                TableKey(VirtualNode::from_index(vnode).to_be_bytes().to_vec()),
                epoch,
            )
            .encode()
            .into()
        };
        SstableInfoInner {
            object_id: id.into(),
            sst_id: id.into(),
            key_range: KeyRange::new(key(left), key(right)),
            table_ids: vec![table_id],
            min_epoch: epoch,
            max_epoch: epoch,
            sst_size: 10,
            ..Default::default()
        }
        .into()
    }

    fn layer_ids(layers: &[Vec<SstableInfo>]) -> Vec<Vec<HummockSstableId>> {
        layers
            .iter()
            .map(|layer| layer.iter().map(|sst| sst.sst_id).collect())
            .collect()
    }

    #[test]
    fn test_disjoint_newer_sst_does_not_reorder_overlapping_versions() {
        // Newest first: C is disjoint from both A and B, but A and B overlap.
        // Sorting layers by max_epoch used to put {A, C} ahead of the newer B.
        let c = ordered_sst(3, 40, 50, 30);
        let b = ordered_sst(2, 10, 30, 20);
        let a = ordered_sst(1, 0, 20, 10);
        let layers = build_nonoverlapping_layers(vec![c.clone(), b.clone(), a.clone()]);
        assert_eq!(
            layer_ids(&layers),
            vec![vec![a.sst_id], vec![b.sst_id, c.sst_id]]
        );

        // Model a point present in both A and B at different snapshots. Hummock reads layers
        // newest first and returns the first visible version, rather than merging point results.
        let point = KeyRange::new(fk(TableId::new(1), 15), fk(TableId::new(1), 15));
        for (read_epoch, expected) in [(9, None), (10, Some(10)), (20, Some(20)), (30, Some(20))] {
            let first_visible = layers
                .iter()
                .rev()
                .flatten()
                .find(|sst| {
                    sst.max_epoch <= test_epoch(read_epoch) && sst.key_range.sstable_overlap(&point)
                })
                .map(|sst| sst.max_epoch);
            assert_eq!(first_visible, expected.map(test_epoch));
        }
    }

    #[test]
    fn test_do_not_fill_a_gap_above_an_overlapping_newer_sst() {
        let newest = ordered_sst(3, 0, 1, 30);
        let middle = ordered_sst(2, 0, 3, 20);
        let oldest = ordered_sst(1, 2, 3, 10);
        // The oldest SST fits beside `newest`, but must stay below `middle`.
        let layers =
            build_nonoverlapping_layers(vec![newest.clone(), middle.clone(), oldest.clone()]);
        assert_eq!(
            layer_ids(&layers),
            vec![
                vec![oldest.sst_id],
                vec![middle.sst_id],
                vec![newest.sst_id]
            ]
        );
    }

    #[test]
    fn test_same_user_key_at_different_epochs_cannot_share_a_layer() {
        let newest = ordered_sst(2, 15, 15, 20);
        let oldest = ordered_sst(1, 15, 15, 10);
        assert!(!newest.key_range.full_key_overlap(&oldest.key_range));
        assert!(newest.key_range.sstable_overlap(&oldest.key_range));
        let layers = build_nonoverlapping_layers(vec![newest.clone(), oldest.clone()]);
        assert_eq!(
            layer_ids(&layers),
            vec![vec![oldest.sst_id], vec![newest.sst_id]]
        );
    }

    #[test]
    fn test_split_and_chunk_preserve_overlapping_sst_order() {
        let table_id = TableId::new(1);
        let boundaries = build_vnode_partition_boundary_keys(table_id, 256, 8);
        let newest = ordered_sst(2, 0, 64, 20);
        let oldest = ordered_sst(1, 0, 64, 10);
        let mut next_id = 100.into();
        let fragments = [newest.clone(), oldest.clone()]
            .into_iter()
            .flat_map(|sst| split_sst_by_boundary_keys(sst, &boundaries, &mut next_id))
            .collect();
        let layers = build_nonoverlapping_layers(fragments);
        assert_eq!(layers.len(), 2);
        assert!(
            layers
                .iter()
                .all(|layer| risingwave_hummock_sdk::can_concat(layer))
        );
        // Adjacent pieces with exclusive right bounds can share a layer, despite different
        // epochs in their encoded boundary keys. Chunking must not interleave old/new layers.
        assert!(
            layers[0]
                .iter()
                .all(|sst| sst.object_id == oldest.object_id)
        );
        assert!(
            layers[1]
                .iter()
                .all(|sst| sst.object_id == newest.object_id)
        );
        let chunks: Vec<_> = layers
            .into_iter()
            .flat_map(|layer| chunk_nonoverlapping_layer_by_size(layer, 1))
            .collect();
        for vnode in [0, 31, 32, 63, 64] {
            let point = KeyRange::new(fk(table_id, vnode), fk(table_id, vnode));
            let matches: Vec<_> = chunks
                .iter()
                .rev()
                .flatten()
                .filter(|sst| sst.key_range.sstable_overlap(&point))
                .map(|sst| sst.object_id)
                .collect();
            assert_eq!(matches, vec![newest.object_id, oldest.object_id]);
        }
    }

    #[test]
    fn test_exhaustive_layer_order_and_minimum_depth() {
        // Enumerate ordered sequences of four closed intervals, including repeated intervals,
        // nested ranges, touching endpoints, and disjoint ranges. Equal epochs deliberately
        // ensure the uploader's order, not an epoch sort, is the source of precedence.
        let ranges: Vec<_> = (0..3)
            .flat_map(|left| (left..3).map(move |right| (left, right)))
            .collect();
        for mut encoding in 0..ranges.len().pow(4) {
            let mut ssts = vec![];
            for id in 0..4 {
                let (left, right) = ranges[encoding % ranges.len()];
                encoding /= ranges.len();
                ssts.push(ordered_sst(id, left, right, 10));
            }
            let layers = build_nonoverlapping_layers(ssts.clone());
            assert!(
                layers
                    .iter()
                    .all(|layer| risingwave_hummock_sdk::can_concat(layer))
            );
            assert_eq!(layers.iter().map(Vec::len).sum::<usize>(), ssts.len());
            let depths: Vec<_> = ssts
                .iter()
                .map(|sst| {
                    layers
                        .iter()
                        .rev()
                        .position(|layer| layer.iter().any(|s| s.sst_id == sst.sst_id))
                        .unwrap()
                })
                .collect();
            for (older_idx, older) in ssts.iter().enumerate() {
                for (newer_idx, newer) in ssts[..older_idx].iter().enumerate() {
                    if newer.key_range.sstable_overlap(&older.key_range) {
                        assert!(depths[newer_idx] < depths[older_idx]);
                    }
                }
            }
            // A chain of overlapping SSTs ordered by age is a lower bound on layer count.
            // Enumerate all subsequences independently of the placement algorithm.
            let minimum_layers = (1u32..1 << ssts.len())
                .filter_map(|mask| {
                    let chain: Vec<_> = ssts
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| mask & (1 << idx) != 0)
                        .map(|(_, sst)| sst)
                        .collect();
                    chain
                        .windows(2)
                        .all(|pair| pair[0].key_range.sstable_overlap(&pair[1].key_range))
                        .then_some(chain.len())
                })
                .max()
                .unwrap();
            assert_eq!(layers.len(), minimum_layers);
        }
        assert!(build_nonoverlapping_layers(vec![]).is_empty());
    }
}
