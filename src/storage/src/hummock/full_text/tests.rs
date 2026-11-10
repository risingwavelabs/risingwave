// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;

use super::*;

fn tokens(words: &[&str]) -> Vec<String> {
    words.iter().map(|word| word.to_string()).collect()
}

fn keys(keys: &[&str]) -> Vec<Bytes> {
    keys.iter()
        .map(|key| Bytes::from(key.to_string()))
        .collect()
}

fn build(docs: &[(&str, u64, &[&str])]) -> BuiltSegment {
    let mut builder = SegmentBuilder::new().unwrap();
    for (i, (key, epoch, words)) in docs.iter().enumerate() {
        let doc_id = builder.add(key.as_bytes(), *epoch, &tokens(words)).unwrap();
        assert_eq!(doc_id, i as u32);
    }
    builder.finish().unwrap().unwrap()
}

fn open(segment: &BuiltSegment) -> OpenedSegment {
    let deletion_vector = segment.deletion_vector.as_ref().map(|(dv, _)| dv.clone());
    OpenedSegment::open(
        &segment.segment_meta,
        segment.bundle.clone(),
        deletion_vector,
    )
    .unwrap()
}

fn search(segment: &OpenedSegment, words: &[&str]) -> Vec<Bytes> {
    segment.search(&tokens(words)).unwrap()
}

fn markers(markers: &[(&str, u64)]) -> Vec<(Bytes, u64)> {
    markers
        .iter()
        .map(|(key, epoch)| (Bytes::from(key.to_string()), *epoch))
        .collect()
}

#[test]
fn test_search_when_other_postings_start_after_lead() {
    // "banana" leads the intersection at doc 0, while "cherry" starts at doc 1.
    let built = build(&[
        ("k1", 1, &["apple", "banana"]),
        ("k2", 1, &["apple", "cherry"]),
        ("k3", 1, &["banana", "cherry"]),
        ("k4", 1, &["banana", "durian"]),
        ("k5", 1, &["cherry", "durian"]),
    ]);
    let segment = open(&built);
    assert_eq!(search(&segment, &["banana", "cherry"]), keys(&["k3"]));
    assert_eq!(search(&segment, &["cherry", "banana"]), keys(&["k3"]));
    assert_eq!(search(&segment, &["durian", "cherry"]), keys(&["k5"]));
    assert_eq!(search(&segment, &["apple", "durian"]), keys(&[]));
}

#[test]
fn test_search_filtered() {
    // After merging, key "a" has an old doc at epoch 1 and a newer one at epoch 3.
    let first = open(&build(&[
        ("a", 1, &["x", "old"]),
        ("b", 1, &["x"]),
        ("c", 2, &["x"]),
    ]));
    let second = open(&build(&[("a", 3, &["x", "new"]), ("d", 3, &["x"])]));
    let merged = open(&merge_segments(&[first, second]).unwrap().unwrap());
    let x = tokens(&["x"]);

    let no_markers = |_: &[u8], _: u64| false;
    assert_eq!(
        merged.search_filtered(&x, Some(&no_markers), None).unwrap(),
        keys(&["a", "b", "c", "d"])
    );

    // A marker of "a" at epoch 3 deletes only the doc written before epoch 3.
    let markers = [(Bytes::from("a"), 3), (Bytes::from("c"), 3)];
    let is_deleted = |key: &[u8], epoch: u64| {
        markers
            .iter()
            .any(|(marker_key, marker_epoch)| marker_key.as_ref() == key && epoch < *marker_epoch)
    };
    assert_eq!(
        merged.search_filtered(&x, Some(&is_deleted), None).unwrap(),
        keys(&["a", "b", "d"])
    );
    assert_eq!(
        merged
            .search_filtered(&tokens(&["old"]), Some(&is_deleted), None)
            .unwrap(),
        keys(&[])
    );
    assert_eq!(
        merged
            .search_filtered(&tokens(&["new"]), Some(&is_deleted), None)
            .unwrap(),
        keys(&["a"])
    );

    // A later marker deletes both docs of "a".
    let delete_all_a = |key: &[u8], epoch: u64| key == b"a" && epoch < 4;
    assert_eq!(
        merged
            .search_filtered(&x, Some(&delete_all_a), None)
            .unwrap(),
        keys(&["b", "c", "d"])
    );

    assert_eq!(
        merged
            .search_filtered(&x, Some(&is_deleted), Some(2))
            .unwrap(),
        keys(&["a", "b"])
    );
    assert_eq!(merged.search_filtered(&x, None, Some(3)).unwrap().len(), 3);
}
#[test]
fn test_limit_stops_after_live_distinct_keys() {
    use std::cell::Cell;

    let built = build(&[
        ("a", 1, &["x"]),
        ("a", 2, &["x"]),
        ("a", 3, &["x"]),
        ("b", 3, &["x"]),
        ("c", 3, &["x"]),
        ("d", 3, &["x"]),
        ("e", 3, &["x"]),
        ("f", 3, &["x"]),
    ]);
    let segment = open(&built);
    let checked = Cell::new(0);
    let is_deleted = |key: &[u8], epoch: u64| {
        checked.set(checked.get() + 1);
        key == b"a" && epoch < 3
    };
    assert_eq!(
        segment
            .search_filtered(&tokens(&["x"]), Some(&is_deleted), Some(2))
            .unwrap(),
        keys(&["a", "b"])
    );
    assert_eq!(checked.get(), 4, "do not check the remaining matches");
    checked.set(0);
    assert!(
        segment
            .search_filtered(&tokens(&["x"]), Some(&is_deleted), Some(0))
            .unwrap()
            .is_empty()
    );
    assert_eq!(checked.get(), 0);
}

#[test]
fn test_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OpenedSegment>();
    assert_send_sync::<SegmentBuilder>();
}

#[test]
fn test_build_and_search() {
    let built = build(&[
        ("k1", 1, &["hello", "world"]),
        ("k2", 1, &["hello"]),
        ("k3", 2, &["world", "foo", "world"]),
        ("k4", 3, &["helloworld"]),
        ("k0", 2, &[]),
    ]);
    assert_eq!(built.num_docs, 5);
    assert_eq!(built.min_epoch, 1);
    assert_eq!(built.max_epoch, 3);
    assert_eq!(built.min_key, Bytes::from("k0"));
    assert_eq!(built.max_key, Bytes::from("k4"));
    assert!(built.deletion_vector.is_none());

    let segment = open(&built);
    assert_eq!(segment.num_docs(), 5);
    assert_eq!(segment.num_deleted(), 0);
    assert_eq!(search(&segment, &["hello"]), keys(&["k1", "k2"]));
    assert_eq!(search(&segment, &["world"]), keys(&["k1", "k3"]));
    // AND semantics, with duplicated query tokens.
    assert_eq!(search(&segment, &["world", "hello"]), keys(&["k1"]));
    assert_eq!(
        search(&segment, &["hello", "hello", "world"]),
        keys(&["k1"])
    );
    assert_eq!(search(&segment, &["foo", "hello"]), keys(&[]));
    // Tokens match exactly: no substrings, no case folding.
    assert_eq!(search(&segment, &["hell"]), keys(&[]));
    assert_eq!(search(&segment, &["Hello"]), keys(&[]));
    assert_eq!(search(&segment, &["helloworld"]), keys(&["k4"]));
    assert_eq!(search(&segment, &["missing"]), keys(&[]));
    assert!(segment.search(&[]).is_err());

    assert_eq!(
        segment.alive_docs().unwrap(),
        vec![
            (Bytes::from("k1"), 1),
            (Bytes::from("k2"), 1),
            (Bytes::from("k3"), 2),
            (Bytes::from("k4"), 3),
            (Bytes::from("k0"), 2),
        ]
    );
}

#[test]
fn test_empty_builder() {
    let builder = SegmentBuilder::new().unwrap();
    assert!(builder.is_empty());
    assert_eq!(builder.num_docs(), 0);
    assert!(builder.finish().unwrap().is_none());
}

#[test]
fn test_builder_size_grows() {
    let mut builder = SegmentBuilder::new().unwrap();
    let initial = builder.estimated_size();
    for i in 0..10_000u32 {
        builder
            .add(
                &i.to_be_bytes(),
                1,
                &[format!("token{i}"), "common".to_owned()],
            )
            .unwrap();
    }
    assert!(!builder.is_empty());
    assert_eq!(builder.num_docs(), 10_000);
    assert!(builder.estimated_size() > initial);
}

#[test]
fn test_builder_delete() {
    let mut builder = SegmentBuilder::new().unwrap();
    builder.add(b"a", 1, &tokens(&["x"])).unwrap();
    let b = builder.add(b"b", 1, &tokens(&["x"])).unwrap();
    builder.add(b"c", 1, &tokens(&["x", "y"])).unwrap();
    builder.delete(b);
    // Deleting twice counts once.
    builder.delete(b);
    let built = builder.finish().unwrap().unwrap();
    assert_eq!(built.num_docs, 3);
    let (_, num_deleted) = built.deletion_vector.as_ref().unwrap();
    assert_eq!(*num_deleted, 1);
    // The key range covers deleted docs too.
    assert_eq!(built.min_key, Bytes::from("a"));
    assert_eq!(built.max_key, Bytes::from("c"));

    let segment = open(&built);
    assert_eq!(segment.num_docs(), 3);
    assert_eq!(segment.num_deleted(), 1);
    assert_eq!(search(&segment, &["x"]), keys(&["a", "c"]));

    // The deletion vector is a separate object: without it, the doc is visible.
    let without_dv = OpenedSegment::open(&built.segment_meta, built.bundle.clone(), None).unwrap();
    assert_eq!(without_dv.num_deleted(), 0);
    assert_eq!(search(&without_dv, &["x"]), keys(&["a", "b", "c"]));
}

#[test]
fn test_builder_delete_all() {
    let mut builder = SegmentBuilder::new().unwrap();
    for key in ["a", "b"] {
        let doc_id = builder.add(key.as_bytes(), 1, &tokens(&["x"])).unwrap();
        builder.delete(doc_id);
    }
    let built = builder.finish().unwrap().unwrap();
    assert_eq!(built.deletion_vector.as_ref().unwrap().1, 2);
    let segment = open(&built);
    assert_eq!(segment.num_deleted(), 2);
    assert_eq!(search(&segment, &["x"]), keys(&[]));
    assert!(merge_segments(&[segment]).unwrap().is_none());
}

#[test]
fn test_resolve_deletes() {
    let built = build(&[
        ("a", 1, &["x"]),
        ("b", 1, &["x"]),
        // "a" re-inserted at a later epoch.
        ("a", 3, &["x", "y"]),
        ("c", 2, &["x"]),
    ]);
    let segment = open(&built);

    // Unknown keys and markers not older than the doc delete nothing.
    assert!(
        segment
            .resolve_deletes(&markers(&[("aa", 10), ("c", 2), ("zzz", 10)]))
            .unwrap()
            .is_none()
    );
    assert!(segment.resolve_deletes(&[]).unwrap().is_none());

    // Deletes only the old version of "a".
    let (dv, num_deleted) = segment
        .resolve_deletes(&markers(&[("a", 2)]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 1);
    let segment = OpenedSegment::open(&built.segment_meta, built.bundle.clone(), Some(dv)).unwrap();
    assert_eq!(segment.num_deleted(), 1);
    assert_eq!(search(&segment, &["x"]), keys(&["a", "b", "c"]));
    assert_eq!(search(&segment, &["y"]), keys(&["a"]));
    assert_eq!(
        segment.alive_docs().unwrap(),
        vec![
            (Bytes::from("b"), 1),
            (Bytes::from("a"), 3),
            (Bytes::from("c"), 2)
        ]
    );

    // Already deleted docs are not deleted again.
    assert!(
        segment
            .resolve_deletes(&markers(&[("a", 2)]))
            .unwrap()
            .is_none()
    );

    // Deletions accumulate, and repeated keys use the largest marker epoch.
    let (dv, num_deleted) = segment
        .resolve_deletes(&markers(&[("a", 2), ("a", 4), ("b", 5)]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 3);
    let segment = OpenedSegment::open(&built.segment_meta, built.bundle.clone(), Some(dv)).unwrap();
    assert_eq!(segment.num_deleted(), 3);
    assert_eq!(search(&segment, &["x"]), keys(&["c"]));
    assert_eq!(search(&segment, &["y"]), keys(&[]));
}

#[test]
fn test_resolve_deletes_on_builder_dv() {
    let mut builder = SegmentBuilder::new().unwrap();
    let a = builder.add(b"a", 1, &tokens(&["x"])).unwrap();
    builder.add(b"b", 1, &tokens(&["x"])).unwrap();
    builder.delete(a);
    let built = builder.finish().unwrap().unwrap();
    let segment = open(&built);
    assert!(
        segment
            .resolve_deletes(&markers(&[("a", 5)]))
            .unwrap()
            .is_none()
    );
    let (_, num_deleted) = segment
        .resolve_deletes(&markers(&[("a", 5), ("b", 5)]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 2);
}

#[test]
fn test_merge() {
    let first = build(&[("a", 1, &["x", "y"]), ("b", 1, &["x"]), ("c", 2, &["y"])]);
    let second = build(&[("b", 3, &["x", "z"]), ("d", 4, &["z"]), ("e", 5, &["x"])]);
    let first_segment = open(&first);
    let (dv, _) = first_segment
        .resolve_deletes(&markers(&[("a", 2), ("b", 3)]))
        .unwrap()
        .unwrap();
    let first_segment =
        OpenedSegment::open(&first.segment_meta, first.bundle.clone(), Some(dv)).unwrap();
    let second_segment = open(&second);
    let (dv, _) = second_segment
        .resolve_deletes(&markers(&[("e", 6)]))
        .unwrap()
        .unwrap();
    let second_segment =
        OpenedSegment::open(&second.segment_meta, second.bundle.clone(), Some(dv)).unwrap();

    // Callers may pass references too.
    let merged = merge_segments(&[&first_segment, &second_segment])
        .unwrap()
        .unwrap();
    assert_eq!(merged.num_docs, 3);
    assert!(merged.deletion_vector.is_none());
    assert_eq!(merged.min_key, Bytes::from("b"));
    assert_eq!(merged.max_key, Bytes::from("d"));
    assert_eq!(merged.min_epoch, 2);
    assert_eq!(merged.max_epoch, 4);

    let merged_segment = open(&merged);
    assert_eq!(merged_segment.num_docs(), 3);
    assert_eq!(merged_segment.num_deleted(), 0);
    assert_eq!(
        merged_segment.alive_docs().unwrap(),
        vec![
            (Bytes::from("c"), 2),
            (Bytes::from("b"), 3),
            (Bytes::from("d"), 4),
        ]
    );
    assert_eq!(search(&merged_segment, &["x"]), keys(&["b"]));
    assert_eq!(search(&merged_segment, &["y"]), keys(&["c"]));
    assert_eq!(search(&merged_segment, &["z"]), keys(&["b", "d"]));
    assert_eq!(search(&merged_segment, &["x", "z"]), keys(&["b"]));
    assert_eq!(search(&merged_segment, &["x", "y"]), keys(&[]));

    // Deletes can still be resolved on the merged segment, by key and epoch.
    let (dv, num_deleted) = merged_segment
        .resolve_deletes(&markers(&[("b", 4)]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 1);
    let merged_segment =
        OpenedSegment::open(&merged.segment_meta, merged.bundle.clone(), Some(dv)).unwrap();
    assert_eq!(search(&merged_segment, &["z"]), keys(&["d"]));

    // Merging again drops the newly deleted doc.
    let merged_again = merge_segments(&[merged_segment]).unwrap().unwrap();
    assert_eq!(merged_again.num_docs, 2);
    assert_eq!(
        open(&merged_again).alive_docs().unwrap(),
        vec![(Bytes::from("c"), 2), (Bytes::from("d"), 4)]
    );
}

#[test]
fn test_merge_all_deleted() {
    let built = build(&[("a", 1, &["x"]), ("b", 1, &["y"])]);
    let segment = open(&built);
    let (dv, num_deleted) = segment
        .resolve_deletes(&markers(&[("a", 2), ("b", 2)]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 2);
    let segment = OpenedSegment::open(&built.segment_meta, built.bundle.clone(), Some(dv)).unwrap();
    assert!(merge_segments(&[segment]).unwrap().is_none());
    assert!(merge_segments::<OpenedSegment>(&[]).unwrap().is_none());
}

#[test]
fn test_delete_file() {
    let deleted = keys(&["", "a", "ab", "b"]);
    let encoded = encode_delete_file(42, &deleted);
    assert_eq!(decode_delete_file(&encoded).unwrap(), (42, deleted));

    let empty = encode_delete_file(7, &[]);
    assert_eq!(decode_delete_file(&empty).unwrap(), (7, vec![]));

    for i in 0..encoded.len() {
        let mut corrupted = encoded.to_vec();
        corrupted[i] ^= 0x10;
        assert!(decode_delete_file(&corrupted).is_err(), "byte {i}");
    }
    assert!(decode_delete_file(&encoded[..encoded.len() - 1]).is_err());
    assert!(decode_delete_file(&[]).is_err());
}

#[test]
fn test_corruption_detected() {
    let mut builder = SegmentBuilder::new().unwrap();
    builder.add(b"a", 1, &tokens(&["x"])).unwrap();
    let b = builder.add(b"b", 1, &tokens(&["x"])).unwrap();
    builder.delete(b);
    let built = builder.finish().unwrap().unwrap();
    let (dv, _) = built.deletion_vector.clone().unwrap();
    let open_with = |segment_meta: &[u8], bundle: Bytes, dv: Option<Bytes>| {
        OpenedSegment::open(segment_meta, bundle, dv)
    };

    // Any flipped byte in the bundle is detected.
    let len = built.bundle.len();
    for i in [0, len / 3, len / 2, len - 30, len - 20, len - 12, len - 1] {
        let mut corrupted = built.bundle.to_vec();
        corrupted[i] ^= 0x01;
        assert!(
            open_with(&built.segment_meta, Bytes::from(corrupted), None).is_err(),
            "byte {i}"
        );
    }
    // Truncated bundles.
    assert!(open_with(&built.segment_meta, built.bundle.slice(..len - 1), None).is_err());
    assert!(open_with(&built.segment_meta, built.bundle.slice(..10), None).is_err());
    assert!(open_with(&built.segment_meta, Bytes::new(), None).is_err());

    // Bad segment meta.
    assert!(open_with(&built.segment_meta[..10], built.bundle.clone(), None).is_err());
    let mut bad_meta = built.segment_meta.to_vec();
    bad_meta[0] ^= 0x01;
    assert!(open_with(&bad_meta, built.bundle.clone(), None).is_err());
    // A segment meta pointing at another segment.
    let other = build(&[("z", 1, &["x"])]);
    assert!(open_with(&other.segment_meta, built.bundle.clone(), None).is_err());

    // Bad deletion vectors.
    let mut bad_dv = dv.to_vec();
    bad_dv[4] ^= 0x01;
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.clone(),
            Some(Bytes::from(bad_dv))
        )
        .is_err()
    );
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.clone(),
            Some(dv.slice(..3))
        )
        .is_err()
    );
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.clone(),
            Some(Bytes::new())
        )
        .is_err()
    );
    // A deletion vector of a segment with a different number of docs.
    let mut bigger = SegmentBuilder::new().unwrap();
    for i in 0..100u32 {
        let doc_id = bigger.add(&i.to_be_bytes(), 1, &tokens(&["x"])).unwrap();
        bigger.delete(doc_id);
    }
    let (bigger_dv, _) = bigger.finish().unwrap().unwrap().deletion_vector.unwrap();
    assert!(open_with(&built.segment_meta, built.bundle.clone(), Some(bigger_dv)).is_err());

    // The intact objects still open.
    let segment = open_with(&built.segment_meta, built.bundle.clone(), Some(dv)).unwrap();
    assert_eq!(search(&segment, &["x"]), keys(&["a"]));
}

#[test]
fn test_large_segment() {
    const NUM_DOCS: u32 = 50_000;
    let key = |i: u32| Bytes::copy_from_slice(&i.to_be_bytes());
    let mut builder = SegmentBuilder::new().unwrap();
    for i in 0..NUM_DOCS {
        let doc_tokens = vec!["all".to_owned(), format!("m{}", i % 10), format!("u{i}")];
        builder.add(&key(i), i as u64 / 1000, &doc_tokens).unwrap();
        if i.is_multiple_of(7) {
            builder.delete(i);
        }
    }
    let built = builder.finish().unwrap().unwrap();
    let deleted_in_builder = NUM_DOCS.div_ceil(7) as u64;
    assert_eq!(built.num_docs, NUM_DOCS as u64);
    assert_eq!(
        built.deletion_vector.as_ref().unwrap().1,
        deleted_in_builder
    );
    assert_eq!(built.min_key, key(0));
    assert_eq!(built.max_key, key(NUM_DOCS - 1));

    let segment = open(&built);
    let expected = |filter: &dyn Fn(u32) -> bool| -> Vec<Bytes> {
        (0..NUM_DOCS).filter(|&i| filter(i)).map(key).collect()
    };
    assert_eq!(
        segment.search(&tokens(&["all", "m3"])).unwrap(),
        expected(&|i| i % 10 == 3 && !i.is_multiple_of(7))
    );
    assert_eq!(search(&segment, &["u12345"]), vec![key(12345)]);
    assert_eq!(search(&segment, &["u14"]), Vec::<Bytes>::new());

    // Delete every 3rd doc of the first half, with markers newer than every epoch.
    let delete_markers: Vec<(Bytes, u64)> = (0..NUM_DOCS / 2)
        .filter(|i| i.is_multiple_of(3))
        .map(|i| (key(i), u64::MAX))
        .collect();
    let (dv, num_deleted) = segment.resolve_deletes(&delete_markers).unwrap().unwrap();
    let is_alive = |i: u32| !i.is_multiple_of(7) && !(i < NUM_DOCS / 2 && i.is_multiple_of(3));
    let num_alive = (0..NUM_DOCS).filter(|&i| is_alive(i)).count() as u64;
    assert_eq!(num_deleted, NUM_DOCS as u64 - num_alive);
    let segment = OpenedSegment::open(&built.segment_meta, built.bundle.clone(), Some(dv)).unwrap();
    assert_eq!(
        segment.search(&tokens(&["all"])).unwrap(),
        expected(&|i| is_alive(i))
    );

    let merged = merge_segments(&[segment]).unwrap().unwrap();
    assert_eq!(merged.num_docs, num_alive);
    let merged_segment = open(&merged);
    assert_eq!(
        merged_segment.search(&tokens(&["m3", "all"])).unwrap(),
        expected(&|i| is_alive(i) && i % 10 == 3)
    );
}
