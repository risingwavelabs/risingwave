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
use tantivy::schema::IndexRecordOption;

use crate::*;

fn tokens(words: &[&str]) -> Vec<String> {
    words.iter().map(|word| word.to_string()).collect()
}

fn keys(keys: &[&str]) -> Vec<Bytes> {
    keys.iter()
        .map(|key| Bytes::from(key.to_string()))
        .collect()
}

fn add_doc(builder: &mut SegmentBuilder, key: &[u8], words: &[&str]) -> u32 {
    builder
        .add(key, &mut |emit| {
            for word in words {
                emit(word);
            }
        })
        .unwrap()
}

fn build(docs: &[(&str, &[&str])]) -> BuiltSegment {
    let mut builder = SegmentBuilder::new().unwrap();
    for (i, (key, words)) in docs.iter().enumerate() {
        let doc_id = add_doc(&mut builder, key.as_bytes(), words);
        assert_eq!(doc_id, i as u32);
    }
    builder.finish().unwrap().unwrap()
}

fn open(segment: &BuiltSegment) -> OpenedSegment {
    let deletion_vector = segment.deletion_vector.as_ref().map(|(dv, _)| dv.clone());
    OpenedSegment::open(
        &segment.segment_meta,
        segment.bundle.to_bytes(),
        deletion_vector,
    )
    .unwrap()
}

#[test]
fn test_pre_analyzed_tokens_are_indexed_verbatim() {
    let built = build(&[
        ("empty", &[]),
        ("multi", &["Blue sky", "X|Y", "X|Y", "雪\0花"]),
        ("split", &["Blue", "sky"]),
    ]);
    let segment = open(&built);

    assert_eq!(built.num_docs, 3);
    assert_eq!(search(&segment, &["Blue sky", "X|Y"]), keys(&["multi"]));
    assert_eq!(search(&segment, &["X|Y"]), keys(&["multi"]));
    assert_eq!(search(&segment, &["雪\0花"]), keys(&["multi"]));
    assert_eq!(search(&segment, &["Blue", "sky"]), keys(&["split"]));
    assert!(search(&segment, &["blue sky"]).is_empty());
}

#[test]
fn test_builder_consumes_tokens_before_buffer_is_reused() {
    let mut builder = SegmentBuilder::new().unwrap();
    builder
        .add(b"key", &mut |emit| {
            let mut token = String::from("first");
            emit(&token);
            token.clear();
            token.push_str("second");
            emit(&token);
            token.clear();
        })
        .unwrap();
    let segment = open(&builder.finish().unwrap().unwrap());
    assert_eq!(search(&segment, &["first", "second"]), keys(&["key"]));
}

#[test]
fn test_long_keys_search_delete_and_merge() {
    let mut prefix = vec![b'a'; 65_536];
    let first = Bytes::copy_from_slice(&prefix);
    prefix.push(b'b');
    let second = Bytes::from(prefix);
    let short = Bytes::from_static(b"z");
    let mut builder = SegmentBuilder::new().unwrap();
    for key in [&short, &second, &first] {
        add_doc(&mut builder, key, &["common", "old"]);
    }
    let built = builder.finish().unwrap().unwrap();
    let segment = open(&built);
    let expected = vec![first.clone(), second.clone(), short.clone()];
    assert_eq!(search(&segment, &["common"]), expected);
    assert_eq!(
        segment
            .search_filtered(&tokens(&["common"]), None, Some(3))
            .unwrap(),
        expected
    );
    assert_eq!(
        segment
            .search_filtered(&tokens(&["common"]), Some(&|key| key == first), None)
            .unwrap(),
        vec![second.clone(), short.clone()]
    );

    let (dv, num_deleted) = segment
        .resolve_deletes(std::slice::from_ref(&first))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 1);
    let deleted =
        OpenedSegment::open(&built.segment_meta, built.bundle.to_bytes(), Some(dv)).unwrap();
    assert_eq!(
        search(&deleted, &["common"]),
        vec![second.clone(), short.clone()]
    );

    let mut builder = SegmentBuilder::new().unwrap();
    add_doc(&mut builder, &first, &["common", "new"]);
    let newer = open(&builder.finish().unwrap().unwrap());
    let merged = merge_segments(&[deleted, newer]).unwrap().unwrap();
    assert_eq!(merged.min_key, first.clone());
    assert_eq!(merged.max_key, short.clone());
    let opened = open(&merged);
    assert_eq!(search(&opened, &["common"]), expected);
    assert_eq!(
        search(&opened, &["old"]),
        vec![second.clone(), short.clone()]
    );
    let (dv, num_deleted) = opened.resolve_deletes(&[second]).unwrap().unwrap();
    assert_eq!(num_deleted, 1);
    let deleted =
        OpenedSegment::open(&merged.segment_meta, merged.bundle.to_bytes(), Some(dv)).unwrap();
    assert_eq!(deleted.alive_keys(), vec![first.clone(), short.clone()]);
    assert_eq!(search(&deleted, &["common"]), vec![first, short]);
}

fn search(segment: &OpenedSegment, words: &[&str]) -> Vec<Bytes> {
    segment.search(&tokens(words)).unwrap()
}

#[test]
fn test_search_when_other_postings_start_after_lead() {
    // "banana" leads the intersection at doc 0, while "cherry" starts at doc 1.
    let built = build(&[
        ("k1", &["apple", "banana"]),
        ("k2", &["apple", "cherry"]),
        ("k3", &["banana", "cherry"]),
        ("k4", &["banana", "durian"]),
        ("k5", &["cherry", "durian"]),
    ]);
    let segment = open(&built);
    assert_eq!(search(&segment, &["banana", "cherry"]), keys(&["k3"]));
    assert_eq!(search(&segment, &["cherry", "banana"]), keys(&["k3"]));
    assert_eq!(search(&segment, &["durian", "cherry"]), keys(&["k5"]));
    assert_eq!(search(&segment, &["apple", "durian"]), keys(&[]));
}

#[test]
fn test_search_filtered() {
    let segment = open(&build(&[
        ("a", &["x"]),
        ("b", &["x"]),
        ("c", &["x"]),
        ("d", &["x", "y"]),
    ]));
    let x = tokens(&["x"]);

    let no_markers = |_: &[u8]| false;
    assert_eq!(
        segment
            .search_filtered(&x, Some(&no_markers), None)
            .unwrap(),
        keys(&["a", "b", "c", "d"])
    );

    let is_deleted = |key: &[u8]| key == b"b" || key == b"c";
    assert_eq!(
        segment
            .search_filtered(&x, Some(&is_deleted), None)
            .unwrap(),
        keys(&["a", "d"])
    );

    assert_eq!(
        segment
            .search_filtered(&tokens(&["x", "y"]), Some(&is_deleted), Some(2))
            .unwrap(),
        keys(&["d"])
    );
    assert_eq!(segment.search_filtered(&x, None, Some(3)).unwrap().len(), 3);
}
#[test]
fn test_limit_stops_after_live_distinct_keys() {
    use std::cell::Cell;

    let built = build(&[
        ("a", &["x"]),
        ("a", &["x"]),
        ("b", &["x"]),
        ("c", &["x"]),
        ("d", &["x"]),
        ("e", &["x"]),
        ("f", &["x"]),
    ]);
    let segment = open(&built);
    let checked = Cell::new(0);
    let is_deleted = |key: &[u8]| {
        checked.set(checked.get() + 1);
        key == b"b"
    };
    assert_eq!(
        segment
            .search_filtered(&tokens(&["x"]), Some(&is_deleted), Some(2))
            .unwrap(),
        keys(&["a", "c"])
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
        ("k1", &["hello", "world"]),
        ("k2", &["hello"]),
        ("k3", &["world", "foo", "world"]),
        ("k4", &["helloworld"]),
        ("k0", &[]),
    ]);
    assert_eq!(built.num_docs, 5);
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

    assert_eq!(segment.alive_keys(), keys(&["k0", "k1", "k2", "k3", "k4"]));
}

#[test]
fn test_membership_only_schema() {
    let schema = full_text_schema();
    let field = schema.schema.get_field_entry(schema.tokens);
    assert!(field.field_type().is_bytes());
    assert_eq!(
        field.field_type().get_index_record_option(),
        Some(IndexRecordOption::Basic)
    );
    assert!(!field.has_fieldnorms());
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
        add_doc(
            &mut builder,
            &i.to_be_bytes(),
            &[&format!("token{i}"), "common"],
        );
    }
    assert!(!builder.is_empty());
    assert_eq!(builder.num_docs(), 10_000);
    assert!(builder.estimated_size() > initial);
}

#[test]
fn test_builder_delete() {
    let mut builder = SegmentBuilder::new().unwrap();
    add_doc(&mut builder, b"a", &["x"]);
    let b = add_doc(&mut builder, b"b", &["x"]);
    add_doc(&mut builder, b"c", &["x", "y"]);
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
    let without_dv =
        OpenedSegment::open(&built.segment_meta, built.bundle.to_bytes(), None).unwrap();
    assert_eq!(without_dv.num_deleted(), 0);
    assert_eq!(search(&without_dv, &["x"]), keys(&["a", "b", "c"]));
}

#[test]
fn test_builder_delete_all() {
    let mut builder = SegmentBuilder::new().unwrap();
    for key in ["a", "b"] {
        let doc_id = add_doc(&mut builder, key.as_bytes(), &["x"]);
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
        ("c", &["x"]),
        ("a", &["x"]),
        ("b", &["x"]),
        ("a", &["x", "y"]),
    ]);
    let segment = open(&built);

    // Unknown keys delete nothing, including a key between two entries.
    assert!(
        segment
            .resolve_deletes(&keys(&["aa", "zzz"]))
            .unwrap()
            .is_none()
    );
    assert!(segment.resolve_deletes(&[]).unwrap().is_none());

    // Both slots for the same PK are found, even when inserted out of order.
    let (dv, num_deleted) = segment.resolve_deletes(&keys(&["a"])).unwrap().unwrap();
    assert_eq!(num_deleted, 2);
    let segment =
        OpenedSegment::open(&built.segment_meta, built.bundle.to_bytes(), Some(dv)).unwrap();
    assert_eq!(segment.num_deleted(), 2);
    assert_eq!(search(&segment, &["x"]), keys(&["b", "c"]));
    assert_eq!(search(&segment, &["y"]), keys(&[]));
    assert_eq!(segment.alive_keys(), keys(&["b", "c"]));

    // Already deleted docs are not deleted again.
    assert!(segment.resolve_deletes(&keys(&["a"])).unwrap().is_none());

    // Deletions accumulate, and repeated keys count once.
    let (dv, num_deleted) = segment
        .resolve_deletes(&keys(&["a", "b", "b"]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 3);
    let segment =
        OpenedSegment::open(&built.segment_meta, built.bundle.to_bytes(), Some(dv)).unwrap();
    assert_eq!(segment.num_deleted(), 3);
    assert_eq!(search(&segment, &["x"]), keys(&["c"]));
    assert_eq!(search(&segment, &["y"]), keys(&[]));
}

#[test]
fn test_resolve_deletes_on_builder_dv() {
    let mut builder = SegmentBuilder::new().unwrap();
    add_doc(&mut builder, b"b", &["x"]);
    let a = add_doc(&mut builder, b"a", &["x"]);
    builder.delete(a);
    let built = builder.finish().unwrap().unwrap();
    let segment = open(&built);
    assert!(segment.resolve_deletes(&keys(&["a"])).unwrap().is_none());
    let (_, num_deleted) = segment
        .resolve_deletes(&keys(&["a", "b"]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 2);
}

#[test]
fn test_merge() {
    let first = build(&[("a", &["x", "y"]), ("b", &["x"]), ("c", &["y"])]);
    let second = build(&[("b", &["x", "z"]), ("d", &["z"]), ("e", &["x"])]);
    let first_segment = open(&first);
    let (dv, _) = first_segment
        .resolve_deletes(&keys(&["a", "b"]))
        .unwrap()
        .unwrap();
    let first_segment =
        OpenedSegment::open(&first.segment_meta, first.bundle.to_bytes(), Some(dv)).unwrap();
    let second_segment = open(&second);
    let (dv, _) = second_segment
        .resolve_deletes(&keys(&["e"]))
        .unwrap()
        .unwrap();
    let second_segment =
        OpenedSegment::open(&second.segment_meta, second.bundle.to_bytes(), Some(dv)).unwrap();

    // Callers may pass references too.
    let merged = merge_segments(&[&first_segment, &second_segment])
        .unwrap()
        .unwrap();
    assert_eq!(merged.num_docs, 3);
    assert!(merged.deletion_vector.is_none());
    assert_eq!(merged.min_key, Bytes::from("b"));
    assert_eq!(merged.max_key, Bytes::from("d"));

    let merged_segment = open(&merged);
    assert_eq!(merged_segment.num_docs(), 3);
    assert_eq!(merged_segment.num_deleted(), 0);
    assert_eq!(merged_segment.alive_keys(), keys(&["b", "c", "d"]));
    assert_eq!(search(&merged_segment, &["x"]), keys(&["b"]));
    assert_eq!(search(&merged_segment, &["y"]), keys(&["c"]));
    assert_eq!(search(&merged_segment, &["z"]), keys(&["b", "d"]));
    assert_eq!(search(&merged_segment, &["x", "z"]), keys(&["b"]));
    assert_eq!(search(&merged_segment, &["x", "y"]), keys(&[]));

    // Delete lookup uses the new doc IDs directly.
    let (dv, num_deleted) = merged_segment
        .resolve_deletes(&keys(&["b"]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 1);
    let merged_segment =
        OpenedSegment::open(&merged.segment_meta, merged.bundle.to_bytes(), Some(dv)).unwrap();
    assert_eq!(search(&merged_segment, &["z"]), keys(&["d"]));

    // Merging again drops the newly deleted doc.
    let merged_again = merge_segments(&[merged_segment]).unwrap().unwrap();
    assert_eq!(merged_again.num_docs, 2);
    assert_eq!(open(&merged_again).alive_keys(), keys(&["c", "d"]));
}

#[test]
fn test_merge_all_deleted() {
    let built = build(&[("a", &["x"]), ("b", &["y"])]);
    let segment = open(&built);
    let (dv, num_deleted) = segment
        .resolve_deletes(&keys(&["a", "b"]))
        .unwrap()
        .unwrap();
    assert_eq!(num_deleted, 2);
    let segment =
        OpenedSegment::open(&built.segment_meta, built.bundle.to_bytes(), Some(dv)).unwrap();
    assert!(merge_segments(&[segment]).unwrap().is_none());
    assert!(merge_segments::<OpenedSegment>(&[]).unwrap().is_none());
}

#[test]
fn test_merge_disjoint_segments_in_pk_order() {
    let later = open(&build(&[("m", &["later"]), ("z", &["later"])]));
    let earlier = build(&[("a", &["earlier"]), ("b", &["earlier"])]);
    let (dv, _) = open(&earlier)
        .resolve_deletes(&keys(&["a"]))
        .unwrap()
        .unwrap();
    let earlier =
        OpenedSegment::open(&earlier.segment_meta, earlier.bundle.to_bytes(), Some(dv)).unwrap();
    let merged = merge_segments(&[later, earlier]).unwrap().unwrap();
    assert_pk_layout(&merged);
    let merged = open(&merged);
    assert_eq!(merged.alive_keys(), keys(&["b", "m", "z"]));
    assert_eq!(search(&merged, &["earlier"]), keys(&["b"]));
    assert_eq!(search(&merged, &["later"]), keys(&["m", "z"]));
}

#[test]
fn test_merge_sorted_pages_and_postings() {
    const NUM_KEYS: u32 = 50_000;
    const LIVE_SOURCES: u32 = 15;
    const DUPLICATE: u32 = 129;
    let key = |i: u32| Bytes::copy_from_slice(&i.to_be_bytes());
    // Exercise the maintenance fan-in with interleaved inputs and one all-deleted input.
    let inputs: Vec<_> = (0..=LIVE_SOURCES)
        .map(|source| {
            let mut builder = SegmentBuilder::new().unwrap();
            let token = format!("source{source}");
            for i in (0..NUM_KEYS).rev() {
                if i % LIVE_SOURCES == source
                    || (source == 0 && i == DUPLICATE)
                    || source == LIVE_SOURCES
                {
                    let doc = add_doc(&mut builder, &key(i), &["all", &token]);
                    if source == LIVE_SOURCES || i == 0 || i == NUM_KEYS - 1 {
                        builder.delete(doc);
                    }
                }
            }
            builder.finish().unwrap().unwrap()
        })
        .collect();
    for built in &inputs {
        assert_pk_layout(built);
    }
    let segments: Vec<_> = inputs.iter().map(open).collect();
    let merged = merge_segments(&segments).unwrap().unwrap();
    assert_pk_layout(&merged);
    assert_eq!(merged.min_key, key(1));
    assert_eq!(merged.max_key, key(NUM_KEYS - 2));
    let merged_segment = open(&merged);
    assert_eq!(
        merged_segment.alive_keys(),
        (1..NUM_KEYS - 1)
            .flat_map(|i| [Some(key(i)), (i == DUPLICATE).then(|| key(i))])
            .flatten()
            .collect::<Vec<_>>()
    );
    for source in 0..LIVE_SOURCES {
        let token = format!("source{source}");
        let expected: Vec<_> = (1..NUM_KEYS - 1)
            .filter(|&i| i % LIVE_SOURCES == source || (source == 0 && i == DUPLICATE))
            .map(key)
            .collect();
        assert_eq!(search(&merged_segment, &[&token, "all"]), expected);
        assert_eq!(search(&segments[source as usize], &[&token]), expected);
    }
    assert!(search(&merged_segment, &[&format!("source{LIVE_SOURCES}")]).is_empty());

    let (dv, deleted) = merged_segment
        .resolve_deletes(&[key(DUPLICATE)])
        .unwrap()
        .unwrap();
    assert_eq!(deleted, 2);
    let deleted =
        OpenedSegment::open(&merged.segment_meta, merged.bundle.to_bytes(), Some(dv)).unwrap();
    assert!(!search(&deleted, &["all"]).contains(&key(DUPLICATE)));
}

fn assert_pk_layout(built: &BuiltSegment) {
    use tantivy::columnar::ColumnarReader;
    use tantivy::index::SegmentComponent;

    let files = super::bundle::decode_bundle(built.bundle.to_bytes()).unwrap();
    let meta = super::bundle::decode_segment_meta(&built.segment_meta).unwrap();
    let prefix = format!("{}.", meta.segment_id.uuid_string());
    assert_eq!(
        files
            .keys()
            .filter(|name| !name.starts_with(&prefix))
            .collect::<Vec<_>>(),
        ["pk_pages"]
    );
    let index = tantivy::Index::create(
        super::bundle::BundleDirectory::new(files),
        full_text_schema().schema.clone(),
        index_settings(),
    )
    .unwrap();
    let segment = index.segment(index.new_segment_meta(meta.segment_id, meta.max_doc));
    let fast = segment.open_read(SegmentComponent::FastFields).unwrap();
    assert_eq!(ColumnarReader::open(fast).unwrap().num_columns(), 0);
}

#[test]
fn test_delete_file() {
    let deleted = keys(&["", "a", "ab", "b"]);
    let encoded = super::delete_file::encode_delete_file(42, &deleted)
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(decode_delete_file(&encoded).unwrap(), (42, deleted));

    let empty = super::delete_file::encode_delete_file(7, &[])
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(decode_delete_file(&empty).unwrap(), (7, vec![]));

    for i in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[i] ^= 0x10;
        assert!(decode_delete_file(&corrupted).is_err(), "byte {i}");
    }
    assert!(decode_delete_file(&encoded[..encoded.len() - 1]).is_err());
    assert!(decode_delete_file(&[]).is_err());
}

#[test]
fn test_version_one_object_layout() {
    use xxhash_rust::xxh64::xxh64;

    let mut expected = b"DFWR\x01\0\0\0\x2a\0\0\0\0\0\0\0\x01\0\0\0\x01\0\0\0k".to_vec();
    expected.extend_from_slice(&xxh64(&expected, 0).to_le_bytes());
    let actual: Vec<_> = super::delete_file::encode_delete_file(42, &keys(&["k"]))
        .flatten()
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(decode_delete_file(&expected).unwrap(), (42, keys(&["k"])));

    let mut expected = b"abc\x01\0\0\0\x01\0f\0\0\0\0\0\0\0\0\x03\0\0\0\0\0\0\0\
        \x03\0\0\0\0\0\0\0\x01\0\0\0BFWR"
        .to_vec();
    expected.extend_from_slice(&xxh64(&expected, 0).to_le_bytes());
    let bundle = super::bundle::SegmentBundle::new(vec![(
        "f".to_owned(),
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")],
    )]);
    assert_eq!(bundle.to_bytes().as_ref(), &expected);
    let files = super::bundle::decode_bundle(Bytes::from(expected)).unwrap();
    assert_eq!(files["f"].read_bytes().unwrap().as_slice(), b"abc");
}

#[test]
fn test_corruption_detected() {
    let mut builder = SegmentBuilder::new().unwrap();
    add_doc(&mut builder, b"a", &["x"]);
    let b = add_doc(&mut builder, b"b", &["x"]);
    builder.delete(b);
    let built = builder.finish().unwrap().unwrap();
    let (dv, _) = built.deletion_vector.clone().unwrap();
    let open_with = |segment_meta: &[u8], bundle: Bytes, dv: Option<Bytes>| {
        OpenedSegment::open(segment_meta, bundle, dv)
    };

    // Any flipped byte in the bundle is detected.
    let bundle = built.bundle.to_bytes();
    let len = bundle.len();
    for i in [0, len / 3, len / 2, len - 30, len - 20, len - 12, len - 1] {
        let mut corrupted = bundle.to_vec();
        corrupted[i] ^= 0x01;
        assert!(
            open_with(&built.segment_meta, Bytes::from(corrupted), None).is_err(),
            "byte {i}"
        );
    }
    // Truncated bundles.
    assert!(open_with(&built.segment_meta, bundle.slice(..len - 1), None).is_err());
    assert!(open_with(&built.segment_meta, bundle.slice(..10), None).is_err());
    assert!(open_with(&built.segment_meta, Bytes::new(), None).is_err());

    // Bad segment meta.
    assert!(open_with(&built.segment_meta[..10], built.bundle.to_bytes(), None).is_err());
    let mut bad_meta = built.segment_meta.to_vec();
    bad_meta[0] ^= 0x01;
    assert!(open_with(&bad_meta, built.bundle.to_bytes(), None).is_err());
    // A segment meta pointing at another segment.
    let other = build(&[("z", &["x"])]);
    assert!(open_with(&other.segment_meta, built.bundle.to_bytes(), None).is_err());

    // Bad deletion vectors.
    let mut bad_dv = dv.to_vec();
    bad_dv[4] ^= 0x01;
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.to_bytes(),
            Some(Bytes::from(bad_dv))
        )
        .is_err()
    );
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.to_bytes(),
            Some(dv.slice(..3))
        )
        .is_err()
    );
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.to_bytes(),
            Some(Bytes::new())
        )
        .is_err()
    );
    // A deletion vector of a segment with a different number of docs.
    let mut bigger = SegmentBuilder::new().unwrap();
    for i in 0..100u32 {
        let doc_id = add_doc(&mut bigger, &i.to_be_bytes(), &["x"]);
        bigger.delete(doc_id);
    }
    let (bigger_dv, _) = bigger.finish().unwrap().unwrap().deletion_vector.unwrap();
    assert!(
        open_with(
            &built.segment_meta,
            built.bundle.to_bytes(),
            Some(bigger_dv)
        )
        .is_err()
    );

    // The intact objects still open.
    let segment = open_with(&built.segment_meta, built.bundle.to_bytes(), Some(dv)).unwrap();
    assert_eq!(search(&segment, &["x"]), keys(&["a"]));
}

#[test]
fn test_large_segment() {
    const NUM_DOCS: u32 = 50_000;
    let key = |i: u32| Bytes::copy_from_slice(&i.to_be_bytes());
    let mut builder = SegmentBuilder::new().unwrap();
    for i in 0..NUM_DOCS {
        add_doc(
            &mut builder,
            &key(i),
            &["all", &format!("m{}", i % 10), &format!("u{i}")],
        );
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

    // Delete every 3rd doc of the first half.
    let delete_markers: Vec<_> = (0..NUM_DOCS / 2)
        .filter(|i| i.is_multiple_of(3))
        .map(key)
        .collect();
    let (dv, num_deleted) = segment.resolve_deletes(&delete_markers).unwrap().unwrap();
    let is_alive = |i: u32| !i.is_multiple_of(7) && !(i < NUM_DOCS / 2 && i.is_multiple_of(3));
    let num_alive = (0..NUM_DOCS).filter(|&i| is_alive(i)).count() as u64;
    assert_eq!(num_deleted, NUM_DOCS as u64 - num_alive);
    let segment =
        OpenedSegment::open(&built.segment_meta, built.bundle.to_bytes(), Some(dv)).unwrap();
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
