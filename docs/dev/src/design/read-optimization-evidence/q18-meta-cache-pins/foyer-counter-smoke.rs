use foyer_memory::{Cache, CacheBuilder, LruConfig};

fn main() {
    let cache: Cache<u64, u64> = CacheBuilder::new(10)
        .with_shards(1)
        .with_eviction_config(LruConfig::default())
        .with_weighter(|_: &u64, _: &u64| 6)
        .build();
    drop(cache.insert(1, 42));
    let handle = cache.get(&1).unwrap();
    let clone = handle.clone();
    let Cache::Lru(lru) = &cache else {
        panic!("expected LRU")
    };
    let snapshot = lru.lru_shard_snapshot();
    assert_eq!(snapshot[0].pinned_entries, 1);
    assert_eq!(snapshot[0].pinned_weight, 6);
    assert_eq!(snapshot[0].usage, 6);
    drop(cache.insert(2, 43));
    let snapshot = lru.lru_shard_snapshot();
    assert_eq!(snapshot[0].usage, 12);
    assert_eq!(snapshot[0].capacity, 10);
    assert_eq!(snapshot[0].over_capacity_insertions, 1);
    assert_eq!(snapshot[0].peak_excess_weight, 2);
    drop(handle);
    assert_eq!(lru.lru_shard_snapshot()[0].pinned_entries, 1);
    drop(clone);
    assert_eq!(lru.lru_shard_snapshot()[0].pinned_weight, 0);
    println!("Clone deduplication, final release, and capacity overshoot verified");
}
