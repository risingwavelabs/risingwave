struct DynamicFilter {
    prev_val: Datum,
}

struct CachedRange {
    min_inclusive: Datum,
    max_inclusive: Datum,
    cache: BTreeMap<Datum>,
}
