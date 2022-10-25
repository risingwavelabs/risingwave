#[macro_export]
macro_rules! trace {
    (GET, $key:ident, $bloom_filter:ident, $opt:ident) => {
        $crate::collector::new_span($crate::record::Operation::Get(
            $key.to_vec(),
            $bloom_filter,
            $opt.epoch,
            $opt.table_id.table_id,
            $opt.retention_seconds,
        ))
    };
    (INGEST, $kvs:ident, $opt:ident) => {
        $crate::collector::new_span($crate::record::Operation::Ingest(
            $kvs.iter()
                .map(|(k, v)| (k.to_vec(), v.user_value.clone().map(|v| v.to_vec())))
                .collect(),
            $opt.epoch,
            $opt.table_id.table_id,
        ))
    };
    (ITER, $prefix:ident, $range:ident, $opt:ident) => {
        $crate::collector::new_span($crate::record::Operation::Iter(
            $prefix.clone(),
            $range.start_bound().map(|b| b.as_ref().to_vec()),
            $range.end_bound().map(|b| b.as_ref().to_vec()),
            $opt.epoch,
            $opt.table_id.table_id,
            $opt.retention_seconds,
        ))
    };
    (ITER_NEXT, $id:expr, $pair:ident) => {
        $crate::collector::new_span($crate::record::Operation::IterNext(
            $id,
            $pair.clone().map(|(k, v)| (k.to_vec(), v.to_vec())),
        ))
    };
    (SYNC, $epoch:ident) => {
        $crate::collector::new_span($crate::record::Operation::Sync($epoch))
    };
    (SEAL, $epoch:ident, $check_point:ident) => {
        $crate::collector::new_span($crate::record::Operation::Seal($epoch, $check_point))
    };
    (VERSION) => {
        $crate::collector::new_span($crate::record::Operation::UpdateVersion())
    };
}
