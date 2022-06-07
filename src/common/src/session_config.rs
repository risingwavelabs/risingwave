/// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
/// until the entire dataflow is refreshed. In other words, every related table & MV will
/// be able to see the write.
pub const IMPLICIT_FLUSH: &str = "RW_IMPLICIT_FLUSH";

/// A temporary config variable to force query running in either local or distributed mode.
/// It will be removed in the future.
pub const QUERY_MODE: &str = "QUERY_MODE";

/// To force the usage of delta join in streaming execution.
pub const DELTA_JOIN: &str = "RW_FORCE_DELTA_JOIN";
