/// Generate a globally unique operator id.
pub fn unique_operator_id(fragment_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((fragment_id as u64) << 32) + operator_id
}

/// Generate a globally unique executor id.
pub fn unique_executor_id(actor_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((actor_id as u64) << 32) + operator_id
}
