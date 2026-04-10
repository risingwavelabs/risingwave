pub(crate) const DEMO_SCHEMA_NAME: &str = "rw_records_demo";
pub(crate) const DEMO_TABLE_NAME: &str = "records";
pub(crate) const DEMO_CURSOR_MV_NAME: &str = "records_cursor";
pub(crate) const DEMO_SUBSCRIPTION_NAME: &str = "records_sub";

pub(crate) fn is_records_demo_table(schema_name: &str, table_name: &str) -> bool {
    schema_name == DEMO_SCHEMA_NAME && table_name == DEMO_TABLE_NAME
}
