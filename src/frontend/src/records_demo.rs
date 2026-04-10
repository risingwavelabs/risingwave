pub(crate) const DEMO_SCHEMA_NAME: &str = "rw_records_demo";
pub(crate) const DEMO_TABLE_NAME: &str = "records";

pub(crate) fn is_records_demo_table(schema_name: &str, table_name: &str) -> bool {
    schema_name == DEMO_SCHEMA_NAME && table_name == DEMO_TABLE_NAME
}
