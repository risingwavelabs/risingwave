pub const DEFAULT_COLUMN_FAMILY_ID: &str = "global";

/// Mimic the cf interface
pub struct ColumnFamilyUtils {}
impl ColumnFamilyUtils {
    /// Return a composed cf which is hummock manager cf + `cf_ident`(user cf)
    pub fn get_composed_cf(manager_cf: &str, cf_ident: &str) -> String {
        String::from(manager_cf) + " " + cf_ident
    }
}