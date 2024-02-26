use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The view `pg_sequences` provides access to useful information about each sequence in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-sequences.html`]
#[system_catalog(view, "pg_catalog.pg_sequences")]
#[derive(Fields)]
struct PgSequences {
    schemaname: String,
    sequencename: String,
    sequenceowner: String,
    increment_by: i64,
    last_value: i64,
    cycle: bool,
    start_value: i64,
    max_value: i64,
    min_value: i64,
}
