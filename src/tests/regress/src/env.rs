// Copyright 2023 RisingWave Labs
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

use std::env::{remove_var, set_var, var};

pub(crate) const VAR_PG_OPTS: &str = "PGOPTIONS";

/// Setup some environments for psql.
///
/// This is useful since it may affect psql's behavior.
pub(crate) fn init_env() {
    // Set default application name.
    set_var("PGAPPNAME", "risingwave_regress");

    // Set translation-related settings to English; otherwise psql will
    // produce translated messages and produce diffs.  (XXX If we ever support
    // translation of pg_regress, this needs to be moved elsewhere, where psql
    // is actually called.)
    remove_var("LANGUAGE");
    remove_var("LC_ALL");
    set_var("LC_MESSAGES", "C");

    // Set timezone and datestyle for datetime-related tests
    set_var("PGTZ", "PST8PDT");
    set_var("PGDATESTYLE", "Postgres, MDY");

    // Likewise set intervalstyle to ensure consistent results.  This is a bit
    // more painful because we must use PGOPTIONS, and we want to preserve the
    // user's ability to set other variables through that.
    {
        let mut pg_opts = var(VAR_PG_OPTS).unwrap_or_else(|_| "".to_string());
        pg_opts.push_str(" -c intervalstyle=postgres_verbose");
        set_var(VAR_PG_OPTS, pg_opts);
    }
}
