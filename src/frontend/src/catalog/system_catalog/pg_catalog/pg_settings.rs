// Copyright 2025 RisingWave Labs
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

use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::{DataType, Datum, Fields, ToOwnedDatum, WithDataType};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;

/// The catalog `pg_settings` stores settings.
/// Ref: `https://www.postgresql.org/docs/current/view-pg-settings.html`
#[derive(Fields)]
#[primary_key(name, context)]
struct PgSetting {
    name: String,
    setting: String,
    short_desc: String,
    context: Context,
}

/// Context required to set the parameter's value.
///
/// Note that we do not strictly follow the PostgreSQL's semantics for each variant
/// but only pick the minimum set of variants required for features like tab-completion.
#[derive(Clone, Copy)]
enum Context {
    /// Used for immutable system parameters.
    Internal,

    /// Used for mutable system parameters.
    // TODO: `postmaster` means that changes require a restart of the server. This is
    // not accurate for all system parameters. Use lower contexts once we guarantee about
    // the behavior of each parameter.
    Postmaster,

    /// Used for session variables.
    // TODO: There might be variables that can only be set by superusers in the future.
    // Should use `superuser` context then.
    User,
}

impl WithDataType for Context {
    fn default_data_type() -> DataType {
        DataType::Varchar
    }
}

impl ToOwnedDatum for Context {
    fn to_owned_datum(self) -> Datum {
        match self {
            Context::Internal => "internal",
            Context::Postmaster => "postmaster",
            Context::User => "user",
        }
        .to_owned_datum()
    }
}

#[system_catalog(table, "pg_catalog.pg_settings")]
fn read_pg_settings(reader: &SysCatalogReaderImpl) -> Vec<PgSetting> {
    let variables = (reader.config.read().show_all())
        .into_iter()
        .map(|info| PgSetting {
            name: info.name,
            setting: info.setting,
            short_desc: info.description,
            context: Context::User,
        });

    let system_params = (reader.system_params.load().get_all())
        .into_iter()
        .map(|info| PgSetting {
            name: info.name.to_owned(),
            setting: info.value,
            short_desc: info.description.to_owned(),
            context: if info.mutable {
                Context::Postmaster
            } else {
                Context::Internal
            },
        });

    variables.chain(system_params).collect()
}
