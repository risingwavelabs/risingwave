// Copyright 2024 RisingWave Labs
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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::{bail, bail_not_implemented};
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_sqlparser::ast::{ObjectName, Value, WithProperties};

use crate::handler::alter_table_column::fetch_table_catalog_for_alter;
use crate::handler::{HandlerArgs, RwPgResponse};

const REQUIRE_CLEAN_STATE: &str = "clean_state";

pub fn handle_alter_table_connector_attr(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    mut new_attr: WithProperties,
) -> crate::error::Result<RwPgResponse> {
    let session = handler_args.session;
    let original_table = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    if original_table.associated_source_id.is_none() {
        bail!("Cannot alter a table without connector")
    }
    let clean_state_flag = check_attr(&mut new_attr)?;

    if clean_state_flag {
        bail_not_implemented!(
            "alter connector params requiring clean its state is not implemented yet, {} should be set to false", REQUIRE_CLEAN_STATE
        );
    }

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

fn check_attr(attr: &mut WithProperties) -> crate::error::Result<bool> {
    // check for REQUIRE_CLEAN_STATE, should be contained in attr
    let mut contain_clean_state_flag = false;
    let mut clean_state_flag = false;

    // cannot change connector
    if attr.0.iter().any(|item| {
        item.name
            .real_value()
            .eq_ignore_ascii_case(UPSTREAM_SOURCE_KEY)
    }) {
        bail!("cannot alter attribute {}", UPSTREAM_SOURCE_KEY);
    }

    for idx in 0..attr.0.len() {
        if attr.0[idx]
            .name
            .real_value()
            .eq_ignore_ascii_case(REQUIRE_CLEAN_STATE)
        {
            contain_clean_state_flag = true;
            if let Value::Boolean(b) = &attr.0[idx].value {
                clean_state_flag = *b
            } else {
                bail!("{} should be a boolean", REQUIRE_CLEAN_STATE);
            }

            attr.0.remove(idx);
            break;
        }
    }
    if !contain_clean_state_flag {
        bail!(
            "{} should be contained in the WITH clause",
            REQUIRE_CLEAN_STATE
        )
    }

    Ok(clean_state_flag)
}
