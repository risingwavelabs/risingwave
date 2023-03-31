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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::handler::HandlerArgs;

pub async fn handle_drop_connection(
    handler_args: HandlerArgs,
    connection_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let connection_name = Binder::resolve_connection_name(connection_name)?;

    {
        let reader = session.env().catalog_reader().read_guard();
        match reader.get_connection_by_name(&connection_name) {
            Ok(_) => (),
            Err(e) => {
                return if if_exists {
                    Ok(RwPgResponse::empty_result_with_notice(
                        StatementType::DROP_CONNECTION,
                        format!(
                            "connection \"{}\" does not exist, skipping",
                            connection_name
                        ),
                    ))
                } else {
                    Err(e.into())
                }
            }
        }
    }

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_connection(&connection_name).await?;

    Ok(PgResponse::empty_result(StatementType::DROP_CONNECTION))
}
