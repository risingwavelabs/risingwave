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

use pgwire::pg_response::StatementType;
use risingwave_common::acl::AclMode;
use risingwave_pb::user::grant_privilege::Object as GrantObject;
use risingwave_sqlparser::ast::ObjectName;

use crate::Binder;
use crate::error::Result;
use crate::handler::privilege::ObjectCheckItem;
use crate::handler::{HandlerArgs, RwPgResponse};

pub fn handle_use_db(handler_args: HandlerArgs, database_name: ObjectName) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let database_name = Binder::resolve_database_name(database_name)?;

    let (database_id, owner_id) = {
        let catalog_reader = session.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        let db = reader.get_database_by_name(&database_name)?;
        (db.id(), db.owner)
    };
    session.check_privileges(&[ObjectCheckItem::new(
        owner_id,
        AclMode::Connect,
        GrantObject::DatabaseId(database_id),
    )])?;

    let mut builder = RwPgResponse::builder(StatementType::USE);
    builder = builder.notice(format!(
        "You are now connected to database \"{}\" as user \"{}\".",
        database_name,
        session.user_name()
    ));

    // reset search_path
    session.reset_config("search_path")?;

    session.update_database(database_name);

    Ok(builder.into())
}
