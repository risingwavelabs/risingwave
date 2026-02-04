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

use super::RwPgResponse;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub(super) async fn handle_recover(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    // Only permit recovery for super users.
    if !handler_args.session.is_super_user() {
        return Err(ErrorCode::PermissionDenied(
            "only superusers can trigger adhoc recovery".to_owned(),
        )
        .into());
    }
    do_recover(&handler_args.session).await?;
    Ok(PgResponse::empty_result(StatementType::RECOVER))
}

pub(crate) async fn do_recover(session: &SessionImpl) -> Result<()> {
    let client = session.env().meta_client();
    client.recover().await?;
    Ok(())
}
