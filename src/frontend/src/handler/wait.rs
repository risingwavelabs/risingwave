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

use pgwire::pg_response::{PgResponse, StatementType};

use super::RwPgResponse;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub(super) async fn handle_wait(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    do_wait(&handler_args.session).await?;
    Ok(PgResponse::empty_result(StatementType::WAIT))
}

pub(crate) async fn do_wait(session: &SessionImpl) -> Result<()> {
    let client = session.env().meta_client();
    client.wait().await?;
    Ok(())
}
