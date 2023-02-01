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

use super::RwPgResponse;
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub(super) async fn handle_flush(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    do_flush(&handler_args.session).await?;
    Ok(PgResponse::empty_result(StatementType::FLUSH))
}

pub(crate) async fn do_flush(session: &SessionImpl) -> Result<()> {
    let client = session.env().meta_client();
    let snapshot = client.flush(true).await?;
    session
        .env()
        .hummock_snapshot_manager()
        .update_epoch(snapshot);
    Ok(())
}
