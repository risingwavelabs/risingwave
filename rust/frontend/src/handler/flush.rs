// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pgwire::pg_response::PgResponse;
use risingwave_common::error::Result;

use crate::session::QueryContext;

pub(super) async fn handle_flush(context: QueryContext) -> Result<PgResponse> {
    let client = context.session_ctx.env().meta_client();
    client.flush().await?;

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::FLUSH,
        0,
        vec![],
        vec![],
    ))
}
