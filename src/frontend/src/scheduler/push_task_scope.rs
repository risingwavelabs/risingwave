// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt};
use risingwave_batch::executor::PushTaskScope;
use risingwave_common::session_config::SearchPath;
use risingwave_expr::expr_context::{STRICT_MODE, TIME_ZONE};

use crate::catalog::catalog_service::CatalogReader;
use crate::expr::function_impl::context::{
    AUTH_CONTEXT, CATALOG_READER, DB_NAME, META_CLIENT, SEARCH_PATH, USER_INFO_READER,
};
use crate::meta_client::FrontendMetaClient;
use crate::session::AuthContext;
use crate::user::user_service::UserInfoReader;

/// Captures frontend task-local contexts for spawned push pipeline adapters.
pub struct FrontendPushTaskScope {
    catalog_reader: CatalogReader,
    user_info_reader: UserInfoReader,
    auth_context: Arc<AuthContext>,
    db_name: String,
    search_path: SearchPath,
    time_zone: String,
    strict_mode: bool,
    meta_client: Arc<dyn FrontendMetaClient>,
}

impl FrontendPushTaskScope {
    pub fn new(
        catalog_reader: CatalogReader,
        user_info_reader: UserInfoReader,
        auth_context: Arc<AuthContext>,
        db_name: String,
        search_path: SearchPath,
        time_zone: String,
        strict_mode: bool,
        meta_client: Arc<dyn FrontendMetaClient>,
    ) -> Self {
        Self {
            catalog_reader,
            user_info_reader,
            auth_context,
            db_name,
            search_path,
            time_zone,
            strict_mode,
            meta_client,
        }
    }
}

impl PushTaskScope for FrontendPushTaskScope {
    fn scope(&self, future: BoxFuture<'static, ()>) -> BoxFuture<'static, ()> {
        let catalog_reader = self.catalog_reader.clone();
        let user_info_reader = self.user_info_reader.clone();
        let auth_context = self.auth_context.clone();
        let db_name = self.db_name.clone();
        let search_path = self.search_path.clone();
        let time_zone = self.time_zone.clone();
        let strict_mode = self.strict_mode;
        let meta_client = self.meta_client.clone();

        async move {
            let exec = async move { CATALOG_READER::scope(catalog_reader, future).await }.boxed();
            let exec = async move { USER_INFO_READER::scope(user_info_reader, exec).await }.boxed();
            let exec = async move { DB_NAME::scope(db_name, exec).await }.boxed();
            let exec = async move { SEARCH_PATH::scope(search_path, exec).await }.boxed();
            let exec = async move { AUTH_CONTEXT::scope(auth_context, exec).await }.boxed();
            let exec = async move { TIME_ZONE::scope(time_zone, exec).await }.boxed();
            let exec = async move { STRICT_MODE::scope(strict_mode, exec).await }.boxed();
            META_CLIENT::scope(meta_client, exec).await;
        }
        .boxed()
    }
}
