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

use tokio_postgres::Client;

pub struct Checker<'a> {
    pub client: &'a Client,
}

impl<'a> Checker<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn is_failure_preserved(&self, old: &str, new: &str) -> bool {
        let old_result = run_query(self.client, old).await;
        let new_result = run_query(self.client, new).await;

        old_result.0 == new_result.0 && old_result.1 == new_result.1
    }
}

pub async fn run_query(client: &Client, query: &str) -> (bool, String) {
    let query_task = client.simple_query(query);
    match query_task.await {
        Ok(_) => (true, String::new()),
        Err(e) => (false, e.to_string()),
    }
}
