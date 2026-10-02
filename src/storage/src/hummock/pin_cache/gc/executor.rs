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

//! Physical deletion only. Selection, file ownership and capacity belong to the caller.

use risingwave_object_store::object::{ObjectError, ObjectStoreRef};

// Match Hummock GC's maximum number of paths per delete request.
pub(super) const DELETE_BATCH_SIZE: usize = 1000;

pub(super) struct DeleteResult {
    // A successful prefix. The failing batch may have partially deleted files, but is not acknowledged.
    pub deleted: usize,
    pub error: Option<ObjectError>,
}

pub(super) async fn delete_files(
    store: &ObjectStoreRef,
    mut paths: impl Iterator<Item = String>,
) -> DeleteResult {
    let mut deleted = 0;
    let mut batch = Vec::with_capacity(DELETE_BATCH_SIZE);
    loop {
        batch.extend(paths.by_ref().take(DELETE_BATCH_SIZE));
        if batch.is_empty() {
            break;
        }
        if let Err(error) = store.delete_objects(&batch).await {
            return DeleteResult {
                deleted,
                error: Some(error),
            };
        }
        deleted += batch.len();
        batch.clear();
        tokio::task::yield_now().await;
    }
    DeleteResult {
        deleted,
        error: None,
    }
}
