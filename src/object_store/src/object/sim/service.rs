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

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::Stream;
use itertools::Itertools;
use spin::Mutex;

use crate::object::error::ObjectResult as Result;
use crate::object::sim::SimError;
use crate::object::{ObjectError, ObjectMetadata};

#[derive(Debug)]
pub(crate) enum Request {
    Upload { path: String, obj: Bytes },
    Read { path: String },
    Delete { path: String },
    DeleteObjects { paths: Vec<String> },
    List { path: String },
    Metadata { path: String },
}

#[derive(Debug)]
pub(crate) enum Response {
    Upload,
    Read(Bytes),
    Delete,
    DeleteObjects,
    List(SimObjectIter),
    Metadata(ObjectMetadata),
}

#[derive(Debug)]
pub(crate) struct SimService {
    storage: Mutex<BTreeMap<String, (ObjectMetadata, Bytes)>>,
}

fn get_obj_meta(path: &str, obj: &Bytes) -> Result<ObjectMetadata> {
    Ok(ObjectMetadata {
        key: path.to_string(),
        last_modified: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(ObjectError::internal)?
            .as_secs_f64(),
        total_size: obj.len(),
    })
}

impl SimService {
    pub fn new() -> Self {
        SimService {
            storage: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn upload(&self, path: String, obj: Bytes) -> Result<Response> {
        let metadata = get_obj_meta(&path, &obj)?;
        self.storage.lock().insert(path.into(), (metadata, obj));
        Ok(Response::Upload)
    }

    pub async fn read(&self, path: String) -> Result<Response> {
        let storage = self.storage.lock();
        let obj = storage
            .get(&path)
            .map(|(_, o)| o)
            .ok_or_else(|| SimError::NotFound(format!("no object at path '{}'", path)))?;
        Ok(Response::Read(obj.clone()))
    }

    pub async fn delete(&self, path: String) -> Result<Response> {
        self.storage.lock().remove(&path);
        Ok(Response::Delete)
    }

    pub async fn delete_objects(&self, paths: Vec<String>) -> Result<Response> {
        for path in paths {
            self.storage.lock().remove(&path);
        }
        Ok(Response::DeleteObjects)
    }

    pub async fn list(&self, prefix: String) -> Result<Response> {
        if prefix.is_empty() {
            return Ok(Response::List(SimObjectIter::new(vec![])));
        }

        let mut scan_end = prefix.chars().collect_vec();
        let next = *scan_end.last().unwrap() as u8 + 1;
        *scan_end.last_mut().unwrap() = next as char;
        let scan_end = scan_end.into_iter().collect::<String>();

        let list_result = self
            .storage
            .lock()
            .range(prefix..scan_end)
            .map(|(_, (o, _))| o.clone())
            .collect_vec();
        Ok(Response::List(SimObjectIter::new(list_result)))
    }

    pub async fn metadata(&self, path: String) -> Result<Response> {
        self.storage
            .lock()
            .get(&path)
            .map(|(metadata, _)| metadata)
            .cloned()
            .ok_or_else(|| SimError::not_found(format!("no object at path '{}'", path)).into())
            .map(|meta| Response::Metadata(meta))
    }
}

#[derive(Debug)]
pub(crate) struct SimObjectIter {
    list_result: VecDeque<ObjectMetadata>,
}

impl SimObjectIter {
    fn new(list_result: Vec<ObjectMetadata>) -> Self {
        Self {
            list_result: list_result.into(),
        }
    }
}

impl Stream for SimObjectIter {
    type Item = Result<ObjectMetadata>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(i) = self.list_result.pop_front() {
            return Poll::Ready(Some(Ok(i)));
        }
        Poll::Ready(None)
    }
}
