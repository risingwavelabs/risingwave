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

use async_nats::jetstream::object_store::ObjectMetadata;
use opendal::{Operator, Lister};

pub struct OpendalSource {
    pub(crate) op: Operator,
    pub(crate) engine_type: EngineType,
}

impl OpendalSource{
    async fn list(&self, prefix: &str) -> ObjectResult<ObjectMetadataIter> {
        let lister = self.op.scan(prefix).await?;
        Ok(Box::pin(OpenDalObjectIter::new(lister, self.op.clone())))
    }
}

#[derive(Clone)]
pub enum EngineType {

    Gcs,
}


struct OpenDalSourceLister {
    lister: Option<Lister>,
    op: Option<Operator>,
    #[allow(clippy::type_complexity)]
    next_future: Option<BoxFuture<'static, (Option<Result<Entry, Error>>, Lister)>>,
    #[allow(clippy::type_complexity)]
    metadata_future: Option<BoxFuture<'static, (Result<ObjectMetadata, Error>, Operator)>>,
}

impl OpenDalSourceLister{
    fn new(lister: Lister, op: Operator) -> Self {
        Self {
            lister: Some(lister),
            op: Some(op),
            next_future: None,
            metadata_future: None,
        }
    }
}

impl Stream for OpenDalObjectIter {
    type Item = ObjectResult<ObjectMetadata>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(metadata_future) = self.metadata_future.as_mut() {
            let (result, op) = ready!(metadata_future.poll_unpin(cx));
            self.op = Some(op);
            return match result {
                Ok(m) => {
                    self.metadata_future = None;
                    Poll::Ready(Some(Ok(m)))
                }
                Err(e) => {
                    self.metadata_future = None;
                    Poll::Ready(Some(Err(e.into())))
                }
            };
        }
        if let Some(next_future) = self.next_future.as_mut() {
            let (option, lister) = ready!(next_future.poll_unpin(cx));
            self.lister = Some(lister);
            return match option {
                None => {
                    self.next_future = None;
                    Poll::Ready(None)
                }
                Some(result) => {
                    self.next_future = None;
                    match result {
                        Ok(object) => {
                            let op = self.op.take().expect("op should not be None");
                            let f = async move {
                                let key = object.path().to_string();
                                // FIXME: How does opendal metadata cache work?
                                // Will below line result in one IO per object?
                                let om = match op
                                    .metadata(
                                        &object,
                                        Metakey::LastModified | Metakey::ContentLength,
                                    )
                                    .await
                                {
                                    Ok(om) => om,
                                    Err(e) => return (Err(e), op),
                                };
                                let last_modified = match om.last_modified() {
                                    Some(t) => t.timestamp() as f64,
                                    None => 0_f64,
                                };
                                let total_size = om.content_length() as usize;
                                let metadata = ObjectMetadata {
                                    key,
                                    last_modified,
                                    total_size,
                                };
                                (Ok(metadata), op)
                            };
                            self.metadata_future = Some(Box::pin(f));
                            self.poll_next(cx)
                        }
                        Err(e) => Poll::Ready(Some(Err(e.into()))),
                    }
                }
            };
        }
        let mut lister = self.lister.take().expect("list should not be None");
        let f = async move { (lister.next().await, lister) };
        self.next_future = Some(Box::pin(f));
        self.poll_next(cx)
    }
}