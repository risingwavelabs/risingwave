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

use std::collections::HashMap;
use std::marker::PhantomData;

use anyhow::anyhow;
use opendal::Operator;
use risingwave_common::catalog::Schema;

use crate::sink::file_sink::OpenDalSinkWriter;
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkError, SinkFormatDesc, SinkParam};

#[derive(Debug, Clone)]
pub struct FileSink<S: OpendalSinkBackend> {
    pub(crate) op: Operator,
    pub(crate) path: String,
    // prefix is used to reduce the number of objects to be listed
    pub(crate) schema: Schema,
    pub(crate) is_append_only: bool,
    pub(crate) pk_indices: Vec<usize>,
    pub(crate) format_desc: SinkFormatDesc,
    pub(crate) marker: PhantomData<S>,
}

pub trait OpendalSinkBackend: Send + Sync + 'static + Clone + PartialEq {
    type Properties: Send + Sync;
    const SINK_NAME: &'static str;

    fn from_hashmap(hash_map: HashMap<String, String>) -> Result<Self::Properties>;
    fn new_operator(properties: Self::Properties) -> Result<Operator>;
    fn get_path(properties: &Self::Properties) -> String;
}

impl<S: OpendalSinkBackend> Sink for FileSink<S> {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = S::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        Ok(OpenDalSinkWriter::new(
            self.op.clone(),
            &self.path,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
            writer_param.executor_id,
            self.format_desc.encode.clone(),
        )?
        .into_log_sinker(writer_param.sink_metrics))
    }
}

impl<S: OpendalSinkBackend> TryFrom<SinkParam> for FileSink<S> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S::from_hashmap(param.properties)?;
        let path = S::get_path(&config);
        let op = S::new_operator(config)?;

        Ok(Self {
            op,
            path,
            schema,
            is_append_only: param.sink_type.is_append_only(),
            pk_indices: param.downstream_pk,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            marker: PhantomData,
        })
    }
}
