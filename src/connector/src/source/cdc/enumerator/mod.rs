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

use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use jni::objects::{JByteArray, JValue, JValueOwned};
use prost::Message;
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_pb::connector_service::{SourceType, ValidateSourceRequest, ValidateSourceResponse};

use crate::source::cdc::{
    CdcProperties, CdcSourceTypeTrait, CdcSplitBase, Citus, DebeziumCdcSplit, MySqlCdcSplit, Mysql,
    Postgres, PostgresCdcSplit,
};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub const DATABASE_SERVERS_KEY: &str = "database.servers";

#[derive(Debug)]
pub struct DebeziumSplitEnumerator<T: CdcSourceTypeTrait> {
    /// The source_id in the catalog
    source_id: u32,
    worker_node_addrs: Vec<HostAddr>,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitEnumerator for DebeziumSplitEnumerator<T>
where
    Self: ListCdcSplits<CdcSourceType = T>,
{
    type Properties = CdcProperties<T>;
    type Split = DebeziumCdcSplit<T>;

    async fn new(
        props: CdcProperties<T>,
        context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<Self> {
        let server_addrs = props
            .props
            .get(DATABASE_SERVERS_KEY)
            .map(|s| {
                s.split(',')
                    .map(HostAddr::from_str)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        assert_eq!(
            props.get_source_type_pb(),
            SourceType::from(T::source_type())
        );

        let mut env = JVM.as_ref()?.attach_current_thread()?;

        let validate_source_request = ValidateSourceRequest {
            source_id: context.info.source_id as u64,
            source_type: props.get_source_type_pb() as _,
            properties: props.props,
            table_schema: Some(props.table_schema),
        };

        let validate_source_request_bytes =
            env.byte_array_from_slice(&Message::encode_to_vec(&validate_source_request))?;

        // validate connector properties
        let response = env.call_static_method(
            "com/risingwave/connector/source/JniSourceValidateHandler",
            "validate",
            "([B)[B",
            &[JValue::Object(&validate_source_request_bytes)],
        )?;

        let validate_source_response_bytes = match response {
            JValueOwned::Object(o) => unsafe { JByteArray::from_raw(o.into_raw()) },
            _ => unreachable!(),
        };

        let validate_source_response: ValidateSourceResponse = Message::decode(
            risingwave_jni_core::to_guarded_slice(&validate_source_response_bytes, &mut env)?
                .deref(),
        )?;

        validate_source_response.error.map_or(Ok(()), |err| {
            Err(anyhow!(format!(
                "source cannot pass validation: {}",
                err.error_message
            )))
        })?;

        tracing::debug!("validate cdc source properties success");
        Ok(Self {
            source_id: context.info.source_id,
            worker_node_addrs: server_addrs,
            _phantom: PhantomData,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<DebeziumCdcSplit<T>>> {
        Ok(self.list_cdc_splits())
    }
}

pub trait ListCdcSplits {
    type CdcSourceType: CdcSourceTypeTrait;
    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>>;
}

impl ListCdcSplits for DebeziumSplitEnumerator<Mysql> {
    type CdcSourceType = Mysql;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        let split = MySqlCdcSplit {
            inner: CdcSplitBase::new(self.source_id, None),
        };
        let dbz_split = DebeziumCdcSplit {
            mysql_split: Some(split),
            pg_split: None,
            _phantom: PhantomData,
        };
        vec![dbz_split]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Postgres> {
    type CdcSourceType = Postgres;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        let split = PostgresCdcSplit {
            inner: CdcSplitBase::new(self.source_id, None),
            server_addr: None,
        };
        let dbz_split = DebeziumCdcSplit {
            mysql_split: None,
            pg_split: Some(split),
            _phantom: Default::default(),
        };
        vec![dbz_split]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Citus> {
    type CdcSourceType = Citus;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        self.worker_node_addrs
            .iter()
            .enumerate()
            .map(|(id, addr)| {
                let split = PostgresCdcSplit {
                    inner: CdcSplitBase::new(id as u32, None),
                    server_addr: Some(addr.to_string()),
                };
                DebeziumCdcSplit {
                    mysql_split: None,
                    pg_split: Some(split),
                    _phantom: Default::default(),
                }
            })
            .collect_vec()
    }
}
