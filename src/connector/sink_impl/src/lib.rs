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

#![expect(dead_code)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(box_patterns)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(lint_reasons)]
#![feature(lazy_cell)]
#![feature(result_option_inspect)]
#![feature(let_chains)]
#![feature(box_into_inner)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(impl_trait_in_assoc_type)]
#![feature(if_let_guard)]
#![feature(iterator_try_collect)]

pub mod sink;

use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::FutureExt;
use maplit::{convert_args, hashmap};
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_connector::*;
use risingwave_pb::catalog::PbSink;
use risingwave_sqlparser::ast::{Encode, Format};

use crate::sink::boxed::BoxCoordinator;
use crate::sink::catalog::SinkCatalog;
use crate::sink::kafka::KafkaSink;
use crate::sink::kinesis::KinesisSink;
use crate::sink::pulsar::PulsarSink;
use crate::sink::redis::RedisSink;
use crate::sink::{build_sink, Sink, SinkError, SinkParam};

#[macro_export]
macro_rules! enable {
    () => {
        // Impls are registered at load time (via `#[ctor]`). This only happens
        // if the impl crate is linked, which only happens if this crate is `use`d.
        use risingwave_sink_impl as _;
    };
}

#[ctor::ctor]
fn __register_sink_impl_fn() {
    risingwave_connector::sink::__sink_impl_functions::set(
        risingwave_connector::sink::__sink_impl_functions::SinkImplItems {
            build_box_coordinator,
            validate_sink,
            default_sink_decouple,
            sink_names: get_sink_names(),
            sink_compatible_format: get_sink_compatible_format(),
        },
    );
}

fn get_sink_names() -> HashSet<&'static str> {
    let mut ret = HashSet::new();
    macro_rules! add_sink_name {
        ({$({$variant_name:ident, $type_name:ty}),*}) => {{
            $(
                {
                    assert!(ret.insert(<$type_name>::SINK_NAME));
                }
            )*
        }}
    }
    for_all_sinks!(add_sink_name);
    ret
}

fn get_sink_compatible_format() -> HashMap<String, HashMap<Format, Vec<Encode>>> {
    convert_args!(hashmap!(
            KafkaSink::SINK_NAME => hashmap!(
                Format::Plain => vec![Encode::Json, Encode::Protobuf],
                Format::Upsert => vec![Encode::Json],
                Format::Debezium => vec![Encode::Json],
            ),
            KinesisSink::SINK_NAME => hashmap!(
                Format::Plain => vec![Encode::Json],
                Format::Upsert => vec![Encode::Json],
                Format::Debezium => vec![Encode::Json],
            ),
            PulsarSink::SINK_NAME => hashmap!(
                Format::Plain => vec![Encode::Json],
                Format::Upsert => vec![Encode::Json],
                Format::Debezium => vec![Encode::Json],
            ),
            RedisSink::SINK_NAME => hashmap!(
                Format::Plain => vec![Encode::Json,Encode::Template],
                Format::Upsert => vec![Encode::Json,Encode::Template],
            ),
    ))
}

fn default_sink_decouple(name: &str, desc: &SinkDesc) -> Result<bool, SinkError> {
    match_sink_name_str!(
        name,
        SinkTypeName,
        Ok(<SinkTypeName>::default_sink_decouple(desc)),
        |unsupported| Err(SinkError::Config(anyhow!(
            "unsupported sink: {}",
            unsupported
        )))
    )
}

fn validate_sink(prost_sink_catalog: &PbSink) -> BoxFuture<'_, Result<(), SinkError>> {
    async move {
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::from(sink_catalog);

        let sink = build_sink(param)?;

        dispatch_sink!(sink, sink, Ok(sink.validate().await?))
    }
    .boxed()
}

fn build_box_coordinator(
    param: SinkParam,
) -> BoxFuture<'static, std::result::Result<BoxCoordinator, SinkError>> {
    async move {
        let sink = build_sink(param)?;
        dispatch_sink!(sink, sink, {
            let coordinator = sink.new_coordinator().await?;
            Ok(Box::new(coordinator) as BoxCoordinator)
        })
    }
    .boxed()
}
