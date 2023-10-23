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

pub mod blackhole;
pub mod clickhouse;
pub mod coordinate;
pub mod doris;
pub mod doris_connector;
pub mod iceberg;
pub mod kafka;
pub mod kinesis;
pub mod nats;
pub mod pulsar;
pub mod redis;
pub mod remote;
pub mod test_sink;

use anyhow::anyhow;
pub use risingwave_connector::sink::Sink;
pub(crate) use risingwave_connector::sink::*;
pub use tracing;

use crate::sink::writer::SinkWriter;

#[macro_export]
macro_rules! for_all_sinks {
    ($macro:path $(, $arg:tt)*) => {
        $macro! {
            {
                { Redis, $crate::sink::redis::RedisSink },
                { Kafka, $crate::sink::kafka::KafkaSink },
                { Pulsar, $crate::sink::pulsar::PulsarSink },
                { BlackHole, $crate::sink::blackhole::BlackHoleSink },
                { Kinesis, $crate::sink::kinesis::KinesisSink },
                { ClickHouse, $crate::sink::clickhouse::ClickHouseSink },
                { Iceberg, $crate::sink::iceberg::IcebergSink },
                { Nats, $crate::sink::nats::NatsSink },
                { RemoteIceberg, $crate::sink::iceberg::RemoteIcebergSink },
                { Jdbc, $crate::sink::remote::JdbcSink },
                { DeltaLake, $crate::sink::remote::DeltaLakeSink },
                { ElasticSearch, $crate::sink::remote::ElasticSearchSink },
                { Cassandra, $crate::sink::remote::CassandraSink },
                { Doris, $crate::sink::doris::DorisSink },
                { Test, $crate::sink::test_sink::TestSink }
            }
            $(,$arg)*
        }
    };
}

#[macro_export]
macro_rules! dispatch_sink {
    ({$({$variant_name:ident, $sink_type:ty}),*}, $impl:tt, $sink:tt, $body:tt) => {{
        use $crate::sink::SinkImpl;

        match $impl {
            $(
                SinkImpl::$variant_name($sink) => $body,
            )*
        }
    }};
    ($impl:expr, $sink:ident, $body:expr) => {{
        $crate::for_all_sinks! {$crate::dispatch_sink, {$impl}, $sink, {$body}}
    }};
}

#[macro_export]
macro_rules! match_sink_name_str {
    ({$({$variant_name:ident, $sink_type:ty}),*}, $name_str:tt, $type_name:ident, $body:tt, $on_other_closure:tt) => {{
        use $crate::sink::Sink;
        match $name_str {
            $(
                <$sink_type>::SINK_NAME => {
                    type $type_name = $sink_type;
                    {
                        $body
                    }
                },
            )*
            other => ($on_other_closure)(other),
        }
    }};
    ($name_str:expr, $type_name:ident, $body:expr, $on_other_closure:expr) => {{
        $crate::for_all_sinks! {$crate::match_sink_name_str, {$name_str}, $type_name, {$body}, {$on_other_closure}}
    }};
}

impl SinkImpl {
    pub fn new(mut param: SinkParam) -> Result<Self> {
        const CONNECTION_NAME_KEY: &str = "connection.name";
        const PRIVATE_LINK_TARGET_KEY: &str = "privatelink.targets";

        // remove privatelink related properties if any
        param.properties.remove(PRIVATE_LINK_TARGET_KEY);
        param.properties.remove(CONNECTION_NAME_KEY);

        let sink_type = param
            .properties
            .get(CONNECTOR_TYPE_KEY)
            .ok_or_else(|| anyhow!("missing config: {}", CONNECTOR_TYPE_KEY))?;
        match_sink_name_str!(
            sink_type.to_lowercase().as_str(),
            SinkType,
            Ok(SinkType::try_from(param)?.into()),
            |other| {
                Err(SinkError::Config(anyhow!(
                    "unsupported sink connector {}",
                    other
                )))
            }
        )
    }
}

pub fn build_sink(param: SinkParam) -> Result<SinkImpl> {
    SinkImpl::new(param)
}

macro_rules! def_sink_impl {
    () => {
        $crate::for_all_sinks! { def_sink_impl }
    };
    ({ $({ $variant_name:ident, $sink_type:ty }),* }) => {
        #[derive(Debug)]
        pub enum SinkImpl {
            $(
                $variant_name($sink_type),
            )*
        }

        $(
            impl From<$sink_type> for SinkImpl {
                fn from(sink: $sink_type) -> SinkImpl {
                    SinkImpl::$variant_name(sink)
                }
            }
        )*
    };
}

def_sink_impl!();
