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

//! Common parameters and utilities for both source and sink.

mod mqtt_common;
pub use mqtt_common::{MqttCommon, QualityOfService as MqttQualityOfService};

mod common;
pub use common::{
    AwsAuthProps, AwsPrivateLinkItem, KafkaCommon, KafkaConnectionProps, KafkaPrivateLinkCommon,
    KinesisCommon, KinesisSdkOptions, MongodbCommon, NatsCommon, PRIVATE_LINK_BROKER_REWRITE_MAP_KEY,
    PRIVATE_LINK_TARGETS_KEY, PulsarCommon, PulsarOauthCommon, RdKafkaPropertiesCommon,
};
mod connection;
pub use connection::{
    ConfluentSchemaRegistryConnection, Connection, ElasticsearchConnection, IcebergConnection,
    KafkaConnection, SCHEMA_REGISTRY_CONNECTION_TYPE, validate_connection,
};

mod iceberg;
#[cfg(not(madsim))]
mod maybe_tls_connector;
pub mod postgres;

pub use iceberg::IcebergCommon;
pub use postgres::{PostgresExternalTable, SslMode, create_pg_client};
