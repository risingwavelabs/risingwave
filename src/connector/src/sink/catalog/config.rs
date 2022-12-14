use std::collections::HashMap;

use enum_as_inner::EnumAsInner;

use crate::sink::console::ConsoleConfig;
use crate::sink::kafka::KafkaConfig;
use crate::sink::redis::RedisConfig;
use crate::sink::remote::RemoteConfig;
use crate::sink::{MySqlConfig, Result, SinkError};
#[derive(Clone, Debug, EnumAsInner)]
pub enum SinkConfig {
    Mysql(MySqlConfig),
    Redis(RedisConfig),
    Kafka(KafkaConfig),
    Remote(RemoteConfig),
    Console(ConsoleConfig),
    BlackHole,
}

pub const BLACKHOLE_SINK: &str = "blackhole";

impl SinkConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        const SINK_TYPE_KEY: &str = "connector";
        let sink_type = properties
            .get(SINK_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(format!("missing config: {}", SINK_TYPE_KEY)))?;
        match sink_type.to_lowercase().as_str() {
            KAFKA_SINK => Ok(SinkConfig::Kafka(KafkaConfig::from_hashmap(properties)?)),
            MYSQL_SINK => Ok(SinkConfig::Mysql(MySqlConfig::from_hashmap(properties)?)),
            CONSOLE_SINK => Ok(SinkConfig::Console(ConsoleConfig::from_hashmap(
                properties,
            )?)),
            BLACKHOLE_SINK => Ok(SinkConfig::BlackHole),
            _ => Ok(SinkConfig::Remote(RemoteConfig::from_hashmap(properties)?)),
        }
    }

    pub fn into_hashmap(self) -> HashMap<String, String> {
        todo!()
    }

    pub fn get_connector(&self) -> &'static str {
        match self {
            SinkConfig::Mysql(_) => "mysql",
            SinkConfig::Kafka(_) => "kafka",
            SinkConfig::Redis(_) => "redis",
            SinkConfig::Remote(_) => "remote",
            SinkConfig::Console(_) => "console",
            SinkConfig::BlackHole => "blackhole",
        }
    }
}
