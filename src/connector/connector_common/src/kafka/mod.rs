pub mod private_link;
pub mod stats;

pub const KAFKA_PROPS_BROKER_KEY: &str = "properties.bootstrap.server";
pub const KAFKA_PROPS_BROKER_KEY_ALIAS: &str = "kafka.brokers";
pub const PRIVATELINK_CONNECTION: &str = "privatelink";
pub const KAFKA_ISOLATION_LEVEL: &str = "read_committed";
