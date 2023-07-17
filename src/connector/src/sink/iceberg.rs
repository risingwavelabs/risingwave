use crate::sink::remote::{CoordinatedRemoteSink, RemoteConfig};

pub const ICEBERG_SINK: &str = "iceberg";

pub type IcebergSink = CoordinatedRemoteSink;
pub type IcebergConfig = RemoteConfig;
