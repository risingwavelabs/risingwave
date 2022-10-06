use serde::Deserialize;

pub mod enumerator;
pub mod source;
pub mod split;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

#[derive(Clone, Debug, Deserialize)]
pub struct PubsubProperties {
    #[serde(rename = "pubsub.split_count")]
    pub split_count: u32,

    #[serde(rename = "pubsub.subscription")]
    pub subscription: String,

    // use against the pubsub emulator
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    #[serde(rename = "pubsub.emulator_host")]
    pub credentials: Option<String>,
}
