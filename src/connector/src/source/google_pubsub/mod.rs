use std::collections::HashMap;

use serde::Deserialize;

// pub mod enumerator;
pub mod source;

pub const GOOGLE_PUBSUB_CONNECTOR: &str = "google_pubsub";

#[derive(Clone, Debug, Deserialize)]
pub struct PubsubProperties {
    #[serde(rename = "topic", alias = "pubsub.topic")]
    pub topic: String,

    #[serde(rename = "split_count", alias = "pubsub.split_count")]
    pub split_count: u32,

    // existing subscription -- or should that even be an option?
    // ? question: can we generate this
    #[serde(rename = "subscription", alias = "pubsub.subscription")]
    pub subscription: String,

    // optionally filter by attribute
    #[serde(rename = "attributes", alias = "pubsub.attributes")]
    pub attributes: Option<HashMap<String, String>>,

    // use against the pubsub emulator
    #[serde(rename = "emulator_host", alias = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    // @todo: more relevant properties: region, access key, secret, session token? (maybe)
    // roles: not sure
}
