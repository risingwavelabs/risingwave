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

//! Helpers for altering the bootstrap servers of a Kafka source, sink or connection on the fly
//! (`ALTER ... CONNECTOR WITH (properties.bootstrap.server = '...')`).
//!
//! Changing the bootstrap servers is meant for address changes of the *same* logical cluster
//! (broker migration, DNS rename, endpoint change). A source keeps its split state (topic
//! partitions and offsets), so pointing it to a cluster with incompatible offsets silently skips
//! or re-reads messages. The helpers here let the catalog layer validate and warn about this.

use std::collections::BTreeMap;

use risingwave_common::bail;
use thiserror_ext::AsReport;

use crate::WithOptionsSecResolved;
use crate::connector_common::PRIVATE_LINK_BROKER_REWRITE_MAP_KEY;
use crate::error::ConnectorResult;
use crate::source::ConnectorProperties;
use crate::source::kafka::{
    KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS, KafkaProperties, fetch_kafka_cluster_id,
};

/// All WITH option keys that refer to the Kafka bootstrap servers.
pub const KAFKA_BROKER_KEYS: [&str; 2] = [KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS];

/// Returns whether `altered_keys` contains a key referring to the Kafka bootstrap servers.
pub fn alters_kafka_brokers<'a>(altered_keys: impl IntoIterator<Item = &'a str>) -> bool {
    altered_keys
        .into_iter()
        .any(|key| KAFKA_BROKER_KEYS.contains(&key))
}

/// Rejects altering the bootstrap servers of a Kafka object created with `PrivateLink`, unless the
/// broker rewrite map is altered along with them (only possible for admin operations).
///
/// The broker rewrite map (`broker.rewrite.endpoints`) is derived from the original bootstrap
/// servers when the object is created and cannot be recomputed here, so the altered brokers would
/// not be rewritten to the `PrivateLink` endpoints.
pub fn ensure_kafka_brokers_alterable<'a>(
    current_props: &BTreeMap<String, String>,
    altered_keys: impl IntoIterator<Item = &'a str>,
) -> ConnectorResult<()> {
    let altered_keys: Vec<&str> = altered_keys.into_iter().collect();
    if alters_kafka_brokers(altered_keys.iter().copied())
        && current_props.contains_key(PRIVATE_LINK_BROKER_REWRITE_MAP_KEY)
        && !altered_keys.contains(&PRIVATE_LINK_BROKER_REWRITE_MAP_KEY)
    {
        bail!(
            "cannot alter `{KAFKA_PROPS_BROKER_KEY}` of a Kafka object created with PrivateLink, \
             because the broker rewrite map is derived from the original bootstrap servers. \
             Drop and recreate it instead."
        );
    }
    Ok(())
}

/// Extracts [`KafkaProperties`] from the plaintext WITH options of a Kafka source.
pub fn kafka_properties_from_plaintext(
    props: BTreeMap<String, String>,
) -> ConnectorResult<KafkaProperties> {
    match ConnectorProperties::extract(WithOptionsSecResolved::without_secrets(props), false)? {
        ConnectorProperties::Kafka(props) => Ok(*props),
        _ => bail!("expected kafka source properties"),
    }
}

/// Compares the Kafka clusters behind the previous and the new bootstrap servers of a source and
/// returns notices for the user. This is best effort and never fails: the previous brokers may
/// already be unreachable, which is a common reason for changing the address.
///
/// The source keeps its partition offsets across the change. That is only correct if the new
/// address points to the same logical cluster (or a cluster that preserves offsets, e.g.
/// Confluent Cluster Linking), so a differing cluster id is reported as a warning.
pub async fn kafka_broker_change_notices(
    previous: &KafkaProperties,
    new: &KafkaProperties,
) -> Vec<String> {
    let previous_brokers = &previous.connection.brokers;
    let new_brokers = &new.connection.brokers;
    let mut notices = vec![format!(
        "Kafka bootstrap servers changed from '{previous_brokers}' to '{new_brokers}'. \
         The current partition offsets are kept, which is only correct if the new address points \
         to the same logical Kafka cluster or to one with identical offsets."
    )];

    let (previous_id, new_id) = tokio::join!(
        fetch_kafka_cluster_id(previous),
        fetch_kafka_cluster_id(new)
    );
    let notice = match (previous_id, new_id) {
        (Ok(Some(previous_id)), Ok(Some(new_id))) if previous_id == new_id => format!(
            "Both addresses report Kafka cluster id '{new_id}', so the existing offsets remain valid."
        ),
        (Ok(Some(previous_id)), Ok(Some(new_id))) => format!(
            "WARNING: the previous brokers report Kafka cluster id '{previous_id}' but the new \
             brokers report '{new_id}'. Offsets of a different cluster are most likely \
             incompatible, so messages may be skipped or ingested twice. Consider recreating the \
             source or resetting its offsets."
        ),
        (previous_id, new_id) => {
            let describe = |id: ConnectorResult<Option<String>>| match id {
                Ok(Some(id)) => format!("'{id}'"),
                Ok(None) => "unknown".to_owned(),
                Err(e) => format!("unavailable ({})", e.as_report()),
            };
            format!(
                "Could not compare the Kafka cluster ids of the previous brokers ({}) and the new \
                 brokers ({}). Make sure the new address points to the same logical cluster.",
                describe(previous_id),
                describe(new_id),
            )
        }
    };
    notices.push(notice);
    notices
}

#[cfg(test)]
mod tests {
    use maplit::btreemap;

    use super::*;

    #[test]
    fn test_alters_kafka_brokers() {
        assert!(alters_kafka_brokers(["properties.bootstrap.server"]));
        assert!(alters_kafka_brokers(["topic", "kafka.brokers"]));
        assert!(!alters_kafka_brokers(["properties.sasl.username"]));
        assert!(!alters_kafka_brokers([]));
    }

    #[test]
    fn test_ensure_kafka_brokers_alterable() {
        let plain = btreemap! {
            "properties.bootstrap.server".to_owned() => "b-1:9092".to_owned(),
        };
        ensure_kafka_brokers_alterable(&plain, ["properties.bootstrap.server"]).unwrap();

        let private_link = btreemap! {
            "properties.bootstrap.server".to_owned() => "b-1:9092".to_owned(),
            "broker.rewrite.endpoints".to_owned() => r#"{"b-1:9092":"dns-1:9092"}"#.to_owned(),
        };
        // Other properties can still be altered.
        ensure_kafka_brokers_alterable(&private_link, ["properties.sasl.username"]).unwrap();
        let err = ensure_kafka_brokers_alterable(&private_link, ["kafka.brokers"]).unwrap_err();
        assert!(err.to_string().contains("PrivateLink"), "{err}");
        // Admin operations may replace the rewrite map along with the brokers.
        ensure_kafka_brokers_alterable(
            &private_link,
            ["properties.bootstrap.server", "broker.rewrite.endpoints"],
        )
        .unwrap();
    }

    #[test]
    fn test_kafka_properties_from_plaintext() {
        let props = kafka_properties_from_plaintext(btreemap! {
            "connector".to_owned() => "kafka".to_owned(),
            "topic".to_owned() => "t".to_owned(),
            "kafka.brokers".to_owned() => "b-1:9092".to_owned(),
        })
        .unwrap();
        assert_eq!(props.connection.brokers, "b-1:9092");

        kafka_properties_from_plaintext(btreemap! {
            "connector".to_owned() => "pulsar".to_owned(),
        })
        .unwrap_err();
    }
}
