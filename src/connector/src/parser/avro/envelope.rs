use anyhow;
use apache_avro::types::Value;
use apache_avro::Schema;
use base64::engine::general_purpose;
use base64::Engine;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Cursor;

const ENVELOPE_SCHEMA: &str = include_str!("envelope_schema.avsc");

#[derive(Debug)]
pub struct Meta {
    pub schema_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct Envelope {
    pub uuid: Vec<u8>,
    pub message_type: String,
    pub schema_id: u64,
    pub payload: Vec<u8>,
    pub meta: Vec<Meta>,
    pub previous_payload: Option<Vec<u8>>,
    pub timestamp: u64,
}

// TODO: use from_value to convert to  instead of this
impl TryFrom<Value> for Envelope {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let mut envelope = HashMap::<String, Value>::new();

        if let Value::Record(fields) = value {
            for field in fields {
                envelope.insert(field.0.to_string(), field.1);
            }
        } else {
            anyhow::bail!("failed to convert value to record")
        }

        let uuid: Vec<u8>;
        let message_type: String;
        let mut schema_id: u64;
        let payload: Vec<u8>;
        let mut meta: Vec<Meta>;
        let previous_payload: Option<Vec<u8>>;
        let timestamp: u64;

        // create Envelope object from envelope hash map
        if let Some(Value::Fixed(_size, uuid_bytes)) = envelope.get("uuid") {
            uuid = uuid_bytes.to_vec();
        } else {
            anyhow::bail!("failed to get uuid from envelope")
        }

        if let Some(Value::Enum(_message_type_int, message_type_str)) = envelope.get("message_type")
        {
            message_type = message_type_str.to_string();
        } else {
            anyhow::bail!("failed to get message_type from envelope")
        }

        if let Some(Value::Int(schema_id_int)) = envelope.get("schema_id") {
            schema_id = *schema_id_int as u64;
        } else {
            anyhow::bail!("failed to get schema_id from envelope")
        }

        if let Some(Value::Bytes(payload_bytes)) = envelope.get("payload") {
            payload = payload_bytes.to_vec();
        } else {
            anyhow::bail!("failed to get payload from envelope")
        }

        if let Some(Value::Union(_idx, meta_array)) = envelope.get("meta") {
            meta = Vec::new();
            let meta_array = meta_array.as_ref();

            if let Value::Array(meta_value) = meta_array {
                for value in meta_value {
                    if let Value::Record(meta_fields) = value {
                        let mut meta_hashmap = HashMap::<String, Value>::new();

                        for field in meta_fields {
                            meta_hashmap.insert(field.0.to_string(), field.1.clone());
                        }

                        if let Some(Value::Int(meta_schema_id_int)) = meta_hashmap.get("schema_id")
                        {
                            schema_id = *meta_schema_id_int as u64;
                        } else {
                            anyhow::bail!("failed to get schema_id from meta")
                        }

                        if let Some(Value::Bytes(meta_payload_bytes)) = meta_hashmap.get("payload")
                        {
                            meta.push(Meta {
                                schema_id,
                                payload: meta_payload_bytes.to_vec(),
                            });
                        } else {
                            anyhow::bail!("failed to get payload from meta")
                        }
                    } else {
                        anyhow::bail!("failed to convert meta value to record")
                    }
                }
            }
        } else {
            anyhow::bail!("failed to get meta from envelope")
        }

        if let Some(Value::Bytes(previous_payload_bytes)) = envelope.get("previous_payload") {
            previous_payload = Some(previous_payload_bytes.to_vec());
        } else {
            previous_payload = None;
        }

        if let Some(Value::Long(timestamp_int)) = envelope.get("timestamp") {
            timestamp = *timestamp_int as u64;
        } else {
            anyhow::bail!("failed to get timestamp from envelope")
        }

        Ok(Envelope {
            uuid,
            message_type,
            schema_id,
            payload,
            meta,
            previous_payload,
            timestamp,
        })
    }
}


pub fn deserialize_envelope<T: AsRef<[u8]>>(bytes: T) -> anyhow::Result<Envelope> {
    let bytes = bytes.as_ref();
    // encoding logic: https://sourcegraph.yelpcorp.com/python-packages/data_pipeline/-/blob/data_pipeline/envelope.py?L99
    assert!(!bytes.is_empty());
    // if this magic byte is set, decode from base64 to ASCII before deserializing
    let ascii_magic_byte = b'a';
    // setting this byte means that the timestamp is in seconds
    let _base64_magic_byte = b'0';
    // setting this byte means that the timestamp is in microseconds
    let _base64_magic_byte_toggled = b'1';

    let magic_byte = bytes.first().expect("this should never happen");
    let bytes = &bytes[1..];

    let envelope_schema = Schema::parse_str(ENVELOPE_SCHEMA)?;

    let envelope: Value;

    if *magic_byte == ascii_magic_byte {
        let decoded_bytes = general_purpose::URL_SAFE.decode(bytes)?;

        let _timestamp_magic_byte = decoded_bytes[0];

        let mut cursor = Cursor::new(decoded_bytes[1..].to_vec());

        envelope =
            apache_avro::from_avro_datum(&envelope_schema, &mut cursor, Some(&envelope_schema))?;
    } else {
        let mut cursor = Cursor::new(bytes);
        envelope =
            apache_avro::from_avro_datum(&envelope_schema, &mut cursor, Some(&envelope_schema))?;
    }

    Envelope::try_from(envelope)
}