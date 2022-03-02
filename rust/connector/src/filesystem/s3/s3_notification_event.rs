use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NotificationEvent {
    pub records: Vec<SqsMessage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NotifyEventType {
    #[serde(rename = "ObjectCreated:Put")]
    ObjectCreatedPut,
    #[serde(rename = "ObjectCreated:Post")]
    ObjectCreatedPost,
    #[serde(rename = "ObjectCreated:Copy")]
    ObjectCreatedCopy,
    #[serde(rename = "ObjectCreated:CompleteMultipartUpload")]
    ObjectCreatedCompleteMultipartUpload,
    #[serde(rename = "ObjectRemoved:Delete")]
    ObjectRemovedDelete,
    #[serde(rename = "ObjectRemoved:DeleteMarkerCreated")]
    ObjectRemovedDeleteMarkerCreated,
    #[serde(rename = "ObjectRestore:Post")]
    ObjectRestorePost,
    #[serde(rename = "ObjectRestore:Completed")]
    ObjectRestoreCompleted,
    #[serde(rename = "ReducedRedundancyLostObject")]
    ReducedRedundancyLostObject,
    #[serde(rename = "Replication:OperationFailedReplication")]
    ReplicationOperationFailedReplication,
    #[serde(rename = "Replication:OperationMissedThreshold")]
    ReplicationOperationMissedThreshold,
    #[serde(rename = "Replication:OperationReplicatedAfterThreshold")]
    ReplicationOperationReplicatedAfterThreshold,
    #[serde(rename = "Replication:OperationNotTracked")]
    ReplicationOperationNotTracked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserIdentity {
    pub principal_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsMessage {
    pub event_version: String,
    pub event_source: String,
    pub aws_region: String,
    pub event_time: String,
    #[serde(rename = "eventName")]
    pub event_type: NotifyEventType,
    pub user_identity: UserIdentity,
    pub request_parameters: RequestParameters,
    pub response_elements: ResponseElements,
    pub s3: S3Info,
    pub glacier_event_data: Option<GlacierEventData>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestParameters {
    #[serde(rename = "sourceIPAddress")]
    pub source_ip: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseElements {
    #[serde(rename = "x-amz-request-id")]
    pub request_id: String,
    #[serde(rename = "x-amz-id-2")]
    pub id2: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerIdentity {
    pub principal_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Bucket {
    pub name: String,
    pub owner_identity: OwnerIdentity,
    pub arn: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Object {
    pub key: String,
    pub size: usize,
    pub e_tag: String,
    pub version_id: Option<String>,
    pub sequencer: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlacierEventData {
    pub restore_event_data: RestoreEventData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreEventData {
    pub lifecycle_restoration_expiry_time: String,
    pub lifecycle_restore_storage_class: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Info {
    pub s3_schema_version: String,
    pub configuration_id: String,
    pub bucket: S3Bucket,
    pub object: S3Object,
}
