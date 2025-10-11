use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::cluster_metadata::{RecordBatches, RecordValue};
use crate::protocol::*;

const DEFAULT_UNKNOWN_TOPIC_UUID: &str = "00000000-0000-0000-0000-000000000000";

struct DescribeTopicPartitionsRequestV0 {
    topic_names: Vec<CompactNullableString>,
    response_partition_limit: i32,
    cursor: u8,
}

impl Deserialize<Self> for DescribeTopicPartitionsRequestV0 {
    fn deserialize(src: &mut Bytes) -> Self {
        let topic_names = CompactArray::<Topic>::deserialize(src);
        let response_partition_limit = src.get_i32();
        let cursor = src.get_u8();
        TagBuffer::deserialize(src);

        Self {
            topic_names,
            response_partition_limit,
            cursor,
        }
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponseV0 {
    header: HeaderV1,
    throttle_time_ms: i32,
    topics: CompactArray<Topic>,
    next_cursor: u8,
}

impl DescribeTopicPartitionsResponseV0 {
    fn new(correlation_id: i32, topics: Vec<Topic>) -> Self {
        Self {
            header: HeaderV1::new(correlation_id),
            throttle_time_ms: 0,
            topics: CompactArray(topics),
            next_cursor: 0xFF,
        }
    }
}

impl Response for DescribeTopicPartitionsResponseV0 {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.header.serialize());
        bytes.put_i32(self.throttle_time_ms);
        bytes.put(self.topics.serialize());
        bytes.put_u8(self.next_cursor);
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

pub fn handle_request(
    header: HeaderV2,
    message: &mut Bytes,
) -> Result<DescribeTopicPartitionsResponseV0> {
    let record_batches = RecordBatches::from_file(CLUSTER_METADATA_LOG_FILE)?;
    let topic_authorized_operations = 0x0DF;
    let req = DescribeTopicPartitionsRequestV0::deserialize(message);
    let mut topics = Vec::new();

    for record_batch in record_batches.batches() {
        for topic_name in &req.topic_names {
            let mut partitions = Vec::new();
            let mut topic_id = Uuid(DEFAULT_UNKNOWN_TOPIC_UUID.to_string());
            let mut topic_error_code = ErrorCode::UnknownTopicOrPartition;

            for rec in &record_batch.records {
                if let RecordValue::Topic(ref topic) = rec.value {
                    if topic.topic_name == *topic_name {
                        topic_id = topic.topic_id.clone();
                        topic_error_code = ErrorCode::None;
                    }
                }
                if let RecordValue::Partition(p) = &rec.value {
                    if p.topic_id == topic_id {
                        partitions.push(Partition::new(
                            ErrorCode::None,
                            p.partition_id,
                            p.leader_id,
                            p.leader_epoch,
                            p.replicas.clone(),
                            p.in_sync_replicas.clone(),
                            p.adding_replicas.clone(),
                            Vec::new(),
                            p.removing_replicas.clone(),
                        ));
                    }
                }
            }

            if !partitions.is_empty() {
                topics.push(Topic {
                    error_code: topic_error_code,
                    name: topic_name.clone(),
                    topic_id: topic_id.clone(),
                    is_internal: false,
                    partitions: CompactArray(partitions),
                    topic_authorized_operations,
                });
            }
        }
    }

    for requested_topic in req.topic_names {
        if !topics.iter().any(|t| t.name == requested_topic) {
            topics.push(Topic {
                error_code: ErrorCode::UnknownTopicOrPartition,
                name: requested_topic,
                topic_id: Uuid(DEFAULT_UNKNOWN_TOPIC_UUID.to_string()),
                is_internal: false,
                partitions: CompactArray(Vec::new()),
                topic_authorized_operations,
            });
        }
    }

    Ok(DescribeTopicPartitionsResponseV0::new(
        header.correlation_id,
        topics,
    ))
}

#[derive(Debug)]
struct Topic {
    error_code: ErrorCode,
    name: CompactNullableString,
    topic_id: Uuid,
    is_internal: bool,
    partitions: CompactArray<Partition>,
    topic_authorized_operations: i32,
}

impl Serialize for Topic {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i16(self.error_code.into());
        b.put(self.name.clone().serialize());
        b.put(self.topic_id.clone().serialize());
        b.put_u8(self.is_internal.into());
        b.put(self.partitions.serialize());
        b.put_i32(self.topic_authorized_operations);
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

impl Deserialize<CompactNullableString> for Topic {
    fn deserialize(src: &mut Bytes) -> CompactNullableString {
        let s = CompactNullableString::deserialize(src);
        TagBuffer::deserialize(src);
        s
    }
}

#[derive(Debug)]
struct Partition {
    error_code: ErrorCode,
    partition_index: u32,
    leader_id: u32,
    leader_epoch: u32,
    replicas: CompactArray<u32>,
    in_sync_replicas: CompactArray<u32>,
    eligible_leader_replicas: CompactArray<u32>,
    last_known_eligible_leader_replicas: CompactArray<u32>,
    offline_replicas: CompactArray<u32>,
}

impl Partition {
    fn new(
        error_code: ErrorCode,
        partition_index: u32,
        leader_id: u32,
        leader_epoch: u32,
        replicas: Vec<u32>,
        in_sync_replicas: Vec<u32>,
        eligible_leader_replicas: Vec<u32>,
        last_known_eligible_leader_replicas: Vec<u32>,
        offline_replicas: Vec<u32>,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replicas: CompactArray(replicas),
            in_sync_replicas: CompactArray(in_sync_replicas),
            eligible_leader_replicas: CompactArray(eligible_leader_replicas),
            last_known_eligible_leader_replicas: CompactArray(last_known_eligible_leader_replicas),
            offline_replicas: CompactArray(offline_replicas),
        }
    }
}

impl Serialize for Partition {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i16(self.error_code.into());
        b.put_u32(self.partition_index);
        b.put_u32(self.leader_id);
        b.put_u32(self.leader_epoch);
        b.put(self.replicas.serialize());
        b.put(self.in_sync_replicas.serialize());
        b.put(self.eligible_leader_replicas.serialize());
        b.put(self.last_known_eligible_leader_replicas.serialize());
        b.put(self.offline_replicas.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

impl Serialize for u32 {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32(*self);
        b.freeze()
    }
}
