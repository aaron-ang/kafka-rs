use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::*;

// Produce-specific request structures
struct ProduceRequestV11 {
    transactional_id: CompactNullableString,
    required_acks: i16,
    timeout_ms: u32,
    topics: Vec<ProduceTopicRequest>,
}

struct ProduceTopicRequest {
    topic_id: Uuid,
    partitions: Vec<ProducePartitionRequest>,
}

struct ProducePartitionRequest {
    partition_index: u32,
    record_batches: Vec<CompactNullableBytes>,
}

// Produce-specific response structures
pub struct ProduceTopicResponse {
    topic_id: Uuid,
    partitions: CompactArray<ProducePartitionResponse>,
}

#[derive(Clone)]
struct ProducePartitionResponse {
    partition_index: u32,
    error_code: ErrorCode,
    base_offset: i64,
    log_append_time_ms: i64,
    log_start_offset: i64,
    record_errors: CompactArray<RecordError>,
    error_message: CompactNullableString,
}

#[derive(Clone)]
struct RecordError {
    batch_index: i32,
    batch_index_error_message: CompactNullableString,
}

impl Serialize for RecordError {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i32(self.batch_index);
        b.put(self.batch_index_error_message.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

impl Deserialize<Self> for ProduceRequestV11 {
    fn deserialize(src: &mut Bytes) -> Self {
        let transactional_id = CompactNullableString::deserialize(src);
        let required_acks = src.get_i16();
        let timeout_ms = src.get_u32();
        let topics = CompactArray::<ProduceTopicRequest>::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            transactional_id,
            required_acks,
            timeout_ms,
            topics,
        }
    }
}

impl Deserialize<Self> for ProduceTopicRequest {
    fn deserialize(src: &mut Bytes) -> Self {
        let topic_id = Uuid::deserialize(src);
        let partitions = CompactArray::<ProducePartitionRequest>::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            topic_id,
            partitions,
        }
    }
}

impl Deserialize<Self> for ProducePartitionRequest {
    fn deserialize(src: &mut Bytes) -> Self {
        let partition_index = src.get_u32();
        let record_batches = CompactArray::<CompactNullableBytes>::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            partition_index,
            record_batches,
        }
    }
}

pub fn handle_request(header: HeaderV2, message: &mut Bytes) -> Result<ProduceResponseV11> {
    let req = ProduceRequestV11::deserialize(message);
    let mut responses = Vec::new();

    for topic in req.topics {
        let mut partitions = Vec::new();

        for partition in topic.partitions {
            let partition_response = ProducePartitionResponse {
                partition_index: partition.partition_index,
                error_code: ErrorCode::None, // For now, assume success
                base_offset: 0,              // For now, return 0
                log_append_time_ms: 0,
                log_start_offset: 0,
                record_errors: CompactArray(Vec::new()),
                error_message: CompactNullableString(None),
            };
            partitions.push(partition_response);
        }

        let topic_response = ProduceTopicResponse {
            topic_id: topic.topic_id,
            partitions: CompactArray(partitions),
        };
        responses.push(topic_response);
    }

    Ok(ProduceResponseV11::new(header, responses))
}

pub struct ProduceResponseV11 {
    header: HeaderV1,
    topics: CompactArray<ProduceTopicResponse>,
    throttle_time_ms: i32,
}

impl ProduceResponseV11 {
    fn new(header: HeaderV2, topics: Vec<ProduceTopicResponse>) -> Self {
        Self {
            header: HeaderV1::new(header.correlation_id),
            topics: CompactArray(topics),
            throttle_time_ms: 0,
        }
    }
}

impl Serialize for ProduceTopicResponse {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(self.topic_id.serialize());
        b.put(self.partitions.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

impl Serialize for ProducePartitionResponse {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i32(self.partition_index as i32);
        b.put_i16(self.error_code.into());
        b.put_i64(self.base_offset);
        b.put_i64(self.log_append_time_ms);
        b.put_i64(self.log_start_offset);
        b.put(self.record_errors.serialize());
        b.put(self.error_message.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

impl Response for ProduceResponseV11 {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.header.serialize());
        bytes.put_i32(self.throttle_time_ms);
        bytes.put(self.topics.serialize());
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}
