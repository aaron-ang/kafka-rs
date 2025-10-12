use std::{fs, io::Write};

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::metadata::RecordBatches;
use crate::protocol::*;

// ============================================================================
// MAIN REQUEST/RESPONSE STRUCTURES
// ============================================================================

#[derive(Debug)]
struct ProduceRequestV11 {
    transactional_id: CompactNullableString,
    required_acks: i16,
    timeout_ms: i32,
    topics: Vec<TopicRequest>,
}

impl Deserialize<Self> for ProduceRequestV11 {
    fn deserialize(src: &mut Bytes) -> Self {
        let transactional_id = CompactNullableString::deserialize(src);
        let required_acks = src.get_i16();
        let timeout_ms = src.get_i32();
        let topics = CompactArray::<TopicRequest>::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            transactional_id,
            required_acks,
            timeout_ms,
            topics,
        }
    }
}

#[derive(Debug)]
pub struct ProduceResponseV11 {
    header: HeaderV1,
    topics: CompactArray<TopicResponse>,
    throttle_time_ms: i32,
}

impl ProduceResponseV11 {
    fn new(header: HeaderV2, topics: Vec<TopicResponse>) -> Self {
        Self {
            header: HeaderV1::new(header.correlation_id),
            topics: CompactArray(topics),
            throttle_time_ms: 0,
        }
    }
}

impl Response for ProduceResponseV11 {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.header.serialize());
        bytes.put(self.topics.serialize());
        bytes.put_i32(self.throttle_time_ms);
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

// ============================================================================
// TOPIC-LEVEL STRUCTURES
// ============================================================================

#[derive(Debug)]
struct TopicRequest {
    topic_name: CompactString,
    partitions: Vec<PartitionRequest>,
}

impl Deserialize<Self> for TopicRequest {
    fn deserialize(src: &mut Bytes) -> Self {
        let topic_name = CompactString::deserialize(src);
        let partitions = CompactArray::<PartitionRequest>::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            topic_name,
            partitions,
        }
    }
}

#[derive(Debug)]
pub struct TopicResponse {
    topic_name: CompactString,
    partition_responses: CompactArray<PartitionResponse>,
}

impl Serialize for TopicResponse {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(self.topic_name.serialize());
        b.put(self.partition_responses.serialize());
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

// ============================================================================
// PARTITION-LEVEL STRUCTURES
// ============================================================================

#[derive(Debug)]
struct PartitionRequest {
    partition_index: i32,
    record_batches: CompactNullableBytes,
}

impl Deserialize<Self> for PartitionRequest {
    fn deserialize(src: &mut Bytes) -> Self {
        let partition_index = src.get_i32();
        let record_batches = CompactNullableBytes::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            partition_index,
            record_batches,
        }
    }
}

#[derive(Debug)]
struct PartitionResponse {
    partition_index: i32,
    error_code: ErrorCode,
    base_offset: i64,
    log_append_time_ms: i64,
    log_start_offset: i64,
    record_errors: CompactArray<RecordError>,
    error_message: CompactNullableString,
}

impl Serialize for PartitionResponse {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i32(self.partition_index);
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

// ============================================================================
// SUPPORTING STRUCTURES
// ============================================================================
#[derive(Debug)]
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

// ============================================================================
// REQUEST HANDLER
// ============================================================================

fn write_batch_to_log(batch_bytes: &Bytes, topic_name: &str, partition_index: i32) -> Result<()> {
    let log_dir = format!("/tmp/kraft-combined-logs/{topic_name}-{partition_index}");
    let log_file = format!("{log_dir}/00000000000000000000.log");
    fs::create_dir_all(&log_dir)?;
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?
        .write_all(batch_bytes)?;
    Ok(())
}

pub fn handle_request(header: HeaderV2, message: &mut Bytes) -> Result<ProduceResponseV11> {
    let req = ProduceRequestV11::deserialize(message);
    println!("request: {req:?}");
    let record_batches = RecordBatches::from_file(CLUSTER_METADATA_LOG_FILE)?;
    let mut responses = Vec::new();
    for topic in req.topics {
        let mut partitions = Vec::new();
        let topic_name = &topic.topic_name.0;
        let topic_uuid = record_batches.find_topic_id(topic_name);

        for partition in topic.partitions {
            let partition_exists = if let Some(ref uuid) = topic_uuid {
                record_batches.validate_partition(uuid, partition.partition_index)
            } else {
                false
            };
            let has_error = topic_uuid.is_none() || !partition_exists;
            let error_code = if has_error {
                ErrorCode::UnknownTopicOrPartition
            } else {
                ErrorCode::None
            };

            if !has_error {
                if let Some(record_batches_data) = &partition.record_batches.0 {
                    if let Err(e) = write_batch_to_log(
                        record_batches_data,
                        topic_name,
                        partition.partition_index,
                    ) {
                        eprintln!("Failed to write records to log: {e}");
                    }
                }
            }

            let partition_response = PartitionResponse {
                partition_index: partition.partition_index,
                error_code,
                base_offset: if has_error { -1 } else { 0 },
                log_append_time_ms: -1, // latest timestamp
                log_start_offset: if has_error { -1 } else { 0 },
                record_errors: CompactArray(Vec::new()),
                error_message: CompactNullableString(None),
            };
            partitions.push(partition_response);
        }
        let topic_response = TopicResponse {
            topic_name: topic.topic_name,
            partition_responses: CompactArray(partitions),
        };
        responses.push(topic_response);
    }
    Ok(ProduceResponseV11::new(header, responses))
}
