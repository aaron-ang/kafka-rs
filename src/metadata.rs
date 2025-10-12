use std::path::Path;

use anyhow::Result;
use bytes::{Buf, Bytes};
use integer_encoding::*;
use num_enum::TryFromPrimitive;

use crate::protocol::*;

pub struct RecordBatches {
    batches: Vec<RecordBatch>,
}

impl RecordBatches {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let file_bytes = std::fs::read(path)?;
        let mut data = Bytes::from(file_bytes);
        let mut batches = Vec::new();
        while data.has_remaining() {
            batches.push(RecordBatch::deserialize(&mut data));
        }
        Ok(Self { batches })
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn raw_batch_for_topic(&self, topic_id: &Uuid, partition_id: i32) -> Result<Option<Bytes>> {
        let topic_name = self.find_topic_name(topic_id);
        if topic_name.as_deref().unwrap_or("").is_empty() {
            return Ok(None);
        }
        let file = format!(
            "/tmp/kraft-combined-logs/{}-{}/00000000000000000000.log",
            topic_name.unwrap(),
            partition_id
        );
        let file_bytes = std::fs::read(file)?;
        Ok(Some(Bytes::from(file_bytes)))
    }

    pub fn find_topic_id(&self, topic_name: &str) -> Option<Uuid> {
        self.batches
            .iter()
            .flat_map(|batch| &batch.records)
            .find_map(|record| {
                if let RecordValue::Topic(topic) = &record.value() {
                    (topic.topic_name.0.as_deref() == Some(topic_name))
                        .then(|| topic.topic_id.clone())
                } else {
                    None
                }
            })
    }

    pub fn validate_partition(&self, topic_id: &Uuid, partition_index: i32) -> bool {
        self.batches().iter().any(|batch| {
            batch.records.iter().any(|record| {
                if let RecordValue::Partition(partition) = &record.value() {
                    partition.topic_id == *topic_id && partition.partition_id == partition_index
                } else {
                    false
                }
            })
        })
    }

    fn find_topic_name(&self, topic_id: &Uuid) -> Option<String> {
        self.batches
            .iter()
            .flat_map(|batch| &batch.records)
            .find_map(|record| {
                if let RecordValue::Topic(topic) = &record.value() {
                    (topic.topic_id == *topic_id)
                        .then(|| topic.topic_name.0.clone().unwrap_or_default())
                } else {
                    None
                }
            })
    }
}

pub struct RecordBatch {
    base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    pub records: Vec<Record>,
}

impl Deserialize<Self> for RecordBatch {
    fn deserialize(src: &mut Bytes) -> Self {
        let base_offset = src.get_i64();
        let batch_length = src.get_i32();
        let partition_leader_epoch = src.get_i32();
        let magic = src.get_i8();
        let crc = src.get_u32();
        let attributes = src.get_i16();
        let last_offset_delta = src.get_i32();
        let base_timestamp = src.get_i64();
        let max_timestamp = src.get_i64();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let base_sequence = src.get_i32();
        let records_count = src.get_i32();
        let records = (0..records_count)
            .map(|_| Record::deserialize(src))
            .collect();
        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }
}

pub struct Record {
    length: i32,
    attributes: i8,
    timestamp_delta: i64,
    offset_delta: i32,
    key: Vec<u8>,
    value_bytes: Vec<u8>,
    headers: Vec<Header>,
}

impl Record {
    pub fn value(&self) -> RecordValue {
        RecordValue::deserialize(&mut Bytes::from(self.value_bytes.clone()))
    }
}

impl Deserialize<Self> for Record {
    fn deserialize(src: &mut Bytes) -> Self {
        let (length, read) = i32::decode_var(src).expect("Failed to decode record length");
        src.advance(read);

        let attributes = src.get_i8();

        let (timestamp_delta, read) =
            i64::decode_var(src).expect("Failed to decode timestamp_delta");
        src.advance(read);

        let (offset_delta, read) = i32::decode_var(src).expect("Failed to decode offset_delta");
        src.advance(read);

        let (key_len, read) = i32::decode_var(src).expect("Failed to decode key length");
        src.advance(read);
        let key = if key_len > 0 {
            src.split_to(key_len as usize).to_vec()
        } else {
            Vec::new()
        };

        let (value_length, read) = i32::decode_var(src).expect("Failed to decode value length");
        src.advance(read);
        let value_bytes = if value_length > 0 {
            src.split_to(value_length as usize).to_vec()
        } else {
            Vec::new()
        };

        let (headers_count, read) = i32::decode_var(src).expect("Failed to decode headers count");
        src.advance(read);
        let headers = (0..headers_count)
            .map(|_| Header::deserialize(src))
            .collect();

        Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_bytes,
            headers,
        }
    }
}

pub struct Header {
    key: String,
    value: Vec<u8>,
}

impl Deserialize<Self> for Header {
    fn deserialize(src: &mut Bytes) -> Self {
        // Header key length
        let (header_key_len, read) =
            i32::decode_var(src).expect("Failed to decode header key length");
        src.advance(read);

        // Header key (UTF-8 string)
        let header_key_bytes = src.split_to(header_key_len as usize);
        let key =
            String::from_utf8(header_key_bytes.to_vec()).expect("Invalid UTF-8 in header key");

        // Header value length
        let (header_value_len, read) =
            i32::decode_var(src).expect("Failed to decode header value length");
        src.advance(read);

        // Header value (byte array)
        let value = if header_value_len > 0 {
            src.split_to(header_value_len as usize).to_vec()
        } else {
            Vec::new()
        };

        Header { key, value }
    }
}

pub enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

pub struct TopicValue {
    pub topic_name: CompactNullableString,
    pub topic_id: Uuid,
}

pub struct PartitionValue {
    pub partition_id: i32,
    pub topic_id: Uuid,
    pub replicas: Vec<i32>,
    pub in_sync_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub leader_id: i32,
    pub leader_epoch: i32,
    partition_epoch: i32,
    directories: Vec<Uuid>,
}

pub struct FeatureLevelValue {
    name: CompactNullableString,
    level: u16,
}

#[derive(TryFromPrimitive)]
#[repr(u8)]
enum RecordType {
    Topic = 2,
    Partition,
    FeatureLevel = 12,
}

impl Deserialize<Self> for RecordValue {
    fn deserialize(src: &mut Bytes) -> Self {
        let frame_version = src.get_u8();
        assert_eq!(frame_version, 1, "frame_version must be 1");
        let record_type = RecordType::try_from(src.get_u8()).unwrap();
        let version = src.get_u8();

        let value = match record_type {
            RecordType::Topic => {
                assert_eq!(version, 0);
                RecordValue::Topic(TopicValue {
                    topic_name: CompactNullableString::deserialize(src),
                    topic_id: Uuid::deserialize(src),
                })
            }
            RecordType::Partition => {
                assert_eq!(version, 1, "version must be 1");
                let partition_id = src.get_i32();
                let topic_id = Uuid::deserialize(src);
                let replicas = CompactArray::<i32>::deserialize(src);
                let in_sync_replicas = CompactArray::<i32>::deserialize(src);
                let removing_replicas = CompactArray::<i32>::deserialize(src);
                let adding_replicas = CompactArray::<i32>::deserialize(src);
                let leader_id = src.get_i32();
                let leader_epoch = src.get_i32();
                let partition_epoch = src.get_i32();
                let directories = CompactArray::<Uuid>::deserialize(src);
                RecordValue::Partition(PartitionValue {
                    partition_id,
                    topic_id,
                    replicas,
                    in_sync_replicas,
                    removing_replicas,
                    adding_replicas,
                    leader_id,
                    leader_epoch,
                    partition_epoch,
                    directories,
                })
            }
            RecordType::FeatureLevel => {
                assert_eq!(version, 0, "version must be 0");
                RecordValue::FeatureLevel(FeatureLevelValue {
                    name: CompactNullableString::deserialize(src),
                    level: src.get_u16(),
                })
            }
        };

        let tagged_fields_count = i64::deserialize(src);
        assert_eq!(tagged_fields_count, 0, "tagged_fields_count must be 0");
        value
    }
}
