use std::path::Path;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
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
            batches.push(RecordBatch::from_bytes(&mut data)?);
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
                if let RecordValue::Topic(topic) = &record.value {
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
                if let RecordValue::Partition(partition) = &record.value {
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
                if let RecordValue::Topic(topic) = &record.value {
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

impl RecordBatch {
    fn from_bytes(src: &mut Bytes) -> Result<Self> {
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

        let records = NullableBytes::<Record>::deserialize(src);
        Ok(Self {
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
        })
    }
}

impl Serialize for RecordBatch {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i64(self.base_offset);
        b.put_i32(self.batch_length);
        b.put_i32(self.partition_leader_epoch);
        b.put_i8(self.magic);
        b.put_u32(self.crc);
        b.put_i16(self.attributes);
        b.put_i32(self.last_offset_delta);
        b.put_i64(self.base_timestamp);
        b.put_i64(self.max_timestamp);
        b.put_i64(self.producer_id);
        b.put_i16(self.producer_epoch);
        b.put_i32(self.base_sequence);
        b.freeze()
    }
}

pub struct Record {
    length: i64,
    attributes: i8,
    timestamp_delta: i64,
    offset_delta: i64,
    key: Vec<u8>,
    value_length: i64,
    pub value: RecordValue,
    headers: Vec<Header>,
}

impl Deserialize<Self> for Record {
    fn deserialize(src: &mut Bytes) -> Self {
        let length = i64::deserialize(src);
        let attributes = src.get_i8();
        let timestamp_delta = i64::deserialize(src);
        let offset_delta = i64::deserialize(src);

        let key_len = i64::deserialize(src);
        let key = if key_len > 0 {
            src.split_to(key_len as usize).to_vec()
        } else {
            Vec::new()
        };

        let value_length = i64::deserialize(src);
        let value = RecordValue::deserialize(src);
        let headers = CompactArray::<Header>::deserialize(src);

        Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_length,
            value,
            headers,
        }
    }
}

// TODO: Implement Header
struct Header;

impl Deserialize<Self> for Header {
    fn deserialize(_: &mut Bytes) -> Self {
        Header
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

struct FeatureLevelValue {
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
        assert_eq!(frame_version, 1);
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
                assert_eq!(version, 1);
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
                assert_eq!(version, 0);
                RecordValue::FeatureLevel(FeatureLevelValue {
                    name: CompactNullableString::deserialize(src),
                    level: src.get_u16(),
                })
            }
        };

        let tagged_fields_count = i64::deserialize(src);
        assert_eq!(tagged_fields_count, 0);
        value
    }
}
