use std::fmt::Display;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};

pub const CLUSTER_METADATA_LOG_FILE: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub trait Response {
    fn as_bytes(&self) -> Bytes;
}

pub trait Serialize {
    fn serialize(&self) -> Bytes;
}

pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> T;
}

#[derive(Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

#[derive(Debug, Clone, Copy, IntoPrimitive)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    UnknownTopicId = 100,
}

pub struct HeaderV0 {
    correlation_id: i32,
}

impl HeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl Serialize for HeaderV0 {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_i32(self.correlation_id);
        bytes.freeze()
    }
}

#[derive(Debug)]
pub struct HeaderV1 {
    correlation_id: i32,
}

impl HeaderV1 {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl Serialize for HeaderV1 {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5);
        bytes.put_i32(self.correlation_id);
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

pub struct HeaderV2 {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: NullableString,
}

impl Deserialize<Self> for HeaderV2 {
    fn deserialize(src: &mut Bytes) -> Self {
        let api_key = src.get_i16();
        let api_version = src.get_i16();
        let correlation_id = src.get_i32();
        let client_id = NullableString::deserialize(src);
        TagBuffer::deserialize(src);
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Uuid(pub String);

impl Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Uuid {
    fn serialize(&self) -> Bytes {
        let hex_str = self.0.replace('-', "");
        let mut b = BytesMut::with_capacity(32);
        b.extend_from_slice(&hex::decode(hex_str).expect("valid UUID string"));
        b.freeze()
    }
}

impl Deserialize<Self> for Uuid {
    fn deserialize(src: &mut Bytes) -> Self {
        let mut s = hex::encode(src.split_to(16));
        s.insert(8, '-');
        s.insert(13, '-');
        s.insert(18, '-');
        s.insert(23, '-');
        Self(s)
    }
}

pub struct NullableString(pub Option<String>);

impl Deserialize<Self> for NullableString {
    fn deserialize(src: &mut Bytes) -> Self {
        let len = src.get_i16();
        let string_len = if len == -1 { 0 } else { len as usize };
        if string_len == 0 || src.remaining() < string_len {
            return Self(None);
        }
        let bytes = src.split_to(string_len);
        Self(Some(String::from_utf8(bytes.to_vec()).unwrap()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactNullableString(pub Option<String>);

impl Serialize for CompactNullableString {
    fn serialize(&self) -> Bytes {
        let len = match &self.0 {
            Some(s) => s.len() as u64 + 1,
            None => 0,
        };
        let mut b = BytesMut::new();
        b.put_slice(&len.encode_var_vec());
        if let Some(s) = &self.0 {
            b.put(s.as_bytes());
        }
        b.freeze()
    }
}

impl Deserialize<Self> for CompactNullableString {
    fn deserialize(src: &mut Bytes) -> Self {
        let (len, read) = u32::decode_var(src).expect("Failed to decode length");
        src.advance(read);
        if len == 0 || src.remaining() < (len as usize - 1) {
            return Self(None);
        }
        let bytes = src.split_to(len as usize - 1);
        Self(Some(String::from_utf8(bytes.to_vec()).unwrap()))
    }
}

#[derive(Debug, Clone)]
pub struct CompactArray<T>(pub Vec<T>);

impl<T: Serialize> Serialize for CompactArray<T> {
    fn serialize(&self) -> Bytes {
        let len = self.0.len() as u64 + 1;
        let mut buf = BytesMut::new();
        buf.put_slice(&len.encode_var_vec());
        for item in &self.0 {
            buf.put(item.serialize());
        }
        buf.freeze()
    }
}

impl<T, U> Deserialize<Vec<U>> for CompactArray<T>
where
    T: Deserialize<U>,
{
    fn deserialize(src: &mut Bytes) -> Vec<U> {
        let (len, read) = u64::decode_var(src).expect("Failed to decode length");
        src.advance(read);
        let items_count = if len > 1 { len as usize - 1 } else { 0 };
        let mut items = Vec::with_capacity(items_count);
        for _ in 0..items_count {
            items.push(T::deserialize(src));
        }
        items
    }
}

pub struct CompactNullableBytes(pub Option<Bytes>);

impl Serialize for CompactNullableBytes {
    fn serialize(&self) -> Bytes {
        let len = match &self.0 {
            Some(b) => b.len() as u64 + 1,
            None => 0,
        };
        let mut b = BytesMut::new();
        b.put_slice(&len.encode_var_vec());
        if let Some(bytes) = &self.0 {
            b.put(bytes.clone());
        }
        b.freeze()
    }
}

impl Deserialize<Self> for CompactNullableBytes {
    fn deserialize(src: &mut Bytes) -> Self {
        let (len, read) = u32::decode_var(src).expect("Failed to decode length");
        src.advance(read);
        if len == 0 {
            return Self(None);
        }
        let bytes = src.split_to(len as usize - 1);
        Self(Some(bytes))
    }
}

pub struct NullableBytes<T>(T);

impl<T, U> Deserialize<Vec<U>> for NullableBytes<T>
where
    T: Deserialize<U>,
{
    fn deserialize(src: &mut Bytes) -> Vec<U> {
        let len = src.get_i32();
        let items_count = if len == -1 { 0 } else { len as usize };
        let mut items = Vec::with_capacity(items_count);
        for _ in 0..items_count {
            items.push(T::deserialize(src));
        }
        items
    }
}

pub struct TagBuffer;

impl TagBuffer {
    pub fn serialize() -> Bytes {
        Bytes::from_static(&[0])
    }
}

impl Deserialize<u8> for TagBuffer {
    fn deserialize(src: &mut Bytes) -> u8 {
        src.get_u8()
    }
}

impl Deserialize<Self> for u32 {
    fn deserialize(src: &mut Bytes) -> Self {
        src.get_u32()
    }
}

impl Deserialize<Self> for i64 {
    fn deserialize(src: &mut Bytes) -> Self {
        let (val, read) = i64::decode_var(src).expect("Failed to decode var i64");
        src.advance(read);
        val
    }
}
