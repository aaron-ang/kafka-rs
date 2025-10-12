use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::*;

#[derive(Debug)]
pub struct ApiVersionsRequestV3;

#[derive(Debug)]
pub struct ApiVersionsResponseV3 {
    header: HeaderV0,
    error_code: ErrorCode,
    api_keys: CompactArray<ApiVersionsApiKey>,
    throttle_time_ms: i32,
}

impl ApiVersionsResponseV3 {
    pub fn new(req_header: HeaderV2) -> Self {
        let header = HeaderV0::new(req_header.correlation_id);

        let mut error_code = ErrorCode::None;
        if !matches!(req_header.api_version, 0..=4) {
            error_code = ErrorCode::UnsupportedVersion
        }

        Self {
            header,
            error_code,
            api_keys: CompactArray(get_api_keys()),
            throttle_time_ms: 0,
        }
    }
}

impl Response for ApiVersionsResponseV3 {
    fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.header.serialize());
        bytes.put_i16(self.error_code.into());
        bytes.put(self.api_keys.serialize());
        bytes.put_i32(self.throttle_time_ms);
        bytes.put(TagBuffer::serialize());
        bytes.freeze()
    }
}

impl ApiHandler for ApiVersionsResponseV3 {
    type Request = ApiVersionsRequestV3;

    fn decode_request(_header: &HeaderV2, _message: &mut Bytes) -> Result<Self::Request> {
        Ok(ApiVersionsRequestV3)
    }

    fn create_response(header: HeaderV2, _request: Self::Request) -> Result<Self> {
        Ok(Self::new(header))
    }
}

#[derive(Debug)]
struct ApiVersionsApiKey {
    key: ApiKey,
    min_version: i16,
    max_version: i16,
}

impl Serialize for ApiVersionsApiKey {
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i16(self.key.into());
        b.put_i16(self.min_version);
        b.put_i16(self.max_version);
        b.put(TagBuffer::serialize());
        b.freeze()
    }
}

fn get_api_keys() -> Vec<ApiVersionsApiKey> {
    vec![
        ApiVersionsApiKey {
            key: ApiKey::Produce,
            min_version: 0,
            max_version: 11,
        },
        ApiVersionsApiKey {
            key: ApiKey::Fetch,
            min_version: 0,
            max_version: 16,
        },
        ApiVersionsApiKey {
            key: ApiKey::ApiVersions,
            min_version: 0,
            max_version: 4,
        },
        ApiVersionsApiKey {
            key: ApiKey::DescribeTopicPartitions,
            min_version: 0,
            max_version: 0,
        },
    ]
}
