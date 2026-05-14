use std::{
    collections::HashMap,
    env, fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use kafka_rs::*;

const PORT: &str = "9092";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let properties = if let Some(path) = args.get(1) {
        Some(parse_properties_file(path)?)
    } else {
        None
    };

    let port = properties
        .as_ref()
        .and_then(|props| props.get("port"))
        .map(|v| v.as_str())
        .unwrap_or(PORT)
        .parse::<u16>()
        .map_err(|e| anyhow!("Invalid port number: {e}"))?;

    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let listener = TcpListener::bind(&bind_addr).await?;
    println!("Started Kafka server on {bind_addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            println!("accepted new connection");
            if let Err(e) = handle_conn(stream).await {
                eprintln!("error: {e}");
            }
        });
    }
}

fn parse_properties_file(path: &str) -> Result<HashMap<String, String>> {
    let content = fs::read_to_string(path)?;
    let mut properties = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            properties.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    Ok(properties)
}

async fn handle_conn(mut stream: TcpStream) -> Result<()> {
    loop {
        let mut message = get_message(&mut stream).await?;
        let resp = process_message(&mut message)?;
        let resp_msg = create_response_message(resp.as_bytes());
        stream.write_all(&resp_msg).await?;
    }
}

async fn get_message(stream: &mut TcpStream) -> Result<Bytes> {
    let mut len_buf = [0; 4];
    stream.read_exact(&mut len_buf).await?;

    let msg_len = i32::from_be_bytes(len_buf) as usize;
    let mut msg_buf = vec![0; msg_len];
    stream.read_exact(&mut msg_buf).await?;

    Ok(Bytes::from(msg_buf))
}

fn process_message(message: &mut Bytes) -> Result<Box<dyn Response + Send>> {
    let header = HeaderV2::deserialize(message);
    let request_api_key = match ApiKey::try_from(header.api_key) {
        Ok(key) => key,
        Err(_) => {
            return Err(anyhow!("Invalid request api key, {:?}", header.api_key));
        }
    };

    let response: Box<dyn Response + Send> = match request_api_key {
        ApiKey::ApiVersions => Box::new(ApiVersionsResponseV3::from_request(header, message)?),
        ApiKey::DescribeTopicPartitions => Box::new(
            DescribeTopicPartitionsResponseV0::from_request(header, message)?,
        ),
        ApiKey::Fetch => Box::new(FetchResponseV16::from_request(header, message)?),
        ApiKey::Produce => Box::new(ProduceResponseV11::from_request(header, message)?),
    };
    Ok(response)
}

fn create_response_message(src: Bytes) -> Bytes {
    let mut bytes = BytesMut::with_capacity(src.len() + 4);
    let msg_size = src.len() as i32;
    bytes.put_i32(msg_size);
    bytes.put_slice(&src);
    bytes.freeze()
}
