use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    error::Error,
    fs, io,
    net::TcpListener,
    path::{Path, PathBuf},
    process,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use subxt::{OnlineClient, PolkadotConfig};
use tokio::time::sleep;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

pub const SYNTHETIC_TEMPLATE: &str = include_str!("../chains/synthetic.toml");

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryExpectation {
    pub description: String,
    pub key: Value,
    pub min_events: usize,
    pub expected_event_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeedManifest {
    pub mode: String,
    pub genesis_hash: String,
    pub start_block: u32,
    pub end_block: u32,
    pub total_blocks: u32,
    pub transactions_submitted: u32,
    pub synthetic_event_count: u32,
    pub queries: Vec<QueryExpectation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkReport {
    pub chain_tip: u32,
    pub indexed_blocks: u32,
    pub queue_depth: u8,
    pub synthetic_event_count: u32,
    pub elapsed_seconds: f64,
    pub blocks_per_second: f64,
    pub synthetic_events_per_second: f64,
    pub size_on_disk_bytes: u64,
}

pub fn key_u32(name: &str, value: u32) -> Value {
    json!({
        "type": "Custom",
        "value": {
            "name": name,
            "kind": "u32",
            "value": value,
        }
    })
}

pub fn key_bytes32(name: &str, value: [u8; 32]) -> Value {
    json!({
        "type": "Custom",
        "value": {
            "name": name,
            "kind": "bytes32",
            "value": bytes32_hex(value),
        }
    })
}

pub fn key_account(account_id: [u8; 32]) -> Value {
    key_bytes32("account_id", account_id)
}

pub fn bytes32_hex(value: [u8; 32]) -> String {
    format!("0x{}", hex::encode(value))
}

pub fn synthetic_digest(batch_id: u32, seq: u32) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&batch_id.to_be_bytes());
    bytes[4..8].copy_from_slice(&seq.to_be_bytes());
    for (idx, byte) in bytes[8..].iter_mut().enumerate() {
        *byte = batch_id
            .wrapping_mul(31)
            .wrapping_add(seq.wrapping_mul(17))
            .wrapping_add(idx as u32) as u8;
    }
    bytes
}

pub async fn fetch_genesis_hash(url: &str) -> Result<String, Box<dyn Error>> {
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(url).await?;
    Ok(hex::encode(api.genesis_hash().as_ref()))
}

pub async fn current_block_number(url: &str) -> Result<u32, Box<dyn Error>> {
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(url).await?;
    let at_block = api.at_current_block().await?;
    Ok(at_block
        .block_number()
        .try_into()
        .map_err(|_| io::Error::other("block number exceeds u32"))?)
}

pub async fn wait_for_node(
    url: &str,
    min_block: u32,
    timeout: Duration,
) -> Result<u32, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match current_block_number(url).await {
            Ok(block) if block >= min_block => return Ok(block),
            Ok(_) | Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(200)).await,
            Ok(block) => {
                return Err(io::Error::other(format!(
                    "node did not reach block {min_block}; last block {block}"
                ))
                .into());
            }
            Err(err) => return Err(err),
        }
    }
}

pub fn render_synthetic_index_spec(
    url: &str,
    genesis_hash: &str,
) -> Result<String, Box<dyn Error>> {
    let mut value: toml::Value = toml::from_str(SYNTHETIC_TEMPLATE)?;
    value["genesis_hash"] = toml::Value::String(genesis_hash.to_owned());
    value["default_url"] = toml::Value::String(url.to_owned());
    Ok(toml::to_string(&value)?)
}

pub fn write_synthetic_index_spec(
    path: &Path,
    url: &str,
    genesis_hash: &str,
) -> Result<(), Box<dyn Error>> {
    let rendered = render_synthetic_index_spec(url, genesis_hash)?;
    fs::write(path, rendered)?;
    Ok(())
}

pub fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", process::id()))
}

pub fn pick_unused_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn recv_json_ws(socket: &mut WsStream) -> Result<Value, Box<dyn Error>> {
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => return Ok(serde_json::from_str(text.as_ref())?),
            Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(frame) => {
                return Err(io::Error::other(format!("websocket closed: {frame:?}")).into());
            }
            _ => continue,
        }
    }

    Err(io::Error::other("websocket ended before a response was received").into())
}

pub struct JsonWsClient {
    socket: WsStream,
    next_request_id: u64,
}

impl JsonWsClient {
    pub async fn connect(url: &str) -> Result<Self, Box<dyn Error>> {
        let (socket, _) = connect_async(url).await?;
        Ok(Self {
            socket,
            next_request_id: 1,
        })
    }

    async fn send_json(&mut self, request: Value) -> Result<(), Box<dyn Error>> {
        self.socket
            .send(Message::Text(request.to_string().into()))
            .await?;
        Ok(())
    }

    pub async fn request(
        &mut self,
        request_type: &str,
        body: Value,
    ) -> Result<Value, Box<dyn Error>> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;

        let mut request = json!({
            "id": request_id,
            "type": request_type,
        });

        match body {
            Value::Null => {}
            Value::Object(extra) => {
                let request_obj = request
                    .as_object_mut()
                    .ok_or_else(|| io::Error::other("request payload was not an object"))?;
                request_obj.extend(extra);
            }
            _ => {
                return Err(
                    io::Error::other("websocket request body must be a JSON object").into(),
                );
            }
        }

        self.send_json(request).await?;
        recv_json_ws(&mut self.socket).await
    }

    pub async fn get_events(&mut self, key: Value, limit: u16) -> Result<Value, Box<dyn Error>> {
        self.request(
            "GetEvents",
            json!({
                "key": key,
                "limit": limit,
            }),
        )
        .await
    }

    pub async fn size_on_disk(&mut self) -> Result<u64, Box<dyn Error>> {
        let response = self.request("SizeOnDisk", Value::Null).await?;
        response["data"]
            .as_u64()
            .ok_or_else(|| io::Error::other(format!("unexpected SizeOnDisk response: {response}")))
            .map_err(Into::into)
    }
}

pub async fn request_json_ws(url: &str, request: Value) -> Result<Value, Box<dyn Error>> {
    let (mut socket, _) = connect_async(url).await?;
    socket
        .send(Message::Text(request.to_string().into()))
        .await?;
    recv_json_ws(&mut socket).await
}

pub async fn fetch_status(indexer_url: &str) -> Result<Value, Box<dyn Error>> {
    request_json_ws(indexer_url, json!({"id": 1, "type": "Status"})).await
}

pub fn spans_cover_tip(status_response: &Value, expected_tip: u32) -> bool {
    status_response["data"].as_array().is_some_and(|spans| {
        spans.iter().any(|span| {
            span["start"].as_u64().unwrap_or(u64::MAX) <= 1
                && span["end"].as_u64().unwrap_or(0) >= u64::from(expected_tip)
        })
    })
}

pub async fn wait_for_indexed_tip(
    indexer_url: &str,
    expected_tip: u32,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match fetch_status(indexer_url).await {
            Ok(status) if spans_cover_tip(&status, expected_tip) => return Ok(()),
            Ok(_) | Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(200)).await,
            Ok(status) => {
                return Err(io::Error::other(format!(
                    "indexer did not reach tip {expected_tip}: {status}"
                ))
                .into());
            }
            Err(err) => return Err(err),
        }
    }
}

pub async fn get_events(
    indexer_url: &str,
    key: Value,
    limit: u16,
) -> Result<Value, Box<dyn Error>> {
    request_json_ws(
        indexer_url,
        json!({
            "id": 2,
            "type": "GetEvents",
            "key": key,
            "limit": limit,
        }),
    )
    .await
}

pub fn events_len(response: &Value) -> usize {
    response["data"]["events"]
        .as_array()
        .map(Vec::len)
        .unwrap_or_default()
}

pub fn decoded_event_names(response: &Value) -> Vec<String> {
    response["data"]["decodedEvents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| event["event"]["eventName"].as_str().map(ToOwned::to_owned))
        .collect()
}

pub fn validate_query_expectation(
    query: &QueryExpectation,
    response: &Value,
) -> Result<(), String> {
    let count = events_len(response);
    if count < query.min_events {
        return Err(format!(
            "query '{}' returned {count} events, expected at least {}",
            query.description, query.min_events
        ));
    }

    let names = decoded_event_names(response);
    for expected in &query.expected_event_names {
        if !names.iter().any(|name| name == expected) {
            return Err(format!(
                "query '{}' missing decoded event '{}' in {:?}",
                query.description, expected, names
            ));
        }
    }

    Ok(())
}

pub async fn size_on_disk(indexer_url: &str) -> Result<u64, Box<dyn Error>> {
    let response = request_json_ws(indexer_url, json!({"id": 3, "type": "SizeOnDisk"})).await?;
    response["data"]
        .as_u64()
        .ok_or_else(|| io::Error::other(format!("unexpected SizeOnDisk response: {response}")))
        .map_err(Into::into)
}
