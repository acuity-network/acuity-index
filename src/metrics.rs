use std::{fmt, sync::Arc, time::Duration};

use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch::Receiver,
};
use tracing::{error, info};

use crate::{
    errors::{IndexError, internal_error},
    protocol::Trees,
};

const METRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
const HISTOGRAM_BUCKETS: [f64; 13] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    rpc_connected: Gauge,
    reconnects_total: Counter,
    indexed_blocks_total: Counter,
    indexed_events_total: Counter,
    indexed_keys_total: Counter,
    current_span_start: Gauge,
    current_span_end: Gauge,
    latest_seen_head: Gauge,
    ws_connections: Gauge,
    status_subscriptions: Gauge,
    event_subscriptions: Gauge,
    db_size_bytes: Gauge,
    block_fetch_seconds: Histogram,
    block_process_seconds: Histogram,
    block_commit_seconds: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let rpc_connected = Gauge::default();
        registry.register(
            "acuity_index_rpc_connected",
            "Whether the upstream RPC connection is currently available.",
            rpc_connected.clone(),
        );

        let reconnects_total = Counter::default();
        registry.register(
            "acuity_index_reconnects",
            "Total number of reconnect attempts after recoverable failures.",
            reconnects_total.clone(),
        );

        let indexed_blocks_total = Counter::default();
        registry.register(
            "acuity_index_indexed_blocks",
            "Total blocks successfully committed to the index.",
            indexed_blocks_total.clone(),
        );

        let indexed_events_total = Counter::default();
        registry.register(
            "acuity_index_indexed_events",
            "Total decoded events seen in successfully committed blocks.",
            indexed_events_total.clone(),
        );

        let indexed_keys_total = Counter::default();
        registry.register(
            "acuity_index_indexed_keys",
            "Total derived keys written for successfully committed blocks.",
            indexed_keys_total.clone(),
        );

        let current_span_start = Gauge::default();
        registry.register(
            "acuity_index_current_span_start",
            "Start block number of the active indexed span.",
            current_span_start.clone(),
        );

        let current_span_end = Gauge::default();
        registry.register(
            "acuity_index_current_span_end",
            "End block number of the active indexed span.",
            current_span_end.clone(),
        );

        let latest_seen_head = Gauge::default();
        registry.register(
            "acuity_index_latest_seen_head",
            "Highest chain head observed by the live head subscription.",
            latest_seen_head.clone(),
        );

        let ws_connections = Gauge::default();
        registry.register(
            "acuity_index_ws_connections",
            "Current number of active WebSocket connections.",
            ws_connections.clone(),
        );

        let status_subscriptions = Gauge::default();
        registry.register(
            "acuity_index_status_subscriptions",
            "Current number of active status subscriptions.",
            status_subscriptions.clone(),
        );

        let event_subscriptions = Gauge::default();
        registry.register(
            "acuity_index_event_subscriptions",
            "Current number of active event subscriptions.",
            event_subscriptions.clone(),
        );

        let db_size_bytes = Gauge::default();
        registry.register(
            "acuity_index_db_size_bytes",
            "Current on-disk size of the sled database in bytes.",
            db_size_bytes.clone(),
        );

        let block_fetch_seconds = Histogram::new(HISTOGRAM_BUCKETS);
        registry.register(
            "acuity_index_block_fetch_seconds",
            "Time spent fetching a block and its events from RPC.",
            block_fetch_seconds.clone(),
        );

        let block_process_seconds = Histogram::new(HISTOGRAM_BUCKETS);
        registry.register(
            "acuity_index_block_process_seconds",
            "Time spent processing a fetched block on CPU.",
            block_process_seconds.clone(),
        );

        let block_commit_seconds = Histogram::new(HISTOGRAM_BUCKETS);
        registry.register(
            "acuity_index_block_commit_seconds",
            "Time spent committing a processed block to storage.",
            block_commit_seconds.clone(),
        );

        Self {
            registry: Arc::new(registry),
            rpc_connected,
            reconnects_total,
            indexed_blocks_total,
            indexed_events_total,
            indexed_keys_total,
            current_span_start,
            current_span_end,
            latest_seen_head,
            ws_connections,
            status_subscriptions,
            event_subscriptions,
            db_size_bytes,
            block_fetch_seconds,
            block_process_seconds,
            block_commit_seconds,
        }
    }

    pub fn encode(&self) -> Result<String, fmt::Error> {
        let mut encoded = String::new();
        encode(&mut encoded, self.registry.as_ref())?;
        Ok(encoded)
    }

    pub fn set_rpc_connected(&self, connected: bool) {
        self.rpc_connected.set(i64::from(connected));
    }

    pub fn inc_reconnects(&self) {
        self.reconnects_total.inc();
    }

    pub fn add_indexed_block(&self, event_count: u32, key_count: u32) {
        self.indexed_blocks_total.inc();
        self.indexed_events_total.inc_by(u64::from(event_count));
        self.indexed_keys_total.inc_by(u64::from(key_count));
    }

    pub fn set_current_span(&self, start: u32, end: u32) {
        self.current_span_start.set(i64::from(start));
        self.current_span_end.set(i64::from(end));
    }

    pub fn set_latest_seen_head(&self, block_number: u32) {
        self.latest_seen_head.set(i64::from(block_number));
    }

    pub fn inc_ws_connections(&self) {
        self.ws_connections.inc();
    }

    pub fn dec_ws_connections(&self) {
        self.ws_connections.dec();
    }

    pub fn set_status_subscriptions(&self, count: usize) {
        self.status_subscriptions.set(usize_to_i64(count));
    }

    pub fn set_event_subscriptions(&self, count: usize) {
        self.event_subscriptions.set(usize_to_i64(count));
    }

    pub fn set_db_size_bytes(&self, bytes: u64) {
        self.db_size_bytes.set(u64_to_i64(bytes));
    }

    pub fn observe_block_fetch(&self, duration: Duration) {
        self.block_fetch_seconds.observe(duration.as_secs_f64());
    }

    pub fn observe_block_process(&self, duration: Duration) {
        self.block_process_seconds.observe(duration.as_secs_f64());
    }

    pub fn observe_block_commit(&self, duration: Duration) {
        self.block_commit_seconds.observe(duration.as_secs_f64());
    }
}

fn usize_to_i64(value: usize) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

fn u64_to_i64(value: u64) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

async fn write_http_response(
    stream: &mut TcpStream,
    status_line: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(), IndexError> {
    let response = format!(
        "HTTP/1.1 {status_line}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn handle_metrics_connection(
    mut stream: TcpStream,
    trees: Trees,
    metrics: Arc<Metrics>,
) -> Result<(), IndexError> {
    let mut buffer = [0u8; 4096];
    let bytes_read = stream.read(&mut buffer).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let request = match std::str::from_utf8(&buffer[..bytes_read]) {
        Ok(request) => request,
        Err(_) => {
            write_http_response(
                &mut stream,
                "400 Bad Request",
                "text/plain; charset=utf-8",
                b"invalid request\n",
            )
            .await?;
            return Ok(());
        }
    };

    let Some(request_line) = request.lines().next() else {
        write_http_response(
            &mut stream,
            "400 Bad Request",
            "text/plain; charset=utf-8",
            b"invalid request\n",
        )
        .await?;
        return Ok(());
    };

    let mut parts = request_line.split_whitespace();
    let method = parts.next();
    let path = parts.next();

    match (method, path) {
        (Some("GET"), Some("/metrics")) => {
            metrics.set_db_size_bytes(trees.root.size_on_disk()?);
            let body = metrics
                .encode()
                .map_err(|err| internal_error(format!("failed to encode metrics: {err}")))?;
            write_http_response(&mut stream, "200 OK", METRICS_CONTENT_TYPE, body.as_bytes())
                .await?;
        }
        (Some("GET"), _) => {
            write_http_response(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
            )
            .await?;
        }
        (Some(_), _) => {
            write_http_response(
                &mut stream,
                "405 Method Not Allowed",
                "text/plain; charset=utf-8",
                b"method not allowed\n",
            )
            .await?;
        }
        _ => {
            write_http_response(
                &mut stream,
                "400 Bad Request",
                "text/plain; charset=utf-8",
                b"invalid request\n",
            )
            .await?;
        }
    }

    Ok(())
}

pub async fn metrics_listen(
    listener: TcpListener,
    trees: Trees,
    metrics: Arc<Metrics>,
    mut exit_rx: Receiver<bool>,
) -> Result<(), IndexError> {
    info!("Metrics listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            _ = exit_rx.changed() => {
                return Ok(());
            }
            incoming = listener.accept() => {
                let (stream, addr) = incoming?;
                let trees = trees.clone();
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_metrics_connection(stream, trees, metrics).await {
                        error!("Metrics request from {addr} failed: {err}");
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sled::Config;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::watch,
    };

    fn open_test_trees() -> (tempfile::TempDir, Trees) {
        let dir = tempfile::tempdir().unwrap();
        let trees = Trees::open(Config::new().path(dir.path())).unwrap();
        (dir, trees)
    }

    #[test]
    fn metrics_encode_openmetrics_text() {
        let metrics = Metrics::new();
        metrics.set_rpc_connected(true);
        metrics.inc_reconnects();
        metrics.add_indexed_block(12, 34);
        metrics.set_current_span(10, 20);
        metrics.set_latest_seen_head(25);
        metrics.set_status_subscriptions(2);
        metrics.set_event_subscriptions(3);
        metrics.set_db_size_bytes(4096);
        metrics.observe_block_fetch(Duration::from_millis(10));

        let encoded = metrics.encode().unwrap();

        assert!(encoded.contains("# HELP acuity_index_rpc_connected"));
        assert!(encoded.contains("acuity_index_rpc_connected 1"));
        assert!(encoded.contains("acuity_index_reconnects_total 1"));
        assert!(encoded.contains("acuity_index_indexed_blocks_total 1"));
        assert!(encoded.contains("acuity_index_db_size_bytes 4096"));
        assert!(encoded.contains("acuity_index_block_fetch_seconds_bucket"));
        assert!(encoded.ends_with("# EOF\n"));
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_openmetrics_response() {
        let (_dir, trees) = open_test_trees();
        trees.root.insert("genesis_hash", b"test").unwrap();
        let metrics = Arc::new(Metrics::new());
        metrics.set_rpc_connected(true);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (exit_tx, exit_rx) = watch::channel(false);
        let server = tokio::spawn(metrics_listen(
            listener,
            trees.clone(),
            metrics.clone(),
            exit_rx,
        ));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await.unwrap();
        let response = String::from_utf8(body).unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains(METRICS_CONTENT_TYPE));
        assert!(response.contains("acuity_index_rpc_connected 1"));
        assert!(response.contains("# EOF\n"));

        let _ = exit_tx.send(true);
        let result = server.await.unwrap();
        assert!(result.is_ok());
    }
}
