use acuity_index::synthetic_devnet::{
    SeedManifest, fetch_genesis_hash, fetch_status, pick_unused_port, render_synthetic_index_spec,
    wait_for_node,
};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::{
    env,
    error::Error,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::OnceLock,
    time::{Duration, Instant},
};
use tempfile::{TempDir, tempdir};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn runtime_manifest() -> PathBuf {
    repo_root().join("runtime/Cargo.toml")
}

pub fn runtime_wasm() -> PathBuf {
    repo_root().join("runtime/target/release/wbuild/synthetic-runtime/synthetic_runtime.wasm")
}

fn build_runtime_release_inner() -> Result<(), Box<dyn Error>> {
    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("--manifest-path")
        .arg(runtime_manifest())
        .status()?;
    if !status.success() {
        return Err("runtime release build failed".into());
    }
    Ok(())
}

pub fn build_runtime_release() -> Result<(), Box<dyn Error>> {
    static BUILD_RESULT: OnceLock<Result<(), String>> = OnceLock::new();

    match BUILD_RESULT.get_or_init(|| build_runtime_release_inner().map_err(|err| err.to_string()))
    {
        Ok(()) => Ok(()),
        Err(err) => Err(io::Error::other(err.clone()).into()),
    }
}

pub fn build_chain_spec(output_path: &Path) -> Result<(), Box<dyn Error>> {
    let status = Command::new("polkadot-omni-node")
        .arg("chain-spec-builder")
        .arg("--chain-spec-path")
        .arg(output_path)
        .arg("create")
        .arg("-t")
        .arg("development")
        .arg("--para-id")
        .arg("1000")
        .arg("--relay-chain")
        .arg("rococo-local")
        .arg("--runtime")
        .arg(runtime_wasm())
        .arg("named-preset")
        .arg("development")
        .status()?;
    if !status.success() {
        return Err("chain spec generation failed".into());
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct ConfigOverrides {
    pub store_events: Option<bool>,
    pub index_variant: Option<bool>,
}

pub fn write_config_with_overrides(
    path: &Path,
    node_url: &str,
    genesis_hash: &str,
    overrides: &ConfigOverrides,
) -> Result<(), Box<dyn Error>> {
    let mut value: toml::Value =
        toml::from_str(&render_synthetic_index_spec(node_url, genesis_hash)?)?;
    let table = value
        .as_table_mut()
        .ok_or_else(|| io::Error::other("synthetic config must be a TOML table"))?;
    if let Some(store_events) = overrides.store_events {
        table.insert("store_events".into(), toml::Value::Boolean(store_events));
    }
    if let Some(index_variant) = overrides.index_variant {
        table.insert("index_variant".into(), toml::Value::Boolean(index_variant));
    }
    fs::write(path, toml::to_string(&value)?)?;
    Ok(())
}

pub fn rewrite_config_table(
    path: &Path,
    mutator: impl FnOnce(&mut toml::map::Map<String, toml::Value>),
) -> Result<(), Box<dyn Error>> {
    let mut value: toml::Value = toml::from_str(&fs::read_to_string(path)?)?;
    let table = value
        .as_table_mut()
        .ok_or_else(|| io::Error::other("synthetic config must be a TOML table"))?;
    mutator(table);
    fs::write(path, toml::to_string(&value)?)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct IndexerOptions {
    pub queue_depth: u8,
    pub max_events_limit: usize,
    pub max_total_subscriptions: Option<usize>,
    pub max_subscriptions_per_connection: Option<usize>,
    pub subscription_buffer_size: Option<usize>,
    pub subscription_control_buffer_size: Option<usize>,
    pub idle_timeout_secs: Option<u64>,
}

impl Default for IndexerOptions {
    fn default() -> Self {
        Self {
            queue_depth: 1,
            max_events_limit: 4096,
            max_total_subscriptions: None,
            max_subscriptions_per_connection: None,
            subscription_buffer_size: None,
            subscription_control_buffer_size: None,
            idle_timeout_secs: None,
        }
    }
}

pub fn start_node(
    chain_spec_path: &Path,
    rpc_port: u16,
    base_path: &Path,
    log_path: &Path,
) -> Result<ManagedChild, Box<dyn Error>> {
    let log_file = File::create(log_path)?;
    let log_file_err = log_file.try_clone()?;
    let child = Command::new("polkadot-omni-node")
        .arg("--chain")
        .arg(chain_spec_path)
        .arg("--dev")
        .arg("--instant-seal")
        .arg("--pool-type")
        .arg("single-state")
        .arg("--state-pruning")
        .arg("archive-canonical")
        .arg("--blocks-pruning")
        .arg("archive-canonical")
        .arg("--rpc-port")
        .arg(rpc_port.to_string())
        .arg("--prometheus-port")
        .arg("0")
        .arg("--no-prometheus")
        .arg("--port")
        .arg("0")
        .arg("--base-path")
        .arg(base_path)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .spawn()?;

    Ok(ManagedChild { child })
}

pub fn read_text(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(path)?)
}

pub fn start_indexer(
    config_path: &Path,
    node_url: &str,
    db_path: &Path,
    port: u16,
    log_path: &Path,
    options: &IndexerOptions,
) -> Result<ManagedChild, Box<dyn Error>> {
    let log_file = File::create(log_path)?;
    let log_file_err = log_file.try_clone()?;
    let bin = env::var("CARGO_BIN_EXE_acuity-index")?;
    let mut command = Command::new(bin);
    command
        .arg("--index-spec")
        .arg(config_path)
        .arg("--url")
        .arg(node_url)
        .arg("--db-path")
        .arg(db_path)
        .arg("--queue-depth")
        .arg(options.queue_depth.to_string())
        .arg("--port")
        .arg(port.to_string())
        .arg("--max-events-limit")
        .arg(options.max_events_limit.to_string());

    if let Some(max_total_subscriptions) = options.max_total_subscriptions {
        command
            .arg("--max-total-subscriptions")
            .arg(max_total_subscriptions.to_string());
    }
    if let Some(max_subscriptions_per_connection) = options.max_subscriptions_per_connection {
        command
            .arg("--max-subscriptions-per-connection")
            .arg(max_subscriptions_per_connection.to_string());
    }
    if let Some(subscription_buffer_size) = options.subscription_buffer_size {
        command
            .arg("--subscription-buffer-size")
            .arg(subscription_buffer_size.to_string());
    }
    if let Some(subscription_control_buffer_size) = options.subscription_control_buffer_size {
        command
            .arg("--subscription-control-buffer-size")
            .arg(subscription_control_buffer_size.to_string());
    }
    if let Some(idle_timeout_secs) = options.idle_timeout_secs {
        command
            .arg("--idle-timeout-secs")
            .arg(idle_timeout_secs.to_string());
    }

    let child = command
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .spawn()?;

    Ok(ManagedChild { child })
}

pub fn run_smoke_seeder(
    node_url: &str,
    output_path: &Path,
) -> Result<SeedManifest, Box<dyn Error>> {
    let bin = env::var("CARGO_BIN_EXE_seed_synthetic_runtime")?;
    let status = Command::new(bin)
        .arg("--url")
        .arg(node_url)
        .arg("--mode")
        .arg("smoke")
        .arg("--output")
        .arg(output_path)
        .status()?;
    if !status.success() {
        return Err("synthetic smoke seeder failed".into());
    }
    Ok(serde_json::from_slice(&std::fs::read(output_path)?)?)
}

pub fn run_bulk_seeder(
    node_url: &str,
    output_path: &Path,
    batch_start: u32,
    batches: u32,
    burst_count: u32,
) -> Result<SeedManifest, Box<dyn Error>> {
    let bin = env::var("CARGO_BIN_EXE_seed_synthetic_runtime")?;
    let status = Command::new(bin)
        .arg("--url")
        .arg(node_url)
        .arg("--mode")
        .arg("bulk")
        .arg("--batch-start")
        .arg(batch_start.to_string())
        .arg("--batches")
        .arg(batches.to_string())
        .arg("--burst-count")
        .arg(burst_count.to_string())
        .arg("--output")
        .arg(output_path)
        .status()?;
    if !status.success() {
        return Err("synthetic bulk seeder failed".into());
    }
    Ok(serde_json::from_slice(&std::fs::read(output_path)?)?)
}

pub async fn wait_for_indexer(indexer_url: &str, timeout: Duration) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match fetch_status(indexer_url).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(200)).await,
            Err(err) => return Err(err),
        }
    }
}

pub struct SyntheticStack {
    _temp: TempDir,
    chain_spec: PathBuf,
    node_base: PathBuf,
    config_path: PathBuf,
    index_db: PathBuf,
    node_log: PathBuf,
    indexer_log: PathBuf,
    rpc_port: u16,
    indexer_port: u16,
    indexer_options: IndexerOptions,
    pub node_url: String,
    pub indexer_url: String,
    pub node: ManagedChild,
    pub indexer: ManagedChild,
}

impl SyntheticStack {
    pub async fn start(
        config_overrides: ConfigOverrides,
        indexer_options: IndexerOptions,
    ) -> Result<Self, Box<dyn Error>> {
        build_runtime_release()?;

        let temp = tempdir()?;
        let chain_spec = temp.path().join("synthetic-dev-chain-spec.json");
        let node_base = temp.path().join("node");
        let config_path = temp.path().join("synthetic.toml");
        let index_db = temp.path().join("db");
        let node_log = temp.path().join("node.log");
        let indexer_log = temp.path().join("indexer.log");
        let rpc_port = pick_unused_port()?;
        let indexer_port = pick_unused_port()?;

        build_chain_spec(&chain_spec)?;

        let node = start_node(&chain_spec, rpc_port, &node_base, &node_log)?;
        let node_url = format!("ws://127.0.0.1:{rpc_port}");
        wait_for_node(&node_url, 0, Duration::from_secs(30)).await?;

        let genesis_hash = fetch_genesis_hash(&node_url).await?;
        write_config_with_overrides(&config_path, &node_url, &genesis_hash, &config_overrides)?;

        let indexer = start_indexer(
            &config_path,
            &node_url,
            &index_db,
            indexer_port,
            &indexer_log,
            &indexer_options,
        )?;
        let indexer_url = format!("ws://127.0.0.1:{indexer_port}");
        wait_for_indexer(&indexer_url, Duration::from_secs(30)).await?;

        Ok(Self {
            _temp: temp,
            chain_spec,
            node_base,
            config_path,
            index_db,
            node_log,
            indexer_log,
            rpc_port,
            indexer_port,
            indexer_options,
            node_url,
            indexer_url,
            node,
            indexer,
        })
    }

    pub async fn restart_indexer(&mut self) -> Result<(), Box<dyn Error>> {
        self.indexer.terminate();
        self.indexer = start_indexer(
            &self.config_path,
            &self.node_url,
            &self.index_db,
            self.indexer_port,
            &self.indexer_log,
            &self.indexer_options,
        )?;
        wait_for_indexer(&self.indexer_url, Duration::from_secs(30)).await
    }

    pub fn rewrite_config(
        &self,
        mutator: impl FnOnce(&mut toml::map::Map<String, toml::Value>),
    ) -> Result<(), Box<dyn Error>> {
        rewrite_config_table(&self.config_path, mutator)
    }

    pub fn stop_node(&mut self) {
        self.node.terminate();
    }

    pub async fn restart_node(&mut self) -> Result<(), Box<dyn Error>> {
        self.node = start_node(
            &self.chain_spec,
            self.rpc_port,
            &self.node_base,
            &self.node_log,
        )?;
        wait_for_node(&self.node_url, 0, Duration::from_secs(30)).await?;
        Ok(())
    }
}

pub struct ManagedChild {
    child: Child,
}

impl ManagedChild {
    pub async fn wait_for_exit(
        &mut self,
        timeout_duration: Duration,
    ) -> Result<ExitStatus, Box<dyn Error>> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            match self.child.try_wait()? {
                Some(status) => return Ok(status),
                None if Instant::now() < deadline => sleep(Duration::from_millis(100)).await,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for child process to exit",
                    )
                    .into());
                }
            }
        }
    }

    pub fn terminate(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }

        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(self.child.id().to_string())
                .status();

            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                match self.child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                    Err(_) => break,
                }
            }
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.terminate();
    }
}

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub struct WsClient {
    socket: WsStream,
}

impl WsClient {
    pub async fn connect(url: &str) -> Result<Self, Box<dyn Error>> {
        let (socket, _) = connect_async(url).await?;
        Ok(Self { socket })
    }

    pub async fn send_json(&mut self, request: Value) -> Result<(), Box<dyn Error>> {
        self.socket
            .send(Message::Text(request.to_string().into()))
            .await?;
        Ok(())
    }

    pub async fn recv_json(&mut self) -> Result<Value, Box<dyn Error>> {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Text(text))) => return Ok(serde_json::from_str(text.as_ref())?),
                Some(Ok(Message::Binary(bytes))) => return Ok(serde_json::from_slice(&bytes)?),
                Some(Ok(Message::Ping(payload))) => {
                    self.socket.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Close(frame))) => {
                    return Err(io::Error::other(format!("websocket closed: {frame:?}")).into());
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(err.into()),
                None => {
                    return Err(
                        io::Error::other("websocket ended before a message was received").into(),
                    );
                }
            }
        }
    }

    pub async fn request(&mut self, request: Value) -> Result<Value, Box<dyn Error>> {
        self.send_json(request).await?;
        self.recv_json().await
    }

    pub async fn recv_json_timeout(&mut self, duration: Duration) -> Result<Value, Box<dyn Error>> {
        match timeout(duration, self.recv_json()).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for websocket message",
            )
            .into()),
        }
    }

    pub async fn wait_for_message_where<F>(
        &mut self,
        timeout_duration: Duration,
        mut predicate: F,
    ) -> Result<Value, Box<dyn Error>>
    where
        F: FnMut(&Value) -> bool,
    {
        let deadline = Instant::now() + timeout_duration;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for matching websocket message",
                )
                .into());
            }

            let message = self
                .recv_json_timeout(deadline.saturating_duration_since(now))
                .await?;
            if predicate(&message) {
                return Ok(message);
            }
        }
    }

    pub async fn expect_no_message(&mut self, duration: Duration) -> Result<(), Box<dyn Error>> {
        match timeout(duration, self.recv_json()).await {
            Ok(Ok(message)) => {
                Err(io::Error::other(format!("unexpected websocket message: {message}")).into())
            }
            Ok(Err(err)) => Err(err),
            Err(_) => Ok(()),
        }
    }

    pub async fn close(mut self) -> Result<(), Box<dyn Error>> {
        self.socket.close(None).await?;
        Ok(())
    }
}
