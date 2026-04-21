use acuity_index::synthetic_devnet::{
    BenchmarkReport, QueryExpectation, SeedManifest, get_events, pick_unused_port, size_on_disk,
    unique_temp_path, validate_query_expectation, write_synthetic_index_spec,
};
use clap::Parser;
use serde_json::to_string_pretty;
use std::{
    error::Error,
    fs::{self, File},
    io,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio::time::sleep;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:9944")]
    node_url: String,
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    indexer_bin: Option<PathBuf>,
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[arg(long)]
    workdir: Option<PathBuf>,
    #[arg(long, default_value_t = 1)]
    queue_depth: u8,
    #[arg(long)]
    indexer_port: Option<u16>,
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let manifest: SeedManifest = serde_json::from_slice(&fs::read(&args.manifest)?)?;
    let workdir = args
        .workdir
        .unwrap_or_else(|| unique_temp_path("synthetic-index-benchmark"));
    fs::create_dir_all(&workdir)?;

    let config_path = workdir.join("synthetic.toml");
    write_synthetic_index_spec(&config_path, &args.node_url, &manifest.genesis_hash)?;

    let db_path = args.db_path.unwrap_or_else(|| workdir.join("db"));
    fs::create_dir_all(&db_path)?;
    let indexer_port = args.indexer_port.unwrap_or(pick_unused_port()?);
    let indexer_url = format!("ws://127.0.0.1:{indexer_port}");
    let max_events_limit = manifest.synthetic_event_count.max(1024).to_string();

    let indexer_bin = match args.indexer_bin {
        Some(path) => path,
        None => sibling_binary("acuity-index")?,
    };

    let log_path = workdir.join("indexer.log");
    let log_file = File::create(&log_path)?;
    let log_file_err = log_file.try_clone()?;

    let mut child = ChildGuard {
        child: Command::new(indexer_bin)
            .arg("--index-config")
            .arg(&config_path)
            .arg("--url")
            .arg(&args.node_url)
            .arg("--db-path")
            .arg(&db_path)
            .arg("--queue-depth")
            .arg(args.queue_depth.to_string())
            .arg("--port")
            .arg(indexer_port.to_string())
            .arg("--max-events-limit")
            .arg(max_events_limit)
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_err))
            .spawn()?,
    };

    let started = Instant::now();
    wait_for_queries(
        &indexer_url,
        &manifest.queries,
        Duration::from_secs(args.timeout_secs),
    )
    .await?;

    let elapsed = started.elapsed().as_secs_f64();
    if elapsed == 0.0 {
        return Err(io::Error::other("benchmark finished too quickly to measure").into());
    }

    let report = BenchmarkReport {
        chain_tip: manifest.end_block,
        indexed_blocks: manifest.total_blocks,
        synthetic_event_count: manifest.synthetic_event_count,
        elapsed_seconds: elapsed,
        blocks_per_second: f64::from(manifest.total_blocks) / elapsed,
        synthetic_events_per_second: f64::from(manifest.synthetic_event_count) / elapsed,
        size_on_disk_bytes: size_on_disk(&indexer_url).await?,
    };

    child.terminate();
    println!("{}", to_string_pretty(&report)?);
    Ok(())
}

async fn verify_query(indexer_url: &str, query: &QueryExpectation) -> Result<(), Box<dyn Error>> {
    let limit = u16::try_from(query.min_events.max(16))
        .map_err(|_| io::Error::other("query limit exceeds u16"))?;
    let response = get_events(indexer_url, query.key.clone(), limit).await?;
    validate_query_expectation(query, &response).map_err(io::Error::other)?;
    Ok(())
}

async fn wait_for_queries(
    indexer_url: &str,
    queries: &[QueryExpectation],
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_ready = true;
        for query in queries {
            match verify_query(indexer_url, query).await {
                Ok(()) => {}
                Err(_) if Instant::now() < deadline => {
                    all_ready = false;
                    break;
                }
                Err(err) => return Err(err),
            }
        }

        if all_ready {
            return Ok(());
        }

        sleep(Duration::from_millis(200)).await;
    }
}

fn sibling_binary(name: &str) -> io::Result<PathBuf> {
    let current = std::env::current_exe()?;
    let binary_name = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_owned()
    };
    Ok(current.with_file_name(binary_name))
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn terminate(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}
