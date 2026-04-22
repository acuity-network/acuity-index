use acuity_index::synthetic_devnet::{
    BenchmarkReport, JsonWsClient, QueryExpectation, SeedManifest, pick_unused_port,
    unique_temp_path, validate_query_expectation, write_synthetic_index_spec,
};
use clap::Parser;
use serde_json::to_string_pretty;
use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
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
    #[arg(long, default_value_t = 4)]
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
    if args.queue_depth == 0 {
        return Err(io::Error::other("starting queue depth must be greater than 0").into());
    }

    let manifest: SeedManifest = serde_json::from_slice(&fs::read(&args.manifest)?)?;
    let workdir = args
        .workdir
        .unwrap_or_else(|| unique_temp_path("synthetic-index-benchmark"));
    fs::create_dir_all(&workdir)?;

    let config_path = workdir.join("synthetic.toml");
    write_synthetic_index_spec(&config_path, &args.node_url, &manifest.genesis_hash)?;

    let db_root = args.db_path.unwrap_or_else(|| workdir.join("db"));
    fs::create_dir_all(&db_root)?;

    let indexer_bin = match args.indexer_bin {
        Some(path) => path,
        None => sibling_binary("acuity-index")?,
    };

    let mut rows = Vec::new();
    let mut queue_depth = args.queue_depth;
    loop {
        match run_benchmark_once(
            &args.node_url,
            &manifest,
            &config_path,
            &indexer_bin,
            &db_root,
            args.indexer_port,
            args.timeout_secs,
            queue_depth,
        )
        .await
        {
            Ok(report) => {
                println!("{}", to_string_pretty(&report)?);
                rows.push(SummaryRow::from(&report));
            }
            Err(err) => {
                print_summary_table(&rows);
                println!("failure queue_depth: {queue_depth}");
                println!("failure error: {err}");
                return Ok(());
            }
        }

        let Some(next_queue_depth) = queue_depth.checked_mul(2) else {
            print_summary_table(&rows);
            println!("failure queue_depth: overflow");
            println!("failure error: next queue depth would exceed u8");
            return Ok(());
        };
        queue_depth = next_queue_depth;
    }
}

async fn run_benchmark_once(
    node_url: &str,
    manifest: &SeedManifest,
    config_path: &Path,
    indexer_bin: &Path,
    db_root: &Path,
    indexer_port: Option<u16>,
    timeout_secs: u64,
    queue_depth: u8,
) -> Result<BenchmarkReport, Box<dyn Error>> {
    let db_path = db_root.join(format!("queue-depth-{queue_depth}"));
    if db_path.exists() {
        fs::remove_dir_all(&db_path)?;
    }
    fs::create_dir_all(&db_path)?;
    let db_path_guard = TempDirGuard::new(db_path.clone());

    let indexer_port = indexer_port.unwrap_or(pick_unused_port()?);
    let indexer_url = format!("ws://127.0.0.1:{indexer_port}");
    let max_events_limit = manifest.synthetic_event_count.max(1024).to_string();

    let mut child = ChildGuard {
        child: Command::new(indexer_bin)
            .arg("--index-config")
            .arg(config_path)
            .arg("--url")
            .arg(node_url)
            .arg("--db-path")
            .arg(db_path_guard.path())
            .arg("--queue-depth")
            .arg(queue_depth.to_string())
            .arg("--port")
            .arg(indexer_port.to_string())
            .arg("--max-events-limit")
            .arg(max_events_limit)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?,
    };

    let started = Instant::now();
    let deadline = started + Duration::from_secs(timeout_secs);
    let mut indexer_ws = connect_indexer(&indexer_url, deadline).await?;
    wait_for_queries(&mut indexer_ws, &manifest.queries, deadline, timeout_secs).await?;

    let elapsed = started.elapsed().as_secs_f64();
    if elapsed == 0.0 {
        return Err(io::Error::other("benchmark finished too quickly to measure").into());
    }

    let report = BenchmarkReport {
        chain_tip: manifest.end_block,
        indexed_blocks: manifest.total_blocks,
        queue_depth,
        synthetic_event_count: manifest.synthetic_event_count,
        elapsed_seconds: elapsed,
        blocks_per_second: f64::from(manifest.total_blocks) / elapsed,
        synthetic_events_per_second: f64::from(manifest.synthetic_event_count) / elapsed,
        size_on_disk_bytes: indexer_ws.size_on_disk().await?,
    };

    child.terminate();
    Ok(report)
}

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Debug, Clone)]
struct SummaryRow {
    queue_depth: u8,
    elapsed_seconds: f64,
    blocks_per_second: f64,
    synthetic_events_per_second: f64,
}

impl From<&BenchmarkReport> for SummaryRow {
    fn from(report: &BenchmarkReport) -> Self {
        Self {
            queue_depth: report.queue_depth,
            elapsed_seconds: report.elapsed_seconds,
            blocks_per_second: report.blocks_per_second,
            synthetic_events_per_second: report.synthetic_events_per_second,
        }
    }
}

fn print_summary_table(rows: &[SummaryRow]) {
    println!();
    let headers = ["queue", "elapsed", "blocks/s", "events/s"];
    let formatted_rows: Vec<[String; 4]> = rows
        .iter()
        .map(|row| {
            [
                row.queue_depth.to_string(),
                format!("{:.3} s", row.elapsed_seconds),
                format!("{:.3}", row.blocks_per_second),
                format!("{:.3}", row.synthetic_events_per_second),
            ]
        })
        .collect();
    let widths = [0, 1, 2, 3].map(|column| {
        let header_width = headers[column].len();
        let value_width = formatted_rows
            .iter()
            .map(|row| row[column].len())
            .max()
            .unwrap_or(0);
        header_width.max(value_width)
    });

    println!(
        "{:<w0$}  {:>w1$}  {:>w2$}  {:>w3$}",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
    );
    println!(
        "{:<w0$}  {:>w1$}  {:>w2$}  {:>w3$}",
        "-".repeat(widths[0]),
        "-".repeat(widths[1]),
        "-".repeat(widths[2]),
        "-".repeat(widths[3]),
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
    );

    for row in formatted_rows {
        println!(
            "{:<w0$}  {:>w1$}  {:>w2$}  {:>w3$}",
            row[0],
            row[1],
            row[2],
            row[3],
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3],
        );
    }
}

async fn connect_indexer(
    indexer_url: &str,
    deadline: Instant,
) -> Result<JsonWsClient, Box<dyn Error>> {
    loop {
        match JsonWsClient::connect(indexer_url).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(err);
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn verify_query(
    client: &mut JsonWsClient,
    query: &QueryExpectation,
) -> Result<(), Box<dyn Error>> {
    let limit = u16::try_from(query.min_events.max(16))
        .map_err(|_| io::Error::other("query limit exceeds u16"))?;
    let response = client.get_events(query.key.clone(), limit).await?;
    validate_query_expectation(query, &response).map_err(io::Error::other)?;
    Ok(())
}

async fn wait_for_queries(
    client: &mut JsonWsClient,
    queries: &[QueryExpectation],
    deadline: Instant,
    timeout_secs: u64,
) -> Result<(), Box<dyn Error>> {
    loop {
        let mut all_ready = true;
        for query in queries {
            match verify_query(client, query).await {
                Ok(()) => {}
                Err(_) if Instant::now() < deadline => {
                    all_ready = false;
                    break;
                }
                Err(err) => {
                    return Err(io::Error::other(format!(
                        "timed out after {timeout_secs}s waiting for benchmark query '{}': {err}",
                        query.description,
                    ))
                    .into());
                }
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
