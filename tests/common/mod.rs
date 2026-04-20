use acuity_index::synthetic_devnet::{write_synthetic_index_spec, SeedManifest};
use std::{
    env,
    error::Error,
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn runtime_manifest() -> PathBuf {
    repo_root().join("runtime/Cargo.toml")
}

pub fn runtime_wasm() -> PathBuf {
    repo_root().join("runtime/target/release/wbuild/synthetic-runtime/synthetic_runtime.wasm")
}

pub fn build_runtime_release() -> Result<(), Box<dyn Error>> {
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

pub fn write_config(path: &Path, node_url: &str, genesis_hash: &str) -> Result<(), Box<dyn Error>> {
    write_synthetic_index_spec(path, node_url, genesis_hash)
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

pub fn start_indexer(
    config_path: &Path,
    node_url: &str,
    db_path: &Path,
    port: u16,
    log_path: &Path,
) -> Result<ManagedChild, Box<dyn Error>> {
    let log_file = File::create(log_path)?;
    let log_file_err = log_file.try_clone()?;
    let bin = env::var("CARGO_BIN_EXE_acuity-index")?;
    let child = Command::new(bin)
        .arg("--index-config")
        .arg(config_path)
        .arg("--url")
        .arg(node_url)
        .arg("--db-path")
        .arg(db_path)
        .arg("--port")
        .arg(port.to_string())
        .arg("--max-events-limit")
        .arg("4096")
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

pub struct ManagedChild {
    child: Child,
}

impl ManagedChild {
    pub fn terminate(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.terminate();
    }
}
