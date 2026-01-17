use anyhow::{Context, Result};
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};
use xxhash_rust::xxh3::xxh3_64;

mod checkpoint;
mod client;
pub use checkpoint::Checkpoint;
pub use client::RorClient;

fn hash_affiliation(affiliation: &str) -> String {
    format!("{:016x}", xxh3_64(affiliation.as_bytes()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RorMatch {
    affiliation: String,
    affiliation_hash: String,
    ror_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RorMatchFailed {
    affiliation: String,
    affiliation_hash: String,
    error: String,
}

#[derive(Args)]
pub struct QueryArgs {
    /// Working directory (reads unique_affiliations.json)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Working directory (writes ror_matches.jsonl)
    #[arg(short, long)]
    pub output: PathBuf,

    /// ROR API base URL
    #[arg(short = 'u', long, default_value = "http://localhost:9292")]
    pub base_url: String,

    /// Concurrent requests
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "30")]
    pub timeout: u64,

    /// Resume from checkpoint
    #[arg(short, long)]
    pub resume: bool,

    /// Enable fallback to standard affiliation endpoint
    #[arg(short, long)]
    pub fallback_multi: bool,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_async(args))
}

pub async fn run_async(args: QueryArgs) -> Result<()> {
    fs::create_dir_all(&args.output)
        .context("Failed to create output directory")?;

    let affiliations_path = args.input.join("unique_affiliations.json");
    let affiliations_file = File::open(&affiliations_path)
        .with_context(|| format!("Failed to open {}", affiliations_path.display()))?;
    let affiliations: Vec<String> = serde_json::from_reader(affiliations_file)
        .context("Failed to parse unique_affiliations.json")?;

    info!("Loaded {} affiliations", affiliations.len());

    let checkpoint_path = args.output.join("ror_matches.checkpoint");
    let checkpoint = if args.resume && checkpoint_path.exists() {
        Checkpoint::load(&checkpoint_path)
            .context("Failed to load checkpoint")?
    } else {
        Checkpoint::new(&checkpoint_path)
    };

    let to_process: Vec<(String, String)> = affiliations
        .into_iter()
        .map(|aff| {
            let hash = hash_affiliation(&aff);
            (aff, hash)
        })
        .filter(|(_, hash)| !checkpoint.is_processed(hash))
        .collect();

    let total = to_process.len();
    let already_processed = checkpoint.len();

    if already_processed > 0 {
        info!(
            "Resuming: {} already processed, {} remaining",
            already_processed, total
        );
    }

    if total == 0 {
        info!("No affiliations to process");
        return Ok(());
    }

    let matches_path = args.output.join("ror_matches.jsonl");
    let failed_path = args.output.join("ror_matches.failed.jsonl");

    let matches_file = if args.resume && matches_path.exists() {
        fs::OpenOptions::new()
            .append(true)
            .open(&matches_path)
            .context("Failed to open matches file for append")?
    } else {
        File::create(&matches_path)
            .context("Failed to create matches file")?
    };

    let failed_file = if args.resume && failed_path.exists() {
        fs::OpenOptions::new()
            .append(true)
            .open(&failed_path)
            .context("Failed to open failed file for append")?
    } else {
        File::create(&failed_path)
            .context("Failed to create failed file")?
    };

    let matches_writer = Arc::new(Mutex::new(BufWriter::new(matches_file)));
    let failed_writer = Arc::new(Mutex::new(BufWriter::new(failed_file)));
    let checkpoint = Arc::new(Mutex::new(checkpoint));

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let client = Arc::new(RorClient::new(
        args.base_url.clone(),
        args.concurrency,
        args.timeout,
    ));

    let mut handles = Vec::with_capacity(total);
    let fallback_multi = args.fallback_multi;

    for (affiliation, hash) in to_process {
        let client = Arc::clone(&client);
        let matches_writer = Arc::clone(&matches_writer);
        let failed_writer = Arc::clone(&failed_writer);
        let checkpoint = Arc::clone(&checkpoint);
        let pb = pb.clone();

        let handle = tokio::spawn(async move {

            match client.query_affiliation(&affiliation, fallback_multi).await {
                Ok(Some(ror_id)) => {
                    let match_record = RorMatch {
                        affiliation: affiliation.clone(),
                        affiliation_hash: hash.clone(),
                        ror_id,
                    };

                    let mut writer = matches_writer.lock().await;
                    if let Err(e) = writeln!(
                        writer,
                        "{}",
                        serde_json::to_string(&match_record).unwrap()
                    ) {
                        error!("Failed to write match: {}", e);
                    }
                }
                Ok(None) => {
                    // No match found - record as failure with "no match" error
                    let failed_record = RorMatchFailed {
                        affiliation: affiliation.clone(),
                        affiliation_hash: hash.clone(),
                        error: "No match found".to_string(),
                    };

                    let mut writer = failed_writer.lock().await;
                    if let Err(e) = writeln!(
                        writer,
                        "{}",
                        serde_json::to_string(&failed_record).unwrap()
                    ) {
                        error!("Failed to write failure: {}", e);
                    }
                }
                Err(e) => {
                    let failed_record = RorMatchFailed {
                        affiliation: affiliation.clone(),
                        affiliation_hash: hash.clone(),
                        error: e.to_string(),
                    };

                    let mut writer = failed_writer.lock().await;
                    if let Err(e) = writeln!(
                        writer,
                        "{}",
                        serde_json::to_string(&failed_record).unwrap()
                    ) {
                        error!("Failed to write failure: {}", e);
                    }
                }
            }

            // Mark as processed in checkpoint
            {
                let mut cp = checkpoint.lock().await;
                cp.mark_processed(&hash);
            }

            pb.inc(1);
        });

        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("Task failed: {}", e);
        }
    }

    pb.finish_with_message("Done");

    {
        let mut writer = matches_writer.lock().await;
        writer.flush().context("Failed to flush matches file")?;
    }
    {
        let mut writer = failed_writer.lock().await;
        writer.flush().context("Failed to flush failed file")?;
    }
    {
        let cp = checkpoint.lock().await;
        cp.save().context("Failed to save checkpoint")?;
    }

    info!("Query complete");

    Ok(())
}
