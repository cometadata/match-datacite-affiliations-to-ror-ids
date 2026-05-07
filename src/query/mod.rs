use anyhow::{anyhow, Context, Result};
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info, warn};

use crate::{hash_affiliation, RorMatch, RorMatchFailed};

mod checkpoint;
mod client;
pub use checkpoint::Checkpoint;
pub use client::RorClient;

#[derive(Args)]
pub struct QueryArgs {
    /// Working directory (reads unique_affiliations.json)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Working directory (writes ror_matches.jsonl)
    #[arg(short, long)]
    pub output: PathBuf,

    /// Match service base URL
    #[arg(short = 'u', long, default_value = "http://localhost:8000")]
    pub base_url: String,

    /// Task name for the match endpoint
    #[arg(long, default_value = "affiliation")]
    pub task: String,

    /// Concurrent in-flight bulk requests; tune to match Marple's worker count
    #[arg(short, long, default_value = "25")]
    pub concurrency: usize,

    /// Inputs per bulk request (server cap is 50)
    #[arg(short = 'b', long, default_value = "50")]
    pub batch_size: usize,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "30")]
    pub timeout: u64,

    /// Resume from checkpoint
    #[arg(short, long)]
    pub resume: bool,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_async(args))
}

pub async fn run_async(args: QueryArgs) -> Result<()> {
    if args.batch_size == 0 {
        return Err(anyhow!("--batch-size must be at least 1"));
    }
    if args.batch_size > 50 {
        warn!(
            "--batch-size {} exceeds default Marple cap of 50; expect HTTP 413 unless MARPLE_MAX_BATCH_SIZE is raised",
            args.batch_size
        );
    }

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

    let client = Arc::new(RorClient::new(args.base_url.clone(), args.timeout));
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    let task = args.task;
    let batches: Vec<Vec<(String, String)>> = to_process
        .chunks(args.batch_size)
        .map(|c| c.to_vec())
        .collect();
    let mut handles = Vec::with_capacity(batches.len());

    for batch in batches {
        let client = Arc::clone(&client);
        let matches_writer = Arc::clone(&matches_writer);
        let failed_writer = Arc::clone(&failed_writer);
        let checkpoint = Arc::clone(&checkpoint);
        let semaphore = Arc::clone(&semaphore);
        let pb = pb.clone();
        let task = task.clone();

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    error!("Semaphore closed: {}", e);
                    return;
                }
            };

            let inputs: Vec<String> = batch.iter().map(|(a, _)| a.clone()).collect();

            match client.match_bulk(&inputs, &task).await {
                Ok(results) => {
                    let mut match_lines: Vec<String> = Vec::new();
                    let mut failed_lines: Vec<String> = Vec::new();
                    for ((aff, hash), res) in batch.iter().zip(results.into_iter()) {
                        match res {
                            Some((ror_id, confidence)) => {
                                let rec = RorMatch {
                                    affiliation: aff.clone(),
                                    affiliation_hash: hash.clone(),
                                    ror_id,
                                    confidence,
                                };
                                match_lines.push(serde_json::to_string(&rec).unwrap());
                            }
                            None => {
                                let rec = RorMatchFailed {
                                    affiliation: aff.clone(),
                                    affiliation_hash: hash.clone(),
                                    error: "No match found".to_string(),
                                };
                                failed_lines.push(serde_json::to_string(&rec).unwrap());
                            }
                        }
                    }

                    if !match_lines.is_empty() {
                        let mut writer = matches_writer.lock().await;
                        for line in &match_lines {
                            if let Err(e) = writeln!(writer, "{}", line) {
                                error!("Failed to write match: {}", e);
                            }
                        }
                    }
                    if !failed_lines.is_empty() {
                        let mut writer = failed_writer.lock().await;
                        for line in &failed_lines {
                            if let Err(e) = writeln!(writer, "{}", line) {
                                error!("Failed to write failure: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    let err_msg = format!("Batch error: {}", e);
                    let mut writer = failed_writer.lock().await;
                    for (aff, hash) in &batch {
                        let rec = RorMatchFailed {
                            affiliation: aff.clone(),
                            affiliation_hash: hash.clone(),
                            error: err_msg.clone(),
                        };
                        if let Err(e) = writeln!(writer, "{}", serde_json::to_string(&rec).unwrap()) {
                            error!("Failed to write failure: {}", e);
                        }
                    }
                }
            }

            {
                let mut cp = checkpoint.lock().await;
                for (_, hash) in &batch {
                    cp.mark_processed(hash);
                }
            }

            pb.inc(batch.len() as u64);
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
