use anyhow::{anyhow, Context, Result};
use clap::Args;
use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use crate::{AffiliationOccurrenceRecord, PartyType};

mod parser;
mod ror;
pub use parser::parse_affiliations;

#[derive(Args)]
pub struct ExtractArgs {
    /// Directory containing .jsonl.gz files
    #[arg(short, long)]
    pub input: PathBuf,

    /// Working directory for output files
    #[arg(short, long)]
    pub output: PathBuf,

    /// Number of threads (0 = auto)
    #[arg(short, long, default_value = "0")]
    pub threads: usize,

    /// Records per batch
    #[arg(short, long, default_value = "5000")]
    pub batch_size: usize,
}

pub fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    let pattern_str = pattern.to_string_lossy();
    Ok(glob(&pattern_str)?.filter_map(Result::ok).collect())
}

#[derive(Debug, Default, Serialize)]
struct FileExtractionStats {
    input_files: u64,
    processed_files: u64,
    processing_complete: bool,
    valid_records: u64,
    valid_dois: u64,
    occurrence_count: u64,
    creator_occurrences: u64,
    contributor_occurrences: u64,
    excluded_zero_length_affiliations: u64,
    malformed_json_records: u64,
    malformed_records: u64,
}

struct FileExtractionOutcome {
    stats: FileExtractionStats,
    error: Option<anyhow::Error>,
}

#[derive(Serialize)]
struct DoiRecord {
    doi: String,
}

impl FileExtractionStats {
    fn checked_add_assign(&mut self, other: &Self) -> Result<()> {
        self.valid_records = self
            .valid_records
            .checked_add(other.valid_records)
            .ok_or_else(|| anyhow!("valid_records total overflowed"))?;
        self.valid_dois = self
            .valid_dois
            .checked_add(other.valid_dois)
            .ok_or_else(|| anyhow!("valid_dois total overflowed"))?;
        self.occurrence_count = self
            .occurrence_count
            .checked_add(other.occurrence_count)
            .ok_or_else(|| anyhow!("occurrence_count total overflowed"))?;
        self.creator_occurrences = self
            .creator_occurrences
            .checked_add(other.creator_occurrences)
            .ok_or_else(|| anyhow!("creator_occurrences total overflowed"))?;
        self.contributor_occurrences = self
            .contributor_occurrences
            .checked_add(other.contributor_occurrences)
            .ok_or_else(|| anyhow!("contributor_occurrences total overflowed"))?;
        self.excluded_zero_length_affiliations = self
            .excluded_zero_length_affiliations
            .checked_add(other.excluded_zero_length_affiliations)
            .ok_or_else(|| anyhow!("excluded_zero_length_affiliations total overflowed"))?;
        self.malformed_json_records = self
            .malformed_json_records
            .checked_add(other.malformed_json_records)
            .ok_or_else(|| anyhow!("malformed_json_records total overflowed"))?;
        self.malformed_records = self
            .malformed_records
            .checked_add(other.malformed_records)
            .ok_or_else(|| anyhow!("malformed_records total overflowed"))?;
        Ok(())
    }
}

fn process_file(
    filepath: &Path,
    unique_affiliations: &Mutex<HashSet<String>>,
    occurrence_tx: &crossbeam_channel::Sender<Vec<AffiliationOccurrenceRecord>>,
    doi_tx: &crossbeam_channel::Sender<Vec<DoiRecord>>,
    batch_size: usize,
) -> FileExtractionOutcome {
    let mut occurrence_batch = Vec::with_capacity(batch_size);
    let mut doi_batch: Vec<DoiRecord> = Vec::with_capacity(batch_size);
    let mut stats = FileExtractionStats::default();

    let result = (|| -> Result<()> {
        let file = File::open(filepath)
            .with_context(|| format!("Failed to open {}", filepath.display()))?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        for line in reader.lines() {
            let line_str = line?;
            if line_str.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<serde_json::Value>(&line_str) {
                Ok(record) => match parse_affiliations(&record) {
                    Ok(parsed) => {
                        stats.valid_records += 1;
                        stats.valid_dois += 1;
                        stats.excluded_zero_length_affiliations +=
                            parsed.excluded_zero_length_affiliations;
                        doi_batch.push(DoiRecord { doi: parsed.doi });

                        for occurrence in parsed.occurrences {
                            stats.occurrence_count += 1;
                            match occurrence.party_type {
                                PartyType::Creator => stats.creator_occurrences += 1,
                                PartyType::Contributor => stats.contributor_occurrences += 1,
                            }
                            unique_affiliations
                                .lock()
                                .unwrap()
                                .insert(occurrence.affiliation.clone());
                            occurrence_batch.push(occurrence);
                        }
                    }
                    Err(error) => {
                        stats.malformed_records += 1;
                        error!("Failed to parse DataCite affiliations: {}", error);
                    }
                },
                Err(error) => {
                    stats.malformed_json_records += 1;
                    error!("Failed to parse JSON record: {}", error);
                }
            }

            if occurrence_batch.len() >= batch_size {
                occurrence_tx
                    .send(std::mem::take(&mut occurrence_batch))
                    .map_err(|_| {
                        anyhow!("occurrence writer stopped before extraction completed")
                    })?;
            }
            if doi_batch.len() >= batch_size {
                doi_tx
                    .send(std::mem::take(&mut doi_batch))
                    .map_err(|_| anyhow!("DOI writer stopped before extraction completed"))?;
            }
        }

        if !occurrence_batch.is_empty() {
            occurrence_tx
                .send(occurrence_batch)
                .map_err(|_| anyhow!("occurrence writer stopped before extraction completed"))?;
        }
        if !doi_batch.is_empty() {
            doi_tx
                .send(doi_batch)
                .map_err(|_| anyhow!("DOI writer stopped before extraction completed"))?;
        }
        Ok(())
    })();

    FileExtractionOutcome {
        stats,
        error: result.err(),
    }
}

fn write_jsonl<T: Serialize>(path: PathBuf, rx: crossbeam_channel::Receiver<Vec<T>>) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    while let Ok(batch) = rx.recv() {
        for record in batch {
            serde_json::to_writer(&mut writer, &record)?;
            writer.write_all(b"\n")?;
        }
    }

    writer.flush()?;
    writer.into_inner()?.sync_all()?;
    Ok(())
}

fn write_report(path: PathBuf, stats: &FileExtractionStats) -> Result<()> {
    let mut file = File::create(path)?;
    serde_json::to_writer(&mut file, stats)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

pub fn run(args: ExtractArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("datacite_ror=info".parse().unwrap()),
        )
        .try_init()
        .ok();

    fs::create_dir_all(&args.output)?;

    let num_threads = if args.threads > 0 {
        args.threads
    } else {
        num_cpus::get()
    };
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok();
    info!("Using {} threads", num_threads);

    let files = find_jsonl_gz_files(&args.input)?;
    info!("Found {} files to process", files.len());
    let progress = ProgressBar::new(files.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"),
    );

    let unique_affiliations: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let channel_capacity = num_threads.max(1).saturating_mul(4);
    let (occurrence_tx, occurrence_rx) =
        bounded::<Vec<AffiliationOccurrenceRecord>>(channel_capacity);
    let (doi_tx, doi_rx) = bounded::<Vec<DoiRecord>>(channel_capacity);

    let occurrence_writer = std::thread::spawn({
        let output_path = args.output.join("doi_affiliation_occurrences.jsonl");
        move || write_jsonl(output_path, occurrence_rx)
    });
    let doi_writer = std::thread::spawn({
        let output_path = args.output.join("dois.jsonl");
        move || write_jsonl(output_path, doi_rx)
    });

    let unique_ref = Arc::clone(&unique_affiliations);
    let file_results: Vec<FileExtractionOutcome> = files
        .par_iter()
        .map(|filepath| {
            let outcome = process_file(
                filepath,
                &unique_ref,
                &occurrence_tx,
                &doi_tx,
                args.batch_size.max(1),
            );
            progress.inc(1);
            outcome
        })
        .collect();

    drop(occurrence_tx);
    drop(doi_tx);
    let occurrence_writer_result = occurrence_writer
        .join()
        .unwrap_or_else(|_| Err(anyhow!("occurrence writer thread panicked")));
    let doi_writer_result = doi_writer
        .join()
        .unwrap_or_else(|_| Err(anyhow!("DOI writer thread panicked")));
    progress.finish();

    let mut stats = FileExtractionStats {
        input_files: files.len() as u64,
        ..Default::default()
    };
    let mut processing_error = None;
    for outcome in file_results {
        if let Err(error) = stats.checked_add_assign(&outcome.stats) {
            processing_error.get_or_insert(error);
        }
        if let Some(error) = outcome.error {
            error!("Failed to process input file: {}", error);
            processing_error.get_or_insert(error);
        } else {
            stats.processed_files += 1;
        }
    }
    if let Err(error) = occurrence_writer_result {
        processing_error.get_or_insert(error);
    }
    if let Err(error) = doi_writer_result {
        processing_error.get_or_insert(error);
    }

    let unique_result = (|| -> Result<usize> {
        let unique = unique_affiliations.lock().unwrap();
        let affiliations_vec: Vec<&String> = unique.iter().collect();
        let affiliations_path = args.output.join("unique_affiliations.json");
        let file = File::create(&affiliations_path)?;
        serde_json::to_writer(file, &affiliations_vec)?;
        Ok(affiliations_vec.len())
    })();
    let unique_affiliation_count = match unique_result {
        Ok(count) => count,
        Err(error) => {
            processing_error.get_or_insert(error);
            0
        }
    };

    stats.processing_complete =
        processing_error.is_none() && stats.processed_files == stats.input_files;
    write_report(args.output.join("extraction_report.json"), &stats)?;

    info!("Extracted {} unique affiliations", unique_affiliation_count);
    info!("Output: {}", args.output.display());

    if let Some(error) = processing_error {
        return Err(error);
    }
    if stats.malformed_json_records != 0 || stats.malformed_records != 0 {
        return Err(anyhow!(
            "Extraction completed with {} malformed JSON record(s) and {} malformed record(s)",
            stats.malformed_json_records,
            stats.malformed_records
        ));
    }

    Ok(())
}
