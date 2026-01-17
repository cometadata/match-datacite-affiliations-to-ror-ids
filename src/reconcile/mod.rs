use anyhow::Result;
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::{AuthorAffiliationRecord, EnrichedAffiliation, EnrichedCreator, EnrichedRecord, RorMatch};

#[derive(Args)]
pub struct ReconcileArgs {
    /// Working directory (reads relationship and match files)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Output file
    #[arg(short, long, default_value = "enriched_records.jsonl")]
    pub output: PathBuf,
}

pub fn load_ror_matches<P: AsRef<Path>>(path: P) -> Result<HashMap<String, String>> {
    let mut lookup = HashMap::new();

    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_str::<RorMatch>(&line) {
            lookup.insert(record.affiliation_hash, record.ror_id);
        }
    }

    Ok(lookup)
}

struct AuthorData {
    name: String,
    affiliations: Vec<(String, String, String)>, // (name, hash, ror_id)
}

/// Returns None if no authors have matched affiliations
fn process_doi_group(
    doi: &str,
    records: &[AuthorAffiliationRecord],
    ror_lookup: &HashMap<String, String>,
) -> Option<EnrichedRecord> {
    // BTreeMap preserves author order by index
    let mut authors: BTreeMap<usize, AuthorData> = BTreeMap::new();

    for record in records {
        let author_entry = authors.entry(record.author_idx).or_insert_with(|| AuthorData {
            name: record.author_name.clone(),
            affiliations: Vec::new(),
        });

        if let Some(ror_id) = ror_lookup.get(&record.affiliation_hash) {
            author_entry.affiliations.push((
                record.affiliation.clone(),
                record.affiliation_hash.clone(),
                ror_id.clone(),
            ));
        }
    }

    let creators: Vec<EnrichedCreator> = authors
        .into_values()
        .filter(|author| !author.affiliations.is_empty())
        .map(|author| {
            let affiliations: Vec<EnrichedAffiliation> = author
                .affiliations
                .into_iter()
                .map(|(name, _hash, ror_id)| EnrichedAffiliation {
                    name,
                    affiliation_identifier: ror_id,
                    affiliation_identifier_scheme: "ROR".to_string(),
                    scheme_uri: "https://ror.org".to_string(),
                })
                .collect();

            EnrichedCreator {
                name: author.name,
                given_name: None,
                family_name: None,
                affiliation: affiliations,
            }
        })
        .collect();

    if creators.is_empty() {
        None
    } else {
        Some(EnrichedRecord {
            doi: doi.to_string(),
            creators,
        })
    }
}

pub fn run(args: ReconcileArgs) -> Result<()> {
    let relationships_path = args.input.join("doi_author_affiliations.jsonl");
    let matches_path = args.input.join("ror_matches.jsonl");

    eprintln!("Loading ROR matches from {:?}...", matches_path);
    let ror_lookup = load_ror_matches(&matches_path)?;
    eprintln!("Loaded {} ROR matches", ror_lookup.len());

    let line_count = {
        let file = File::open(&relationships_path)?;
        BufReader::new(file).lines().count() as u64
    };

    let progress = ProgressBar::new(line_count);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"),
    );

    let input_file = File::open(&relationships_path)?;
    let reader = BufReader::new(input_file);
    let output_file = File::create(&args.output)?;
    let mut writer = BufWriter::new(output_file);

    let mut current_doi: Option<String> = None;
    let mut current_group: Vec<AuthorAffiliationRecord> = Vec::new();
    let mut records_written = 0u64;

    for line in reader.lines() {
        progress.inc(1);
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let record: AuthorAffiliationRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(_) => continue,
        };

        let is_new_doi = current_doi.as_ref() != Some(&record.doi);

        if is_new_doi && !current_group.is_empty() {
            if let Some(enriched) = process_doi_group(
                current_doi.as_ref().unwrap(),
                &current_group,
                &ror_lookup,
            ) {
                writeln!(writer, "{}", serde_json::to_string(&enriched)?)?;
                records_written += 1;
            }
            current_group.clear();
        }

        current_doi = Some(record.doi.clone());
        current_group.push(record);
    }

    if !current_group.is_empty() {
        if let Some(enriched) = process_doi_group(
            current_doi.as_ref().unwrap(),
            &current_group,
            &ror_lookup,
        ) {
            writeln!(writer, "{}", serde_json::to_string(&enriched)?)?;
            records_written += 1;
        }
    }

    progress.finish_with_message("Done");
    writer.flush()?;

    eprintln!(
        "Wrote {} enriched records to {:?}",
        records_written, args.output
    );

    Ok(())
}
