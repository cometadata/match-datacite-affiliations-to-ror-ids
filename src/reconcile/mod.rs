mod ror_data;
pub use ror_data::load_ror_data;

use anyhow::Result;
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::{
    AuthorAffiliationRecord, Disagreement, EnrichedAffiliation, EnrichedCreator, EnrichedRecord,
    EnrichmentConfig, EnrichmentOutputRecord, ExistingAssignment, ExistingAssignmentAggregated,
    RorIdCount, RorMatch,
};

#[derive(Args)]
pub struct ReconcileArgs {
    /// Working directory (reads relationship and match files)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Output file for enriched records
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Path to ROR data dump JSON file
    #[arg(short, long)]
    pub ror_data: PathBuf,

    /// Output in DataCite enrichment format
    #[arg(long)]
    pub enrichment_format: bool,

    /// Path to enrichment config YAML file (required with --enrichment-format).
    /// Must contain "contributors" and "resources" arrays.
    #[arg(long)]
    pub enrichment_config: Option<PathBuf>,
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

struct AffiliationData {
    name: String,
    ror_id: Option<String>, // None if no ROR match
}

struct AuthorData {
    name: String,
    name_type: Option<String>,
    given_name: Option<String>,
    family_name: Option<String>,
    affiliations: Vec<AffiliationData>,
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
            name_type: record.author_name_type.clone(),
            given_name: record.author_given_name.clone(),
            family_name: record.author_family_name.clone(),
            affiliations: Vec::new(),
        });

        let ror_id = ror_lookup.get(&record.affiliation_hash).cloned();
        author_entry.affiliations.push(AffiliationData {
            name: record.affiliation.clone(),
            ror_id,
        });
    }

    let creators: Vec<EnrichedCreator> = authors
        .into_values()
        .filter(|author| author.affiliations.iter().any(|a| a.ror_id.is_some()))
        .map(|author| {
            let affiliations: Vec<EnrichedAffiliation> = author
                .affiliations
                .into_iter()
                .filter_map(|a| {
                    a.ror_id.map(|ror_id| EnrichedAffiliation {
                        name: a.name,
                        affiliation_identifier: ror_id,
                        affiliation_identifier_scheme: "ROR".to_string(),
                        scheme_uri: "https://ror.org".to_string(),
                    })
                })
                .collect();

            EnrichedCreator {
                name: author.name,
                given_name: author.given_name,
                family_name: author.family_name,
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

/// Build a creator JSON object with name fields, optionally including affiliations
fn build_creator_value(
    author: &AuthorData,
    affiliations: Option<Vec<serde_json::Value>>,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    if let Some(ref nt) = author.name_type {
        obj.insert("nameType".to_string(), serde_json::json!(nt));
    }
    if let Some(ref gn) = author.given_name {
        obj.insert("givenName".to_string(), serde_json::json!(gn));
    }
    if let Some(ref fn_) = author.family_name {
        obj.insert("familyName".to_string(), serde_json::json!(fn_));
    }
    obj.insert("name".to_string(), serde_json::json!(&author.name));
    if let Some(affs) = affiliations {
        obj.insert("affiliation".to_string(), serde_json::json!(affs));
    }
    serde_json::Value::Object(obj)
}

/// Returns one EnrichmentOutputRecord per author with matched affiliations
fn process_doi_group_enrichment(
    doi: &str,
    records: &[AuthorAffiliationRecord],
    ror_lookup: &HashMap<String, String>,
    config: &EnrichmentConfig,
) -> Vec<EnrichmentOutputRecord> {
    let mut authors: BTreeMap<usize, AuthorData> = BTreeMap::new();

    for record in records {
        let author_entry = authors.entry(record.author_idx).or_insert_with(|| AuthorData {
            name: record.author_name.clone(),
            name_type: record.author_name_type.clone(),
            given_name: record.author_given_name.clone(),
            family_name: record.author_family_name.clone(),
            affiliations: Vec::new(),
        });

        let ror_id = ror_lookup.get(&record.affiliation_hash).cloned();
        author_entry.affiliations.push(AffiliationData {
            name: record.affiliation.clone(),
            ror_id,
        });
    }

    authors
        .into_values()
        .filter(|author| author.affiliations.iter().any(|a| a.ror_id.is_some()))
        .map(|author| {
            // originalValue: creator with original affiliations (name only)
            let original_affiliations: Vec<serde_json::Value> = author
                .affiliations
                .iter()
                .map(|a| serde_json::json!({ "name": a.name }))
                .collect();

            // enrichedValue: creator with ROR-enriched affiliations
            let enriched_affiliations: Vec<serde_json::Value> = author
                .affiliations
                .iter()
                .map(|a| {
                    if let Some(ref ror_id) = a.ror_id {
                        serde_json::json!({
                            "name": a.name,
                            "affiliationIdentifier": ror_id,
                            "affiliationIdentifierScheme": "ROR",
                            "schemeUri": "https://ror.org"
                        })
                    } else {
                        serde_json::json!({ "name": a.name })
                    }
                })
                .collect();

            let original_value = build_creator_value(&author, Some(original_affiliations));
            let enriched_value = build_creator_value(&author, Some(enriched_affiliations));

            EnrichmentOutputRecord {
                doi: doi.to_string(),
                contributors: config.contributors.clone(),
                resources: config.resources.clone(),
                field: "creators".to_string(),
                action: "updateChild".to_string(),
                original_value,
                enriched_value,
            }
        })
        .collect()
}

pub fn run(args: ReconcileArgs) -> Result<()> {
    let relationships_path = args.input.join("doi_author_affiliations.jsonl");
    let matches_path = args.input.join("ror_matches.jsonl");

    let enrichment_config = if args.enrichment_format {
        let config_path = args.enrichment_config.as_ref().ok_or_else(|| {
            anyhow::anyhow!("--enrichment-config is required when using --enrichment-format")
        })?;
        let config_str = std::fs::read_to_string(config_path)?;
        let config: EnrichmentConfig = serde_yaml::from_str(&config_str)?;
        Some(config)
    } else {
        None
    };

    let default_output = if args.enrichment_format {
        PathBuf::from("enrichments.jsonl")
    } else {
        PathBuf::from("enriched_records.jsonl")
    };
    let output_path = args.output.unwrap_or(default_output);

    eprintln!("Loading ROR data from {:?}...", args.ror_data);
    let ror_names = load_ror_data(&args.ror_data)?;
    eprintln!("Loaded {} ROR organizations", ror_names.len());

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

    let mut existing_by_hash: HashMap<String, HashMap<String, usize>> = HashMap::new(); // hash -> (ror_id -> count)
    let mut affiliation_strings: HashMap<String, String> = HashMap::new(); // hash -> affiliation string

    let mut current_doi: Option<String> = None;
    let mut current_group: Vec<AuthorAffiliationRecord> = Vec::new();

    let output_dir = output_path.parent().unwrap_or(Path::new("."));
    let enriched_file = File::create(&output_path)?;
    let mut enriched_writer = BufWriter::new(enriched_file);

    let existing_file = File::create(output_dir.join("existing_assignments.jsonl"))?;
    let mut existing_writer = BufWriter::new(existing_file);

    let input_file = File::open(&relationships_path)?;
    let reader = BufReader::new(input_file);
    let mut records_enriched = 0u64;
    let mut records_existing = 0u64;

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

        affiliation_strings
            .entry(record.affiliation_hash.clone())
            .or_insert_with(|| record.affiliation.clone());

        if let Some(ref existing_ror_id) = record.existing_ror_id {
            let ror_name = ror_names
                .get(existing_ror_id)
                .cloned()
                .unwrap_or_else(|| "Unknown".to_string());

            let assignment = ExistingAssignment {
                doi: record.doi.clone(),
                author_idx: record.author_idx,
                author_name: record.author_name.clone(),
                affiliation: record.affiliation.clone(),
                ror_id: existing_ror_id.clone(),
                ror_name: ror_name.clone(),
            };
            writeln!(existing_writer, "{}", serde_json::to_string(&assignment)?)?;
            records_existing += 1;

            existing_by_hash
                .entry(record.affiliation_hash.clone())
                .or_default()
                .entry(existing_ror_id.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        } else {
            let is_new_doi = current_doi.as_ref() != Some(&record.doi);

            if is_new_doi && !current_group.is_empty() {
                if let Some(ref config) = enrichment_config {
                    let enrichments = process_doi_group_enrichment(
                        current_doi.as_ref().unwrap(),
                        &current_group,
                        &ror_lookup,
                        config,
                    );
                    for enrichment in &enrichments {
                        writeln!(enriched_writer, "{}", serde_json::to_string(enrichment)?)?;
                    }
                    records_enriched += enrichments.len() as u64;
                } else if let Some(enriched) = process_doi_group(
                    current_doi.as_ref().unwrap(),
                    &current_group,
                    &ror_lookup,
                ) {
                    writeln!(enriched_writer, "{}", serde_json::to_string(&enriched)?)?;
                    records_enriched += 1;
                }
                current_group.clear();
            }

            current_doi = Some(record.doi.clone());
            current_group.push(record);
        }
    }

    if !current_group.is_empty() {
        if let Some(ref config) = enrichment_config {
            let enrichments = process_doi_group_enrichment(
                current_doi.as_ref().unwrap(),
                &current_group,
                &ror_lookup,
                config,
            );
            for enrichment in &enrichments {
                writeln!(enriched_writer, "{}", serde_json::to_string(enrichment)?)?;
            }
            records_enriched += enrichments.len() as u64;
        } else if let Some(enriched) = process_doi_group(
            current_doi.as_ref().unwrap(),
            &current_group,
            &ror_lookup,
        ) {
            writeln!(enriched_writer, "{}", serde_json::to_string(&enriched)?)?;
            records_enriched += 1;
        }
    }

    progress.finish_with_message("Processing complete");
    enriched_writer.flush()?;
    existing_writer.flush()?;

    let aggregated_file = File::create(output_dir.join("existing_assignments_aggregated.jsonl"))?;
    let mut aggregated_writer = BufWriter::new(aggregated_file);

    for (hash, ror_counts) in &existing_by_hash {
        let affiliation = affiliation_strings.get(hash).cloned().unwrap_or_default();
        for (ror_id, count) in ror_counts {
            let ror_name = ror_names
                .get(ror_id)
                .cloned()
                .unwrap_or_else(|| "Unknown".to_string());

            let agg = ExistingAssignmentAggregated {
                affiliation: affiliation.clone(),
                affiliation_hash: hash.clone(),
                ror_id: ror_id.clone(),
                ror_name,
                count: *count,
            };
            writeln!(aggregated_writer, "{}", serde_json::to_string(&agg)?)?;
        }
    }
    aggregated_writer.flush()?;

    let disagreements_file = File::create(output_dir.join("disagreements.jsonl"))?;
    let mut disagreements_writer = BufWriter::new(disagreements_file);
    let mut user_disagreements = 0u64;
    let mut match_disagreements = 0u64;

    for (hash, ror_counts) in &existing_by_hash {
        let affiliation = affiliation_strings.get(hash).cloned().unwrap_or_default();

        // User disagreement: multiple different ROR IDs for same affiliation
        if ror_counts.len() > 1 {
            let ror_ids: Vec<RorIdCount> = ror_counts
                .iter()
                .map(|(ror_id, count)| RorIdCount {
                    ror_id: ror_id.clone(),
                    ror_name: ror_names
                        .get(ror_id)
                        .cloned()
                        .unwrap_or_else(|| "Unknown".to_string()),
                    count: *count,
                })
                .collect();

            let disagreement = Disagreement::User {
                affiliation: affiliation.clone(),
                affiliation_hash: hash.clone(),
                ror_ids,
            };
            writeln!(disagreements_writer, "{}", serde_json::to_string(&disagreement)?)?;
            user_disagreements += 1;
        }

        // Match disagreement: our match differs from existing assignment(s)
        if let Some(our_match) = ror_lookup.get(hash) {
            for (existing_ror_id, count) in ror_counts {
                if our_match != existing_ror_id {
                    let disagreement = Disagreement::Match {
                        affiliation: affiliation.clone(),
                        affiliation_hash: hash.clone(),
                        existing_ror_id: existing_ror_id.clone(),
                        existing_ror_name: ror_names
                            .get(existing_ror_id)
                            .cloned()
                            .unwrap_or_else(|| "Unknown".to_string()),
                        existing_count: *count,
                        matched_ror_id: our_match.clone(),
                        matched_ror_name: ror_names
                            .get(our_match)
                            .cloned()
                            .unwrap_or_else(|| "Unknown".to_string()),
                    };
                    writeln!(disagreements_writer, "{}", serde_json::to_string(&disagreement)?)?;
                    match_disagreements += 1;
                }
            }
        }
    }
    disagreements_writer.flush()?;

    eprintln!(
        "\nResults:\n  Enriched records: {}\n  Existing assignments: {}\n  User disagreements: {}\n  Match disagreements: {}",
        records_enriched, records_existing, user_disagreements, match_disagreements
    );
    eprintln!(
        "\nOutput files:\n  {:?}\n  {:?}\n  {:?}\n  {:?}",
        output_path,
        output_dir.join("existing_assignments.jsonl"),
        output_dir.join("existing_assignments_aggregated.jsonl"),
        output_dir.join("disagreements.jsonl")
    );

    Ok(())
}
