use datacite_ror::AuthorAffiliationRecord;
use datacite_ror::EnrichedRecord;
use std::fs::File;
use std::io::{BufRead, Write};
use tempfile::TempDir;

#[test]
fn test_load_ror_matches_builds_hash_map() {
    let temp_dir = TempDir::new().unwrap();
    let matches_file = temp_dir.path().join("ror_matches.jsonl");

    {
        let mut file = File::create(&matches_file).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    let lookup = datacite_ror::reconcile::load_ror_matches(&matches_file).unwrap();

    assert_eq!(lookup.len(), 2);
    assert_eq!(lookup.get("abc123"), Some(&"https://ror.org/052gg0110".to_string()));
    assert_eq!(lookup.get("def456"), Some(&"https://ror.org/042nb2s44".to_string()));
}

#[test]
fn test_load_ror_matches_handles_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let matches_file = temp_dir.path().join("ror_matches.jsonl");
    File::create(&matches_file).unwrap();

    let lookup = datacite_ror::reconcile::load_ror_matches(&matches_file).unwrap();

    assert!(lookup.is_empty());
}

#[test]
fn test_reconcile_full_pipeline() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("output.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    // Create doi_author_affiliations.jsonl
    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            author_idx: 0,
            author_name: "Doe, Jane".to_string(),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            author_idx: 0,
            author_name: "Doe, Jane".to_string(),
            affiliation_idx: 1,
            affiliation: "Unknown Institution".to_string(),
            affiliation_hash: "unknown".to_string(),
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            author_idx: 1,
            author_name: "Smith, John".to_string(),
            affiliation_idx: 0,
            affiliation: "MIT".to_string(),
            affiliation_hash: "def456".to_string(),
            existing_ror_id: None,
        },
        // This author has no matching affiliations - should be excluded
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            author_idx: 2,
            author_name: "No Match Author".to_string(),
            affiliation_idx: 0,
            affiliation: "No Match Univ".to_string(),
            affiliation_hash: "nomatch".to_string(),
            existing_ror_id: None,
        },
    ];

    {
        let file = File::create(input_dir.join("doi_author_affiliations.jsonl")).unwrap();
        let mut writer = std::io::BufWriter::new(file);
        for r in &relationships {
            writeln!(writer, "{}", serde_json::to_string(r).unwrap()).unwrap();
        }
    }

    // Create ror_matches.jsonl
    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    // Run reconcile
    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: output_file.clone(),
    };
    datacite_ror::reconcile::run(args).unwrap();

    // Check output
    assert!(output_file.exists());

    let reader = std::io::BufReader::new(File::open(&output_file).unwrap());
    let records: Vec<EnrichedRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 1); // One DOI
    let record = &records[0];
    assert_eq!(record.doi, "10.1234/test");
    assert_eq!(record.creators.len(), 2); // Only authors with matches

    // First author has one matched affiliation (Oxford), not the unknown one
    assert_eq!(record.creators[0].name, "Doe, Jane");
    assert_eq!(record.creators[0].affiliation.len(), 1);
    assert_eq!(record.creators[0].affiliation[0].name, "University of Oxford");
    assert_eq!(record.creators[0].affiliation[0].affiliation_identifier, "https://ror.org/052gg0110");
    assert_eq!(record.creators[0].affiliation[0].affiliation_identifier_scheme, "ROR");
    assert_eq!(record.creators[0].affiliation[0].scheme_uri, "https://ror.org");

    // Second author
    assert_eq!(record.creators[1].name, "Smith, John");
    assert_eq!(record.creators[1].affiliation[0].affiliation_identifier, "https://ror.org/042nb2s44");
}

#[test]
fn test_reconcile_skips_doi_with_no_matches() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("output.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    // Create relationship with no matching ROR
    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/nomatch".to_string(),
            author_idx: 0,
            author_name: "Author".to_string(),
            affiliation_idx: 0,
            affiliation: "Unknown".to_string(),
            affiliation_hash: "unknown".to_string(),
            existing_ror_id: None,
        },
    ];

    {
        let file = File::create(input_dir.join("doi_author_affiliations.jsonl")).unwrap();
        let mut writer = std::io::BufWriter::new(file);
        for r in &relationships {
            writeln!(writer, "{}", serde_json::to_string(r).unwrap()).unwrap();
        }
    }

    // Empty ror_matches.jsonl
    File::create(input_dir.join("ror_matches.jsonl")).unwrap();

    // Run reconcile
    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: output_file.clone(),
    };
    datacite_ror::reconcile::run(args).unwrap();

    // Output should be empty
    let content = std::fs::read_to_string(&output_file).unwrap();
    assert!(content.trim().is_empty());
}

#[test]
fn test_load_ror_data_builds_name_lookup() {
    let temp_dir = TempDir::new().unwrap();
    let ror_file = temp_dir.path().join("ror_data.json");

    let ror_data = r#"[
        {
            "id": "https://ror.org/052gg0110",
            "names": [
                {"value": "Oxford", "types": ["acronym"], "lang": null},
                {"value": "University of Oxford", "types": ["ror_display", "label"], "lang": "en"}
            ]
        },
        {
            "id": "https://ror.org/042nb2s44",
            "names": [
                {"value": "MIT", "types": ["acronym"], "lang": null},
                {"value": "Massachusetts Institute of Technology", "types": ["ror_display", "label"], "lang": "en"}
            ]
        }
    ]"#;

    std::fs::write(&ror_file, ror_data).unwrap();

    let lookup = datacite_ror::reconcile::load_ror_data(&ror_file).unwrap();

    assert_eq!(lookup.len(), 2);
    assert_eq!(lookup.get("https://ror.org/052gg0110"), Some(&"University of Oxford".to_string()));
    assert_eq!(lookup.get("https://ror.org/042nb2s44"), Some(&"Massachusetts Institute of Technology".to_string()));
}

#[test]
fn test_load_ror_data_handles_missing_ror_display() {
    let temp_dir = TempDir::new().unwrap();
    let ror_file = temp_dir.path().join("ror_data.json");

    // Record with only alias, no ror_display - should fall back to first name
    let ror_data = r#"[
        {
            "id": "https://ror.org/test123",
            "names": [
                {"value": "Test Org", "types": ["alias"], "lang": "en"}
            ]
        }
    ]"#;

    std::fs::write(&ror_file, ror_data).unwrap();

    let lookup = datacite_ror::reconcile::load_ror_data(&ror_file).unwrap();

    assert_eq!(lookup.get("https://ror.org/test123"), Some(&"Test Org".to_string()));
}
