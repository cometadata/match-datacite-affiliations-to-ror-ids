use datacite_ror::AuthorAffiliationRecord;
use datacite_ror::Disagreement;
use datacite_ror::EnrichedRecord;
use datacite_ror::ExistingAssignment;
use datacite_ror::ExistingAssignmentAggregated;
use datacite_ror::RecordField;
use std::fs::File;
use std::io::{BufRead, Write};
use tempfile::TempDir;

fn create_minimal_ror_data(dir: &std::path::Path) -> std::path::PathBuf {
    let ror_file = dir.join("ror_data.json");
    let ror_data = r#"[
        {"id": "https://ror.org/052gg0110", "names": [{"value": "University of Oxford", "types": ["ror_display"], "lang": "en"}]},
        {"id": "https://ror.org/042nb2s44", "names": [{"value": "Massachusetts Institute of Technology", "types": ["ror_display"], "lang": "en"}]}
    ]"#;
    std::fs::write(&ror_file, ror_data).unwrap();
    ror_file
}

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

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Doe, Jane"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: None,
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Doe, Jane"}),
            affiliation_idx: 1,
            affiliation: "Unknown Institution".to_string(),
            affiliation_hash: "unknown".to_string(),
            affiliation_raw: None,
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 1,
            source_raw: serde_json::json!({"name": "Smith, John"}),
            affiliation_idx: 0,
            affiliation: "MIT".to_string(),
            affiliation_hash: "def456".to_string(),
            affiliation_raw: None,
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 2,
            source_raw: serde_json::json!({"name": "No Match Author"}),
            affiliation_idx: 0,
            affiliation: "No Match Univ".to_string(),
            affiliation_hash: "nomatch".to_string(),
            affiliation_raw: None,
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    assert!(output_file.exists());

    let reader = std::io::BufReader::new(File::open(&output_file).unwrap());
    let records: Vec<EnrichedRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.doi, "10.1234/test");
    assert_eq!(record.creators.len(), 2);
    assert_eq!(record.creators[0].name, "Doe, Jane");
    assert_eq!(record.creators[0].affiliation.len(), 1);
    assert_eq!(record.creators[0].affiliation[0].name, "University of Oxford");
    assert_eq!(record.creators[0].affiliation[0].affiliation_identifier, "https://ror.org/052gg0110");
    assert_eq!(record.creators[0].affiliation[0].affiliation_identifier_scheme, "ROR");
    assert_eq!(record.creators[0].affiliation[0].scheme_uri, "https://ror.org");

    assert_eq!(record.creators[1].name, "Smith, John");
    assert_eq!(record.creators[1].affiliation[0].affiliation_identifier, "https://ror.org/042nb2s44");
}

#[test]
fn test_reconcile_skips_doi_with_no_matches() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("output.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/nomatch".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Author"}),
            affiliation_idx: 0,
            affiliation: "Unknown".to_string(),
            affiliation_hash: "unknown".to_string(),
            affiliation_raw: None,
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

    File::create(input_dir.join("ror_matches.jsonl")).unwrap();

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();
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

#[test]
fn test_reconcile_excludes_existing_ror_ids_from_enriched() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("enriched.jsonl");
    let ror_data_file = temp_dir.path().join("ror_data.json");
    std::fs::create_dir_all(&input_dir).unwrap();

    let ror_data = r#"[
        {"id": "https://ror.org/052gg0110", "names": [{"value": "University of Oxford", "types": ["ror_display"], "lang": "en"}]},
        {"id": "https://ror.org/042nb2s44", "names": [{"value": "MIT", "types": ["ror_display"], "lang": "en"}]}
    ]"#;
    std::fs::write(&ror_data_file, ror_data).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Doe, Jane"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: None,
            existing_ror_id: Some("https://ror.org/052gg0110".to_string()),
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 1,
            source_raw: serde_json::json!({"name": "Smith, John"}),
            affiliation_idx: 0,
            affiliation: "MIT".to_string(),
            affiliation_hash: "def456".to_string(),
            affiliation_raw: None,
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    let reader = std::io::BufReader::new(File::open(&output_file).unwrap());
    let records: Vec<EnrichedRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].creators.len(), 1);
    assert_eq!(records[0].creators[0].name, "Smith, John");
    assert_eq!(records[0].creators[0].affiliation[0].name, "MIT");
}

#[test]
fn test_reconcile_writes_existing_assignments() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test1".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Doe, Jane"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: None,
            existing_ror_id: Some("https://ror.org/052gg0110".to_string()),
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test2".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Smith, John"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: None,
            existing_ror_id: Some("https://ror.org/052gg0110".to_string()),
        },
    ];

    {
        let file = File::create(input_dir.join("doi_author_affiliations.jsonl")).unwrap();
        let mut writer = std::io::BufWriter::new(file);
        for r in &relationships {
            writeln!(writer, "{}", serde_json::to_string(r).unwrap()).unwrap();
        }
    }

    File::create(input_dir.join("ror_matches.jsonl")).unwrap();

    let output_file = output_dir.join("enriched.jsonl");
    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    let existing_file = output_dir.join("existing_assignments.jsonl");
    assert!(existing_file.exists());

    let reader = std::io::BufReader::new(File::open(&existing_file).unwrap());
    let records: Vec<ExistingAssignment> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].ror_id, "https://ror.org/052gg0110");
    assert_eq!(records[0].ror_name, "University of Oxford");

    let agg_file = output_dir.join("existing_assignments_aggregated.jsonl");
    assert!(agg_file.exists());

    let reader = std::io::BufReader::new(File::open(&agg_file).unwrap());
    let agg_records: Vec<ExistingAssignmentAggregated> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(agg_records.len(), 1);
    assert_eq!(agg_records[0].count, 2);
}

#[test]
fn test_reconcile_detects_user_disagreements() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();

    let ror_file = temp_dir.path().join("ror_data.json");
    let ror_data = r#"[
        {"id": "https://ror.org/aaa111", "names": [{"value": "Org A", "types": ["ror_display"], "lang": "en"}]},
        {"id": "https://ror.org/bbb222", "names": [{"value": "Org B", "types": ["ror_display"], "lang": "en"}]}
    ]"#;
    std::fs::write(&ror_file, ror_data).unwrap();

    // Same affiliation string assigned to different ROR IDs by different users
    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test1".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Author 1"}),
            affiliation_idx: 0,
            affiliation: "Ambiguous Org".to_string(),
            affiliation_hash: "ambig123".to_string(),
            affiliation_raw: None,
            existing_ror_id: Some("https://ror.org/aaa111".to_string()),
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test2".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Author 2"}),
            affiliation_idx: 0,
            affiliation: "Ambiguous Org".to_string(),
            affiliation_hash: "ambig123".to_string(),
            affiliation_raw: None,
            existing_ror_id: Some("https://ror.org/bbb222".to_string()),
        },
    ];

    {
        let file = File::create(input_dir.join("doi_author_affiliations.jsonl")).unwrap();
        let mut writer = std::io::BufWriter::new(file);
        for r in &relationships {
            writeln!(writer, "{}", serde_json::to_string(r).unwrap()).unwrap();
        }
    }

    File::create(input_dir.join("ror_matches.jsonl")).unwrap();

    let output_file = output_dir.join("enriched.jsonl");
    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file),
        ror_data: ror_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    let disagreements_file = output_dir.join("disagreements.jsonl");
    assert!(disagreements_file.exists());

    let content = std::fs::read_to_string(&disagreements_file).unwrap();
    let disagreement: Disagreement = serde_json::from_str(content.trim()).unwrap();

    match disagreement {
        Disagreement::User { affiliation, ror_ids, .. } => {
            assert_eq!(affiliation, "Ambiguous Org");
            assert_eq!(ror_ids.len(), 2);
        }
        _ => panic!("Expected User disagreement"),
    }
}

#[test]
fn test_reconcile_detects_match_disagreements() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();

    let ror_file = temp_dir.path().join("ror_data.json");
    let ror_data = r#"[
        {"id": "https://ror.org/user_choice", "names": [{"value": "User Choice Org", "types": ["ror_display"], "lang": "en"}]},
        {"id": "https://ror.org/our_match", "names": [{"value": "Our Match Org", "types": ["ror_display"], "lang": "en"}]}
    ]"#;
    std::fs::write(&ror_file, ror_data).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Author"}),
            affiliation_idx: 0,
            affiliation: "Some Org".to_string(),
            affiliation_hash: "some123".to_string(),
            affiliation_raw: None,
            existing_ror_id: Some("https://ror.org/user_choice".to_string()),
        },
    ];

    {
        let file = File::create(input_dir.join("doi_author_affiliations.jsonl")).unwrap();
        let mut writer = std::io::BufWriter::new(file);
        for r in &relationships {
            writeln!(writer, "{}", serde_json::to_string(r).unwrap()).unwrap();
        }
    }

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"Some Org","affiliation_hash":"some123","ror_id":"https://ror.org/our_match"}}"#).unwrap();
    }

    let output_file = output_dir.join("enriched.jsonl");
    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file),
        ror_data: ror_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    let disagreements_file = output_dir.join("disagreements.jsonl");
    let content = std::fs::read_to_string(&disagreements_file).unwrap();
    let disagreement: Disagreement = serde_json::from_str(content.trim()).unwrap();

    match disagreement {
        Disagreement::Match {
            existing_ror_id,
            existing_ror_name,
            matched_ror_id,
            matched_ror_name,
            ..
        } => {
            assert_eq!(existing_ror_id, "https://ror.org/user_choice");
            assert_eq!(existing_ror_name, "User Choice Org");
            assert_eq!(matched_ror_id, "https://ror.org/our_match");
            assert_eq!(matched_ror_name, "Our Match Org");
        }
        _ => panic!("Expected Match disagreement"),
    }
}

#[test]
fn test_reconcile_enrichment_format() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("enrichments.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({
                "name": "Doe, Jane",
                "nameType": "Personal",
                "givenName": "Jane",
                "familyName": "Doe",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0001-2345-6789",
                        "nameIdentifierScheme": "ORCID",
                        "schemeUri": "https://orcid.org"
                    }
                ]
            }),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: Some(serde_json::json!({"name": "University of Oxford"})),
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 1,
            source_raw: serde_json::json!({
                "name": "Smith, John",
                "nameType": "Personal",
                "givenName": "John",
                "familyName": "Smith"
            }),
            affiliation_idx: 0,
            affiliation: "MIT".to_string(),
            affiliation_hash: "def456".to_string(),
            affiliation_raw: Some(serde_json::json!({"name": "MIT"})),
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let config_file = temp_dir.path().join("enrichment_config.yaml");
    std::fs::write(
        &config_file,
        r#"contributors:
  - name: "COMET"
    nameType: "Organizational"
    contributorType: "Producer"
resources:
  - relatedIdentifier: "http://doi.org/10.82461/160e-8q92"
    relatedIdentifierType: "DOI"
    relationType: "IsDocumentedBy"
    resourceTypeGeneral: "Project"
"#,
    )
    .unwrap();

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: true,
        enrichment_config: Some(config_file),
    };
    datacite_ror::reconcile::run(args).unwrap();

    // Should produce one record per author (2 records), not one per DOI
    let content = std::fs::read_to_string(&output_file).unwrap();
    let records: Vec<serde_json::Value> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(records.len(), 2);

    // Validate every record against the DataCite enrichment input schema
    let schema_str = include_str!("../enrichment_input_schema.json");
    let schema: serde_json::Value = serde_json::from_str(schema_str).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();

    for (i, rec) in records.iter().enumerate() {
        let errors: Vec<String> = validator
            .iter_errors(rec)
            .map(|e| format!("  - {} (at {})", e, e.instance_path))
            .collect();
        if !errors.is_empty() {
            panic!(
                "Record {} failed schema validation:\n{}",
                i,
                errors.join("\n")
            );
        }
    }

    let rec = &records[0];
    assert_eq!(rec["doi"], "10.1234/test");
    assert_eq!(rec["field"], "creators");
    assert_eq!(rec["action"], "updateChild");

    assert_eq!(rec["originalValue"]["name"], "Doe, Jane");
    assert_eq!(rec["originalValue"]["nameType"], "Personal");
    assert_eq!(rec["originalValue"]["givenName"], "Jane");
    assert_eq!(rec["originalValue"]["familyName"], "Doe");
    let orig_affs = rec["originalValue"]["affiliation"].as_array().unwrap();
    assert_eq!(orig_affs.len(), 1);
    assert_eq!(orig_affs[0]["name"], "University of Oxford");
    assert!(orig_affs[0].get("affiliationIdentifier").is_none());

    assert_eq!(rec["enrichedValue"]["name"], "Doe, Jane");
    assert_eq!(rec["enrichedValue"]["nameType"], "Personal");
    assert_eq!(rec["enrichedValue"]["givenName"], "Jane");
    assert_eq!(rec["enrichedValue"]["familyName"], "Doe");

    let affiliations = rec["enrichedValue"]["affiliation"].as_array().unwrap();
    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0]["name"], "University of Oxford");
    assert_eq!(affiliations[0]["affiliationIdentifier"], "https://ror.org/052gg0110");
    assert_eq!(affiliations[0]["affiliationIdentifierScheme"], "ROR");
    assert_eq!(affiliations[0]["schemeUri"], "https://ror.org");

    assert!(rec["originalValue"]["nameIdentifiers"].is_array());
    assert!(rec["enrichedValue"]["nameIdentifiers"].is_array());

    let contributors = rec["contributors"].as_array().unwrap();
    assert_eq!(contributors[0]["name"], "COMET");
    assert_eq!(contributors[0]["nameType"], "Organizational");
    assert_eq!(contributors[0]["contributorType"], "Producer");

    let resources = rec["resources"].as_array().unwrap();
    assert_eq!(resources[0]["relatedIdentifier"], "http://doi.org/10.82461/160e-8q92");
    assert_eq!(resources[0]["relatedIdentifierType"], "DOI");

    let rec2 = &records[1];
    assert_eq!(rec2["originalValue"]["name"], "Smith, John");
    assert_eq!(rec2["enrichedValue"]["affiliation"][0]["affiliationIdentifier"], "https://ror.org/042nb2s44");
}

#[test]
fn test_enrichment_format_preserves_name_identifiers() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("enrichments.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({
                "name": "Doe, Jane",
                "nameType": "Personal",
                "givenName": "Jane",
                "familyName": "Doe",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0001-2345-6789",
                        "nameIdentifierScheme": "ORCID",
                        "schemeUri": "https://orcid.org"
                    }
                ]
            }),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: Some(serde_json::json!({
                "name": "University of Oxford",
                "affiliationIdentifier": "https://isni.org/isni/0000000121901201",
                "affiliationIdentifierScheme": "ISNI",
                "schemeUri": "https://isni.org"
            })),
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let config_file = temp_dir.path().join("config.yaml");
    std::fs::write(
        &config_file,
        "contributors:\n  - name: \"Test\"\n    contributorType: \"Producer\"\nresources:\n  - relatedIdentifier: \"http://example.com\"\n    relatedIdentifierType: \"URL\"\n    relationType: \"IsDocumentedBy\"\n",
    ).unwrap();

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: true,
        enrichment_config: Some(config_file),
    };
    datacite_ror::reconcile::run(args).unwrap();

    let content = std::fs::read_to_string(&output_file).unwrap();
    let rec: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

    let orig_name_ids = rec["originalValue"]["nameIdentifiers"].as_array().unwrap();
    assert_eq!(orig_name_ids.len(), 1);
    assert_eq!(orig_name_ids[0]["nameIdentifier"], "0000-0001-2345-6789");
    assert_eq!(orig_name_ids[0]["nameIdentifierScheme"], "ORCID");

    let orig_affs = rec["originalValue"]["affiliation"].as_array().unwrap();
    assert_eq!(orig_affs[0]["name"], "University of Oxford");
    assert_eq!(orig_affs[0]["affiliationIdentifier"], "https://isni.org/isni/0000000121901201");
    assert_eq!(orig_affs[0]["affiliationIdentifierScheme"], "ISNI");

    let enr_name_ids = rec["enrichedValue"]["nameIdentifiers"].as_array().unwrap();
    assert_eq!(enr_name_ids.len(), 1);
    assert_eq!(enr_name_ids[0]["nameIdentifier"], "0000-0001-2345-6789");

    let enr_affs = rec["enrichedValue"]["affiliation"].as_array().unwrap();
    assert_eq!(enr_affs[0]["name"], "University of Oxford");
    assert_eq!(enr_affs[0]["affiliationIdentifier"], "https://ror.org/052gg0110");
    assert_eq!(enr_affs[0]["affiliationIdentifierScheme"], "ROR");
    assert_eq!(enr_affs[0]["schemeUri"], "https://ror.org");
}

#[test]
fn test_enrichment_format_preserves_unmatched_affiliation_identifiers() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("enrichments.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    // Author with two affiliations: one matched, one unmatched with ISNI
    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Doe, Jane"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: Some(serde_json::json!({"name": "University of Oxford"})),
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Doe, Jane"}),
            affiliation_idx: 1,
            affiliation: "Some Lab".to_string(),
            affiliation_hash: "lab456".to_string(),
            affiliation_raw: Some(serde_json::json!({
                "name": "Some Lab",
                "affiliationIdentifier": "https://isni.org/isni/999",
                "affiliationIdentifierScheme": "ISNI",
                "schemeUri": "https://isni.org"
            })),
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let config_file = temp_dir.path().join("config.yaml");
    std::fs::write(
        &config_file,
        "contributors:\n  - name: \"Test\"\n    contributorType: \"Producer\"\nresources:\n  - relatedIdentifier: \"http://example.com\"\n    relatedIdentifierType: \"URL\"\n    relationType: \"IsDocumentedBy\"\n",
    ).unwrap();

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: true,
        enrichment_config: Some(config_file),
    };
    datacite_ror::reconcile::run(args).unwrap();

    let content = std::fs::read_to_string(&output_file).unwrap();
    let rec: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

    // enrichedValue: unmatched affiliation should keep its ISNI identifier
    let enr_affs = rec["enrichedValue"]["affiliation"].as_array().unwrap();
    assert_eq!(enr_affs.len(), 2);

    assert_eq!(enr_affs[0]["affiliationIdentifier"], "https://ror.org/052gg0110");

    assert_eq!(enr_affs[1]["name"], "Some Lab");
    assert_eq!(enr_affs[1]["affiliationIdentifier"], "https://isni.org/isni/999");
    assert_eq!(enr_affs[1]["affiliationIdentifierScheme"], "ISNI");
}

#[test]
fn test_enrichment_format_preserves_unknown_creator_fields() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("enrichments.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({
                "name": "Doe, Jane",
                "nameType": "Personal",
                "givenName": "Jane",
                "familyName": "Doe",
                "lang": "en",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0001-2345-6789",
                        "nameIdentifierScheme": "ORCID",
                        "schemeUri": "https://orcid.org"
                    }
                ]
            }),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: Some(serde_json::json!({"name": "University of Oxford"})),
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let config_file = temp_dir.path().join("config.yaml");
    std::fs::write(
        &config_file,
        "contributors:\n  - name: \"Test\"\n    contributorType: \"Producer\"\nresources:\n  - relatedIdentifier: \"http://example.com\"\n    relatedIdentifierType: \"URL\"\n    relationType: \"IsDocumentedBy\"\n",
    ).unwrap();

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: true,
        enrichment_config: Some(config_file),
    };
    datacite_ror::reconcile::run(args).unwrap();

    let content = std::fs::read_to_string(&output_file).unwrap();
    let rec: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

    // Unknown field "lang" should survive in both originalValue and enrichedValue
    assert_eq!(rec["originalValue"]["lang"], "en");
    assert_eq!(rec["enrichedValue"]["lang"], "en");

    assert_eq!(rec["originalValue"]["name"], "Doe, Jane");
    assert_eq!(rec["enrichedValue"]["name"], "Doe, Jane");
    assert!(rec["originalValue"]["nameIdentifiers"].is_array());
    assert!(rec["enrichedValue"]["nameIdentifiers"].is_array());
}

#[test]
fn test_reconcile_separates_creators_and_contributors() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("output.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Creator One"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: None,
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Contributors,
            idx: 0,
            source_raw: serde_json::json!({"name": "Contributor One", "contributorType": "Editor"}),
            affiliation_idx: 0,
            affiliation: "MIT".to_string(),
            affiliation_hash: "def456".to_string(),
            affiliation_raw: None,
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    let reader = std::io::BufReader::new(File::open(&output_file).unwrap());
    let records: Vec<EnrichedRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 1);
    let record = &records[0];

    assert_eq!(record.creators.len(), 1);
    assert_eq!(record.creators[0].name, "Creator One");

    assert_eq!(record.contributors.len(), 1);
    assert_eq!(record.contributors[0].name, "Contributor One");
    assert_eq!(record.contributors[0].contributor_type, "Editor");
    assert_eq!(record.contributors[0].affiliation[0].affiliation_identifier, "https://ror.org/042nb2s44");
}

#[test]
fn test_reconcile_enrichment_format_uses_correct_field() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("enrichments.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Creators,
            idx: 0,
            source_raw: serde_json::json!({"name": "Creator One"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: Some(serde_json::json!({"name": "University of Oxford"})),
            existing_ror_id: None,
        },
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Contributors,
            idx: 0,
            source_raw: serde_json::json!({
                "name": "Contributor One",
                "contributorType": "Supervisor"
            }),
            affiliation_idx: 0,
            affiliation: "MIT".to_string(),
            affiliation_hash: "def456".to_string(),
            affiliation_raw: Some(serde_json::json!({"name": "MIT"})),
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
        writeln!(file, r#"{{"affiliation":"MIT","affiliation_hash":"def456","ror_id":"https://ror.org/042nb2s44"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let config_file = temp_dir.path().join("config.yaml");
    std::fs::write(
        &config_file,
        "contributors:\n  - name: \"Test\"\n    contributorType: \"Producer\"\nresources:\n  - relatedIdentifier: \"http://example.com\"\n    relatedIdentifierType: \"URL\"\n    relationType: \"IsDocumentedBy\"\n",
    ).unwrap();

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: true,
        enrichment_config: Some(config_file),
    };
    datacite_ror::reconcile::run(args).unwrap();

    let content = std::fs::read_to_string(&output_file).unwrap();
    let records: Vec<serde_json::Value> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(records.len(), 2);

    let creator_rec = records.iter().find(|r| r["field"] == "creators").unwrap();
    let contributor_rec = records.iter().find(|r| r["field"] == "contributors").unwrap();

    assert_eq!(creator_rec["originalValue"]["name"], "Creator One");
    assert_eq!(contributor_rec["originalValue"]["name"], "Contributor One");
    assert_eq!(contributor_rec["originalValue"]["contributorType"], "Supervisor");
    assert_eq!(contributor_rec["enrichedValue"]["contributorType"], "Supervisor");
}

#[test]
fn test_reconcile_contributors_only() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_file = temp_dir.path().join("output.jsonl");
    std::fs::create_dir_all(&input_dir).unwrap();

    let relationships = vec![
        AuthorAffiliationRecord {
            doi: "10.1234/test".to_string(),
            field: RecordField::Contributors,
            idx: 0,
            source_raw: serde_json::json!({"name": "Editor One", "contributorType": "Editor"}),
            affiliation_idx: 0,
            affiliation: "University of Oxford".to_string(),
            affiliation_hash: "abc123".to_string(),
            affiliation_raw: None,
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

    {
        let mut file = File::create(input_dir.join("ror_matches.jsonl")).unwrap();
        writeln!(file, r#"{{"affiliation":"University of Oxford","affiliation_hash":"abc123","ror_id":"https://ror.org/052gg0110"}}"#).unwrap();
    }

    let ror_data_file = create_minimal_ror_data(temp_dir.path());

    let args = datacite_ror::reconcile::ReconcileArgs {
        input: input_dir,
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(args).unwrap();

    let reader = std::io::BufReader::new(File::open(&output_file).unwrap());
    let records: Vec<EnrichedRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    // DOI with only contributors should still produce output
    assert_eq!(records.len(), 1);
    assert!(records[0].creators.is_empty());
    assert_eq!(records[0].contributors.len(), 1);
    assert_eq!(records[0].contributors[0].name, "Editor One");
    assert_eq!(records[0].contributors[0].contributor_type, "Editor");
}
