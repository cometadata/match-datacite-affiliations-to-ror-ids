use datacite_ror::{AffiliationOccurrenceRecord, PartyType};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use tempfile::TempDir;

// Helper to create test .jsonl.gz file
fn create_test_file(dir: &std::path::Path, name: &str, content: &str) {
    let file_path = dir.join(name);
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let file = File::create(&file_path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(content.as_bytes()).unwrap();
    encoder.finish().unwrap();
}

#[test]
fn test_find_jsonl_gz_files_finds_files_recursively() {
    let temp_dir = TempDir::new().unwrap();

    // Create files at different levels
    create_test_file(temp_dir.path(), "root.jsonl.gz", "{}");
    create_test_file(temp_dir.path(), "subdir/nested.jsonl.gz", "{}");
    create_test_file(temp_dir.path(), "subdir/deep/deeper.jsonl.gz", "{}");
    // Non-matching file
    fs::write(temp_dir.path().join("ignore.txt"), "text").unwrap();

    let files = datacite_ror::extract::find_jsonl_gz_files(temp_dir.path()).unwrap();

    assert_eq!(files.len(), 3);
}

#[test]
fn test_find_jsonl_gz_files_returns_empty_for_no_matches() {
    let temp_dir = TempDir::new().unwrap();
    fs::write(temp_dir.path().join("file.txt"), "text").unwrap();

    let files = datacite_ror::extract::find_jsonl_gz_files(temp_dir.path()).unwrap();

    assert!(files.is_empty());
}

#[test]
fn test_parse_datacite_record_extracts_affiliations() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Doe, Jane",
                    "givenName": "Jane",
                    "familyName": "Doe",
                    "affiliation": [
                        {"name": "University of Oxford"},
                        {"name": "MIT"}
                    ]
                },
                {
                    "name": "Smith, John",
                    "affiliation": [
                        {"name": "Stanford University"}
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let parsed = datacite_ror::extract::parse_affiliations(&record).unwrap();
    let affiliations = parsed.occurrences;

    assert_eq!(affiliations.len(), 3);

    // Check first author, first affiliation
    assert_eq!(affiliations[0].doi, "10.1234/test");
    assert_eq!(affiliations[0].party_index, 0);
    assert_eq!(affiliations[0].party_name.as_deref(), Some("Doe, Jane"));
    assert_eq!(affiliations[0].affiliation_index, 0);
    assert_eq!(affiliations[0].affiliation, "University of Oxford");
    assert_eq!(affiliations[0].affiliation_hash.len(), 16);

    // Check first author, second affiliation
    assert_eq!(affiliations[1].party_index, 0);
    assert_eq!(affiliations[1].affiliation, "MIT");

    // Check second author
    assert_eq!(affiliations[2].party_index, 1);
    assert_eq!(affiliations[2].party_name.as_deref(), Some("Smith, John"));
    assert_eq!(affiliations[2].affiliation, "Stanford University");
}

#[test]
fn test_parse_datacite_record_handles_missing_affiliations() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {"name": "No Affiliation Author"}
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record)
        .unwrap()
        .occurrences;

    assert!(affiliations.is_empty());
}

#[test]
fn test_parse_datacite_record_handles_string_affiliation() {
    // Some records have affiliation as plain string instead of object
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Author Name",
                    "affiliation": ["Simple Affiliation String"]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record)
        .unwrap()
        .occurrences;

    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0].affiliation, "Simple Affiliation String");
}

#[test]
fn test_extract_produces_output_files() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    // A creator DOI (with a repeated placement), a contributor-only DOI, and a DOI without affiliations.
    let records = concat!(
        r#"{"id":"10.1234/one","attributes":{"creators":[{"name":"Doe, Jane","affiliation":[{"name":"University of Oxford"},{"name":"MIT"},{"name":"MIT"}]}]}}"#,
        "\n",
        r#"{"id":"10.1234/two","attributes":{"contributors":[{"name":"Contributor, Casey","affiliation":[{"name":"Contributor University"},{"name":"Contributor Institute"}]}]}}"#,
        "\n",
        r#"{"id":"10.1234/three","attributes":{"creators":[{"name":"No Affiliation Author"}]}}"#,
    );
    create_test_file(&input_dir, "test.jsonl.gz", records);

    // Run extract
    let args = datacite_ror::extract::ExtractArgs {
        input: input_dir,
        output: output_dir.clone(),
        threads: 1,
        batch_size: 100,
    };
    datacite_ror::extract::run(args).unwrap();

    // Check unique_affiliations.json exists and has correct content
    let affiliations_file = output_dir.join("unique_affiliations.json");
    assert!(affiliations_file.exists());
    let affiliations: Vec<String> =
        serde_json::from_reader(File::open(&affiliations_file).unwrap()).unwrap();
    assert_eq!(affiliations.len(), 4); // All creator and contributor affiliations, deduplicated
    assert!(affiliations.contains(&"University of Oxford".to_string()));
    assert!(affiliations.contains(&"MIT".to_string()));

    // Check canonical occurrence output preserves every valid placement.
    let occurrences_file = output_dir.join("doi_affiliation_occurrences.jsonl");
    assert!(occurrences_file.exists());

    let reader = BufReader::new(File::open(&occurrences_file).unwrap());
    let occurrence_records: Vec<AffiliationOccurrenceRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    let reader = BufReader::new(File::open(output_dir.join("dois.jsonl")).unwrap());
    let doi_records: Vec<String> = reader
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(&line.unwrap()).unwrap())
        .map(|record| record["doi"].as_str().unwrap().to_string())
        .collect();

    let report: serde_json::Value =
        serde_json::from_reader(File::open(output_dir.join("extraction_report.json")).unwrap())
            .unwrap();

    let occurrence_rows: Vec<(&str, PartyType, usize, Option<&str>, usize, &str)> =
        occurrence_records
            .iter()
            .map(|record| {
                (
                    record.doi.as_str(),
                    record.party_type,
                    record.party_index,
                    record.party_name.as_deref(),
                    record.affiliation_index,
                    record.affiliation.as_str(),
                )
            })
            .collect();
    assert_eq!(
        occurrence_rows,
        vec![
            (
                "10.1234/one",
                PartyType::Creator,
                0,
                Some("Doe, Jane"),
                0,
                "University of Oxford"
            ),
            (
                "10.1234/one",
                PartyType::Creator,
                0,
                Some("Doe, Jane"),
                1,
                "MIT"
            ),
            (
                "10.1234/one",
                PartyType::Creator,
                0,
                Some("Doe, Jane"),
                2,
                "MIT"
            ),
            (
                "10.1234/two",
                PartyType::Contributor,
                0,
                Some("Contributor, Casey"),
                0,
                "Contributor University"
            ),
            (
                "10.1234/two",
                PartyType::Contributor,
                0,
                Some("Contributor, Casey"),
                1,
                "Contributor Institute"
            ),
        ]
    );
    assert_eq!(
        doi_records,
        vec!["10.1234/one", "10.1234/two", "10.1234/three"]
    );
    assert_eq!(report["valid_records"], 3);
    assert_eq!(report["valid_dois"], 3);
    assert_eq!(report["occurrence_count"], 5);
    assert_eq!(report["creator_occurrences"], 3);
    assert_eq!(report["contributor_occurrences"], 2);
}

#[test]
fn test_extract_writes_report_before_returning_malformed_input_error() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&input_dir).unwrap();

    create_test_file(
        &input_dir,
        "test.jsonl.gz",
        concat!(
            "{this is not JSON}\n",
            r#"{"id":"10.1234/valid","attributes":{"creators":[]}}"#,
        ),
    );

    let args = datacite_ror::extract::ExtractArgs {
        input: input_dir,
        output: output_dir.clone(),
        threads: 1,
        batch_size: 100,
    };

    assert!(datacite_ror::extract::run(args).is_err());

    let report: serde_json::Value =
        serde_json::from_reader(File::open(output_dir.join("extraction_report.json")).unwrap())
            .unwrap();
    assert_eq!(report["malformed_json_records"], 1);
}

#[test]
fn test_extract_writes_report_before_returning_late_output_error() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(output_dir.join("unique_affiliations.json")).unwrap();
    create_test_file(
        &input_dir,
        "test.jsonl.gz",
        r#"{"id":"10.1234/valid","attributes":{"creators":[]}}"#,
    );

    let args = datacite_ror::extract::ExtractArgs {
        input: input_dir,
        output: output_dir.clone(),
        threads: 1,
        batch_size: 100,
    };

    assert!(datacite_ror::extract::run(args).is_err());

    let report: serde_json::Value =
        serde_json::from_reader(File::open(output_dir.join("extraction_report.json")).unwrap())
            .unwrap();
    assert_eq!(report["valid_records"], 1);
}

#[test]
fn test_parse_datacite_record_extracts_existing_ror_id() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Doe, Jane",
                    "affiliation": [
                        {
                            "name": "University of Oxford",
                            "affiliationIdentifier": "https://ror.org/052gg0110",
                            "affiliationIdentifierScheme": "ROR"
                        }
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record)
        .unwrap()
        .occurrences;

    assert_eq!(affiliations.len(), 1);
    assert_eq!(
        affiliations[0].existing_ror_id,
        Some("https://ror.org/052gg0110".to_string())
    );
}

#[test]
fn test_parse_datacite_record_none_for_missing_ror_id() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Doe, Jane",
                    "affiliation": [{"name": "MIT"}]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record)
        .unwrap()
        .occurrences;

    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0].existing_ror_id, None);
}

#[test]
fn test_parse_datacite_record_ignores_non_ror_identifier() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Doe, Jane",
                    "affiliation": [
                        {
                            "name": "Some Org",
                            "affiliationIdentifier": "grid.123456.7",
                            "affiliationIdentifierScheme": "GRID"
                        }
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record)
        .unwrap()
        .occurrences;

    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0].existing_ror_id, None);
}

#[test]
fn test_parse_datacite_record_extracts_creator_and_contributor_occurrences() {
    let record: serde_json::Value = serde_json::from_str(r#"{
      "id":"10.1234/all-parties",
      "attributes":{
        "creators":[{"name":"Creator One","affiliation":[{"name":"Shared University"},{"name":"Shared University"}]}],
        "contributors":[{"name":"Contributor One","affiliation":["Shared University",{"name":"Contributor Institute"}]}]
      }
    }"#).unwrap();

    let parsed = datacite_ror::extract::parse_affiliations(&record).unwrap();

    assert_eq!(parsed.doi, "10.1234/all-parties");
    assert_eq!(parsed.occurrences.len(), 4);
    assert_eq!(parsed.occurrences[0].party_type, PartyType::Creator);
    assert_eq!(parsed.occurrences[2].party_type, PartyType::Contributor);
    assert_eq!(parsed.occurrences[2].party_index, 0);
    assert_eq!(parsed.occurrences[3].affiliation, "Contributor Institute");
}

#[test]
fn test_parse_datacite_record_counts_empty_affiliations_without_excluding_whitespace() {
    let record: serde_json::Value = serde_json::from_str(
        r#"{
      "id":"10.1234/empty-affiliations",
      "attributes":{"creators":[{"affiliation":["", "  "]}]}
    }"#,
    )
    .unwrap();

    let parsed = datacite_ror::extract::parse_affiliations(&record).unwrap();

    assert_eq!(parsed.excluded_zero_length_affiliations, 1);
    assert_eq!(parsed.occurrences.len(), 1);
    assert_eq!(parsed.occurrences[0].affiliation, "  ");
}

#[test]
fn test_parse_datacite_record_rejects_malformed_shapes() {
    let missing_doi: serde_json::Value = serde_json::from_str(
        r#"{
      "attributes":{"creators":[]}
    }"#,
    )
    .unwrap();
    assert!(datacite_ror::extract::parse_affiliations(&missing_doi).is_err());

    let non_array_creators: serde_json::Value = serde_json::from_str(
        r#"{
      "id":"10.1234/non-array-creators", "attributes":{"creators":{}}
    }"#,
    )
    .unwrap();
    assert!(datacite_ror::extract::parse_affiliations(&non_array_creators).is_err());

    let non_array_contributors: serde_json::Value = serde_json::from_str(
        r#"{
      "id":"10.1234/non-array-contributors", "attributes":{"contributors":{}}
    }"#,
    )
    .unwrap();
    assert!(datacite_ror::extract::parse_affiliations(&non_array_contributors).is_err());

    let unsupported_affiliation: serde_json::Value = serde_json::from_str(
        r#"{
      "id":"10.1234/unsupported-affiliation",
      "attributes":{"creators":[{"affiliation":[42]}]}
    }"#,
    )
    .unwrap();
    assert!(datacite_ror::extract::parse_affiliations(&unsupported_affiliation).is_err());
}

#[test]
fn test_parse_canonical_occurrence_preserves_raw_party_and_affiliation_payloads() {
    let record: serde_json::Value = serde_json::from_str(
        r#"{
          "id":"10.1234/raw",
          "attributes":{"creators":[{
            "name":"Doe, Jane", "lang":"en",
            "nameIdentifiers":[{"nameIdentifier":"0000-0001-2345-6789","nameIdentifierScheme":"ORCID"}],
            "affiliation":[{"name":"University of Oxford","affiliationIdentifier":"https://isni.org/isni/0000000121901201","affiliationIdentifierScheme":"ISNI"}]
          }]}
        }"#,
    )
    .unwrap();

    let parsed = datacite_ror::extract::parse_affiliations(&record).unwrap();
    let occurrence = &parsed.occurrences[0];
    assert_eq!(occurrence.party_raw.as_ref().unwrap()["lang"], "en");
    assert!(occurrence.party_raw.as_ref().unwrap()["nameIdentifiers"].is_array());
    assert_eq!(
        occurrence.affiliation_raw.as_ref().unwrap()["affiliationIdentifierScheme"],
        "ISNI"
    );
}
