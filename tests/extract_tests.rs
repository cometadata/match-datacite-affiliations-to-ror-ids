use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use tempfile::TempDir;
use flate2::write::GzEncoder;
use flate2::Compression;
use datacite_ror::{AuthorAffiliationRecord, RecordField};

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

    create_test_file(temp_dir.path(), "root.jsonl.gz", "{}");
    create_test_file(temp_dir.path(), "subdir/nested.jsonl.gz", "{}");
    create_test_file(temp_dir.path(), "subdir/deep/deeper.jsonl.gz", "{}");
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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 3);

    assert_eq!(affiliations[0].doi, "10.1234/test");
    assert_eq!(affiliations[0].field, RecordField::Creators);
    assert_eq!(affiliations[0].idx, 0);
    assert_eq!(affiliations[0].source_raw["name"], "Doe, Jane");
    assert_eq!(affiliations[0].affiliation_idx, 0);
    assert_eq!(affiliations[0].affiliation, "University of Oxford");
    assert_eq!(affiliations[0].affiliation_hash.len(), 16);

    assert_eq!(affiliations[1].idx, 0);
    assert_eq!(affiliations[1].affiliation, "MIT");

    assert_eq!(affiliations[2].idx, 1);
    assert_eq!(affiliations[2].source_raw["name"], "Smith, John");
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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

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

    let record = r#"{"id":"10.1234/test","attributes":{"doi":"10.1234/test","creators":[{"name":"Doe, Jane","affiliation":[{"name":"University of Oxford"},{"name":"MIT"}]},{"name":"Smith, John","affiliation":[{"name":"MIT"}]}]}}"#;
    create_test_file(&input_dir, "test.jsonl.gz", record);

    let args = datacite_ror::extract::ExtractArgs {
        input: input_dir,
        output: output_dir.clone(),
        threads: 1,
        batch_size: 100,
    };
    datacite_ror::extract::run(args).unwrap();

    let affiliations_file = output_dir.join("unique_affiliations.json");
    assert!(affiliations_file.exists());
    let affiliations: Vec<String> = serde_json::from_reader(
        File::open(&affiliations_file).unwrap()
    ).unwrap();
    assert_eq!(affiliations.len(), 2);
    assert!(affiliations.contains(&"University of Oxford".to_string()));
    assert!(affiliations.contains(&"MIT".to_string()));

    let relationships_file = output_dir.join("doi_author_affiliations.jsonl");
    assert!(relationships_file.exists());

    let reader = BufReader::new(File::open(&relationships_file).unwrap());
    let records: Vec<AuthorAffiliationRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 3);
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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0].existing_ror_id, Some("https://ror.org/052gg0110".to_string()));
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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0].existing_ror_id, None);
}

#[test]
fn test_parse_preserves_name_identifiers() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Doe, Jane",
                    "givenName": "Jane",
                    "familyName": "Doe",
                    "nameIdentifiers": [
                        {
                            "nameIdentifier": "0000-0001-2345-6789",
                            "nameIdentifierScheme": "ORCID",
                            "schemeUri": "https://orcid.org"
                        }
                    ],
                    "affiliation": [
                        {"name": "University of Oxford"}
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 1);
    let name_ids = affiliations[0].source_raw["nameIdentifiers"].as_array().unwrap();
    assert_eq!(name_ids.len(), 1);
    assert_eq!(name_ids[0]["nameIdentifier"], "0000-0001-2345-6789");
    assert_eq!(name_ids[0]["nameIdentifierScheme"], "ORCID");
}

#[test]
fn test_parse_preserves_full_affiliation_object() {
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
                            "affiliationIdentifier": "https://isni.org/isni/0000000121901201",
                            "affiliationIdentifierScheme": "ISNI",
                            "schemeUri": "https://isni.org"
                        }
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 1);
    let raw = affiliations[0].affiliation_raw.as_ref().unwrap();
    assert_eq!(raw["affiliationIdentifier"], "https://isni.org/isni/0000000121901201");
    assert_eq!(raw["affiliationIdentifierScheme"], "ISNI");
    assert_eq!(raw["schemeUri"], "https://isni.org");
}

#[test]
fn test_parse_preserves_source_raw() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
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
                    ],
                    "affiliation": [
                        {"name": "University of Oxford"}
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 1);
    let raw = &affiliations[0].source_raw;
    assert_eq!(raw["name"], "Doe, Jane");
    assert_eq!(raw["nameType"], "Personal");
    assert_eq!(raw["givenName"], "Jane");
    assert_eq!(raw["familyName"], "Doe");
    assert_eq!(raw["lang"], "en");
    assert!(raw["nameIdentifiers"].is_array());
    // affiliation should be stripped from source_raw to avoid redundancy
    assert!(raw.get("affiliation").is_none());
}

#[test]
fn test_parse_datacite_record_extracts_contributor_affiliations() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [],
            "contributors": [
                {
                    "name": "Doe, Jane",
                    "givenName": "Jane",
                    "familyName": "Doe",
                    "contributorType": "Supervisor",
                    "affiliation": [
                        {"name": "University of Oxford"}
                    ]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 1);
    assert_eq!(affiliations[0].field, RecordField::Contributors);
    assert_eq!(affiliations[0].source_raw["name"], "Doe, Jane");
    assert_eq!(affiliations[0].source_raw["contributorType"], "Supervisor");
    assert_eq!(affiliations[0].affiliation, "University of Oxford");

    assert!(affiliations[0].source_raw.get("affiliation").is_none());
}

#[test]
fn test_parse_datacite_record_extracts_both_creators_and_contributors() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [
                {
                    "name": "Creator One",
                    "affiliation": [{"name": "Harvard University"}]
                }
            ],
            "contributors": [
                {
                    "name": "Contributor One",
                    "contributorType": "Editor",
                    "affiliation": [{"name": "Stanford University"}]
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 2);

    assert_eq!(affiliations[0].field, RecordField::Creators);
    assert_eq!(affiliations[0].idx, 0);
    assert_eq!(affiliations[0].source_raw["name"], "Creator One");

    assert_eq!(affiliations[1].field, RecordField::Contributors);
    assert_eq!(affiliations[1].idx, 0);
    assert_eq!(affiliations[1].source_raw["name"], "Contributor One");
    assert_eq!(affiliations[1].source_raw["contributorType"], "Editor");
}

#[test]
fn test_parse_contributor_without_affiliations_is_skipped() {
    let record_json = r#"{
        "id": "10.1234/test",
        "attributes": {
            "doi": "10.1234/test",
            "creators": [],
            "contributors": [
                {
                    "name": "No Affiliation Contributor",
                    "contributorType": "Editor"
                }
            ]
        }
    }"#;

    let record: serde_json::Value = serde_json::from_str(record_json).unwrap();
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert!(affiliations.is_empty());
}
