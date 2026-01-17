use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use tempfile::TempDir;
use flate2::write::GzEncoder;
use flate2::Compression;
use datacite_ror::AuthorAffiliationRecord;

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
    let affiliations = datacite_ror::extract::parse_affiliations(&record);

    assert_eq!(affiliations.len(), 3);

    // Check first author, first affiliation
    assert_eq!(affiliations[0].doi, "10.1234/test");
    assert_eq!(affiliations[0].author_idx, 0);
    assert_eq!(affiliations[0].author_name, "Doe, Jane");
    assert_eq!(affiliations[0].affiliation_idx, 0);
    assert_eq!(affiliations[0].affiliation, "University of Oxford");
    assert_eq!(affiliations[0].affiliation_hash.len(), 16);

    // Check first author, second affiliation
    assert_eq!(affiliations[1].author_idx, 0);
    assert_eq!(affiliations[1].affiliation, "MIT");

    // Check second author
    assert_eq!(affiliations[2].author_idx, 1);
    assert_eq!(affiliations[2].author_name, "Smith, John");
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

    // Create test DataCite record
    let record = r#"{"id":"10.1234/test","attributes":{"doi":"10.1234/test","creators":[{"name":"Doe, Jane","affiliation":[{"name":"University of Oxford"},{"name":"MIT"}]},{"name":"Smith, John","affiliation":[{"name":"MIT"}]}]}}"#;
    create_test_file(&input_dir, "test.jsonl.gz", record);

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
    let affiliations: Vec<String> = serde_json::from_reader(
        File::open(&affiliations_file).unwrap()
    ).unwrap();
    assert_eq!(affiliations.len(), 2); // Oxford and MIT (deduplicated)
    assert!(affiliations.contains(&"University of Oxford".to_string()));
    assert!(affiliations.contains(&"MIT".to_string()));

    // Check doi_author_affiliations.jsonl exists
    let relationships_file = output_dir.join("doi_author_affiliations.jsonl");
    assert!(relationships_file.exists());

    let reader = BufReader::new(File::open(&relationships_file).unwrap());
    let records: Vec<AuthorAffiliationRecord> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(records.len(), 3); // 3 author-affiliation pairs
}
