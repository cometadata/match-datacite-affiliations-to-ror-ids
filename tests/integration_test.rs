use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn create_test_datacite_file(dir: &std::path::Path, name: &str, content: &str) {
    let file_path = dir.join(name);
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let file = File::create(&file_path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(content.as_bytes()).unwrap();
    encoder.finish().unwrap();
}

#[tokio::test]
async fn test_full_pipeline_extract_query_reconcile() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let work_dir = temp_dir.path().join("work");
    let output_file = temp_dir.path().join("enriched.jsonl");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&work_dir).unwrap();

    let records = vec![
        r#"{"id":"10.1234/paper1","attributes":{"doi":"10.1234/paper1","creators":[{"name":"Author One","nameIdentifiers":[{"nameIdentifier":"0000-0001-0000-0001","nameIdentifierScheme":"ORCID","schemeUri":"https://orcid.org"}],"affiliation":[{"name":"Harvard University"}]}],"contributors":[{"name":"Editor One","contributorType":"Editor","affiliation":[{"name":"Harvard University"}]}]}}"#,
        r#"{"id":"10.1234/paper2","attributes":{"doi":"10.1234/paper2","creators":[{"name":"Author Two","affiliation":[{"name":"Stanford University"}]},{"name":"Author Three","affiliation":[{"name":"Harvard University"}]}]}}"#,
    ];
    create_test_datacite_file(&data_dir, "test.jsonl.gz", &records.join("\n"));

    let extract_args = datacite_ror::extract::ExtractArgs {
        input: data_dir.clone(),
        output: work_dir.clone(),
        threads: 1,
        batch_size: 100,
    };
    datacite_ror::extract::run(extract_args).unwrap();

    assert!(work_dir.join("unique_affiliations.json").exists());
    assert!(work_dir.join("doi_affiliation_occurrences.jsonl").exists());
    assert!(work_dir.join("dois.jsonl").exists());
    assert!(work_dir.join("extraction_report.json").exists());

    let affiliations: Vec<String> =
        serde_json::from_reader(File::open(work_dir.join("unique_affiliations.json")).unwrap())
            .unwrap();
    assert_eq!(affiliations.len(), 2);

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/organizations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "chosen": true,
                    "organization": {
                        "id": "https://ror.org/03vek6s52"
                    }
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    let query_args = datacite_ror::query::QueryArgs {
        input: work_dir.clone(),
        output: work_dir.clone(),
        base_url: mock_server.uri(),
        concurrency: 2,
        timeout: 5,
        resume: false,
        fallback_multi: false,
    };
    datacite_ror::query::run_async(query_args).await.unwrap();

    assert!(work_dir.join("ror_matches.jsonl").exists());
    assert!(work_dir.join("ror_matches.checkpoint").exists());

    let ror_data_file = temp_dir.path().join("ror_data.json");
    let ror_data = r#"[
        {"id": "https://ror.org/03vek6s52", "names": [{"value": "Harvard University", "types": ["ror_display"], "lang": "en"}]},
        {"id": "https://ror.org/00f54p054", "names": [{"value": "Stanford University", "types": ["ror_display"], "lang": "en"}]}
    ]"#;
    fs::write(&ror_data_file, ror_data).unwrap();

    let reconcile_args = datacite_ror::reconcile::ReconcileArgs {
        input: work_dir.clone(),
        output: Some(output_file.clone()),
        ror_data: ror_data_file,
        enrichment_format: false,
        enrichment_config: None,
    };
    datacite_ror::reconcile::run(reconcile_args).unwrap();

    assert!(output_file.exists());

    let content = fs::read_to_string(&output_file).unwrap();
    let records: Vec<datacite_ror::EnrichedRecord> = content
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();

    assert!(!records.is_empty());

    for record in &records {
        for creator in &record.creators {
            for affiliation in &creator.affiliation {
                assert_eq!(affiliation.affiliation_identifier_scheme, "ROR");
                assert_eq!(affiliation.scheme_uri, "https://ror.org");
                assert!(affiliation
                    .affiliation_identifier
                    .starts_with("https://ror.org/"));
            }
        }
    }

    // Canonical reconciliation is intentionally creator-only; contributor placements
    // remain in the occurrence stream for aggregation but do not create enrichment.
    let paper1: serde_json::Value = content
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|record: &serde_json::Value| record["doi"] == "10.1234/paper1")
        .unwrap();
    assert!(paper1.get("contributors").is_none());
}
