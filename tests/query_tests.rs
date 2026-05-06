use datacite_ror::RorMatch;
use std::fs::{self, File};
use std::io::BufRead;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_query_marple_match_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/match"))
        .and(query_param("task", "affiliation"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": {
                "items": [
                    {
                        "id": "https://ror.org/052gg0110",
                        "confidence": 0.92,
                        "strategies": ["affiliation-single-search"]
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(mock_server.uri(), 50, 30);

    let result = client
        .query_affiliation("University of Oxford", "affiliation")
        .await;

    assert!(result.is_ok());
    let matched = result.unwrap();
    assert_eq!(
        matched,
        Some(("https://ror.org/052gg0110".to_string(), 0.92))
    );
}

#[tokio::test]
async fn test_query_marple_no_match_returns_none() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": { "items": [] }
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(mock_server.uri(), 50, 30);

    let result = client
        .query_affiliation("Unknown Institution", "affiliation")
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[test]
fn test_checkpoint_save_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("test.checkpoint");

    let mut checkpoint = datacite_ror::query::Checkpoint::new(&checkpoint_path);

    checkpoint.mark_processed("abc123");
    checkpoint.mark_processed("def456");
    checkpoint.mark_processed("ghi789");

    checkpoint.save().unwrap();

    let loaded = datacite_ror::query::Checkpoint::load(&checkpoint_path).unwrap();

    assert!(loaded.is_processed("abc123"));
    assert!(loaded.is_processed("def456"));
    assert!(loaded.is_processed("ghi789"));
    assert!(!loaded.is_processed("unknown"));
}

#[test]
fn test_checkpoint_load_nonexistent_returns_empty() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("nonexistent.checkpoint");

    let checkpoint = datacite_ror::query::Checkpoint::load(&checkpoint_path).unwrap();

    assert!(!checkpoint.is_processed("anything"));
}

#[tokio::test]
async fn test_query_full_pipeline() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    let affiliations = vec!["University of Oxford", "MIT"];
    let affiliations_file = input_dir.join("unique_affiliations.json");
    serde_json::to_writer(File::create(&affiliations_file).unwrap(), &affiliations).unwrap();

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": {
                "items": [
                    {
                        "id": "https://ror.org/test123",
                        "confidence": 0.85,
                        "strategies": ["affiliation-single-search"]
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let args = datacite_ror::query::QueryArgs {
        input: input_dir,
        output: output_dir.clone(),
        base_url: mock_server.uri(),
        task: "affiliation".to_string(),
        concurrency: 2,
        timeout: 5,
        resume: false,
    };

    datacite_ror::query::run_async(args).await.unwrap();

    let matches_file = output_dir.join("ror_matches.jsonl");
    assert!(matches_file.exists());

    let reader = std::io::BufReader::new(File::open(&matches_file).unwrap());
    let matches: Vec<RorMatch> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(matches.len(), 2);
    for m in &matches {
        assert_eq!(m.ror_id, "https://ror.org/test123");
        assert_eq!(m.confidence, 0.85);
    }

    let checkpoint_file = output_dir.join("ror_matches.checkpoint");
    assert!(checkpoint_file.exists());
}
