use datacite_ror::RorMatch;
use std::fs::{self, File};
use std::io::BufRead;
use tempfile::TempDir;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_match_bulk_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .and(query_param("task", "affiliation"))
        .and(body_json(serde_json::json!({
            "inputs": ["University of Oxford"]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": {
                "items": [
                    {
                        "items": [
                            {
                                "id": "https://ror.org/052gg0110",
                                "confidence": 0.92,
                                "strategies": ["affiliation-single-search"]
                            }
                        ],
                        "target_data": "ROR v1.67",
                        "strategy": "affiliation-single-search"
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(mock_server.uri(), 30);

    let result = client
        .match_bulk(&["University of Oxford".to_string()], "affiliation")
        .await;

    assert!(result.is_ok());
    let matched = result.unwrap();
    assert_eq!(
        matched,
        vec![Some(("https://ror.org/052gg0110".to_string(), 0.92))]
    );
}

#[tokio::test]
async fn test_match_bulk_no_match_returns_none_per_slot() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": {
                "items": [
                    { "items": [], "target_data": "ROR v1.67", "strategy": "affiliation-single-search" }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(mock_server.uri(), 30);

    let result = client
        .match_bulk(&["Unknown Institution".to_string()], "affiliation")
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), vec![None]);
}

#[tokio::test]
async fn test_match_bulk_preserves_order() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": {
                "items": [
                    {
                        "items": [
                            {
                                "id": "https://ror.org/aaaaaaaaa",
                                "confidence": 0.91,
                                "strategies": ["s"]
                            }
                        ],
                        "target_data": "ROR v1.67",
                        "strategy": "s"
                    },
                    { "items": [], "target_data": "ROR v1.67", "strategy": "s" }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(mock_server.uri(), 30);

    let result = client
        .match_bulk(
            &["Matched Org".to_string(), "Unknown".to_string()],
            "affiliation",
        )
        .await;

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        vec![
            Some(("https://ror.org/aaaaaaaaa".to_string(), 0.91)),
            None,
        ]
    );
}

#[tokio::test]
async fn test_match_bulk_413_no_retry() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(413).set_body_string("too big"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(mock_server.uri(), 30);

    let inputs: Vec<String> = (0..200).map(|_| "x".to_string()).collect();
    let result = client.match_bulk(&inputs, "affiliation").await;

    assert!(result.is_err());
    // Mock's `.expect(1)` is verified on drop — if the client retried, the test fails.
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

    Mock::given(method("POST"))
        .and(path("/match/bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": {
                "items": [
                    {
                        "items": [
                            {
                                "id": "https://ror.org/test123",
                                "confidence": 0.85,
                                "strategies": ["affiliation-single-search"]
                            }
                        ],
                        "target_data": "ROR v1.67",
                        "strategy": "affiliation-single-search"
                    },
                    {
                        "items": [
                            {
                                "id": "https://ror.org/test123",
                                "confidence": 0.85,
                                "strategies": ["affiliation-single-search"]
                            }
                        ],
                        "target_data": "ROR v1.67",
                        "strategy": "affiliation-single-search"
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
        batch_size: 50,
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
