use datacite_ror::RorMatch;
use std::fs::{self, File};
use std::io::BufRead;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_query_ror_single_search_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/organizations"))
        .and(query_param("single_search", ""))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "chosen": true,
                    "organization": {
                        "id": "https://ror.org/052gg0110"
                    }
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(
        mock_server.uri(),
        50,
        30,
    );

    let result = client.query_affiliation("University of Oxford", false).await;

    assert!(result.is_ok());
    let ror_id = result.unwrap();
    assert_eq!(ror_id, Some("https://ror.org/052gg0110".to_string()));
}

#[tokio::test]
async fn test_query_ror_no_match_returns_none() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/organizations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": []
        })))
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(
        mock_server.uri(),
        50,
        30,
    );

    let result = client.query_affiliation("Unknown Institution", false).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[tokio::test]
async fn test_query_ror_retry_on_500() {
    let mock_server = MockServer::start().await;

    // First request with quotes returns 500, second without quotes succeeds
    Mock::given(method("GET"))
        .and(path("/v2/organizations"))
        .and(query_param("affiliation", "\"Test University\""))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/organizations"))
        .and(query_param("affiliation", "Test University"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "chosen": true,
                    "organization": {
                        "id": "https://ror.org/abc123"
                    }
                }
            ]
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = datacite_ror::query::RorClient::new(
        mock_server.uri(),
        50,
        30,
    );

    let result = client.query_affiliation("Test University", false).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("https://ror.org/abc123".to_string()));
}

#[test]
fn test_checkpoint_save_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("test.checkpoint");

    let mut checkpoint = datacite_ror::query::Checkpoint::new(&checkpoint_path);

    // Add some hashes
    checkpoint.mark_processed("abc123");
    checkpoint.mark_processed("def456");
    checkpoint.mark_processed("ghi789");

    // Save
    checkpoint.save().unwrap();

    // Load in new instance
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

    // Create unique_affiliations.json
    let affiliations = vec!["University of Oxford", "MIT"];
    let affiliations_file = input_dir.join("unique_affiliations.json");
    serde_json::to_writer(File::create(&affiliations_file).unwrap(), &affiliations).unwrap();

    // Start mock server
    let mock_server = MockServer::start().await;

    // Mock responses
    Mock::given(method("GET"))
        .and(path("/v2/organizations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "chosen": true,
                    "organization": {
                        "id": "https://ror.org/test123"
                    }
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Run query
    let args = datacite_ror::query::QueryArgs {
        input: input_dir,
        output: output_dir.clone(),
        base_url: mock_server.uri(),
        concurrency: 2,
        timeout: 5,
        resume: false,
        fallback_multi: false,
    };

    datacite_ror::query::run_async(args).await.unwrap();

    // Check ror_matches.jsonl exists
    let matches_file = output_dir.join("ror_matches.jsonl");
    assert!(matches_file.exists());

    let reader = std::io::BufReader::new(File::open(&matches_file).unwrap());
    let matches: Vec<RorMatch> = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter_map(|l| serde_json::from_str(&l).ok())
        .collect();

    assert_eq!(matches.len(), 2);

    // Check checkpoint exists
    let checkpoint_file = output_dir.join("ror_matches.checkpoint");
    assert!(checkpoint_file.exists());
}
