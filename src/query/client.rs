use anyhow::{anyhow, Result};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::warn;
use urlencoding::encode;

#[derive(Debug, Serialize)]
struct BulkRequest<'a> {
    inputs: &'a [String],
}

#[derive(Debug, Deserialize)]
struct BulkResponse {
    message: BulkMessage,
}

#[derive(Debug, Deserialize)]
struct BulkMessage {
    items: Vec<BulkOuterItem>,
}

#[derive(Debug, Deserialize)]
struct BulkOuterItem {
    items: Vec<BulkInnerItem>,
}

#[derive(Debug, Deserialize)]
struct BulkInnerItem {
    id: String,
    confidence: f64,
}

pub struct RorClient {
    client: Client,
    base_url: String,
}

impl RorClient {
    pub fn new(base_url: String, timeout_secs: u64) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("Failed to build HTTP client");

        Self { client, base_url }
    }

    /// POSTs `inputs` to `/match/bulk` and returns one slot per input in input order.
    /// `Some((ror_id, confidence))` when matched (first item wins), `None` for empty
    /// per-input results. Whole-batch errors return `Err`.
    pub async fn match_bulk(
        &self,
        inputs: &[String],
        task: &str,
    ) -> Result<Vec<Option<(String, f64)>>> {
        let url = format!("{}/match/bulk?task={}", self.base_url, encode(task));
        let body = BulkRequest { inputs };

        let max_retries = 2;
        for attempt in 0..max_retries {
            match self.client.post(&url).json(&body).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let parsed: BulkResponse = response.json().await?;
                        if parsed.message.items.len() != inputs.len() {
                            return Err(anyhow!(
                                "Bulk response length mismatch: got {} results for {} inputs",
                                parsed.message.items.len(),
                                inputs.len()
                            ));
                        }
                        return Ok(parsed
                            .message
                            .items
                            .into_iter()
                            .map(|outer| {
                                outer
                                    .items
                                    .into_iter()
                                    .next()
                                    .map(|i| (i.id, i.confidence))
                            })
                            .collect());
                    } else if status == StatusCode::TOO_MANY_REQUESTS {
                        let wait = response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(2u64.pow(attempt as u32));
                        warn!("Rate limited, waiting {}s", wait);
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                        continue;
                    } else if status == StatusCode::PAYLOAD_TOO_LARGE {
                        return Err(anyhow!(
                            "Batch size {} exceeds Marple cap (HTTP 413). Lower --batch-size or raise MARPLE_MAX_BATCH_SIZE.",
                            inputs.len()
                        ));
                    } else if status == StatusCode::NOT_FOUND
                        || status == StatusCode::BAD_REQUEST
                    {
                        let body = response.text().await.unwrap_or_default();
                        return Err(anyhow!("HTTP {}: {}", status, body));
                    } else {
                        return Err(anyhow!("HTTP {}", status));
                    }
                }
                Err(e) => {
                    if attempt < max_retries - 1 {
                        let wait = 2u64.pow(attempt as u32);
                        warn!("Request error, retrying in {}s: {}", wait, e);
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }

        Err(anyhow!("Max retries exceeded"))
    }
}
