use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::warn;
use urlencoding::encode;

#[derive(Debug, Deserialize)]
struct RorResponse {
    items: Vec<RorItem>,
}

#[derive(Debug, Deserialize)]
struct RorItem {
    chosen: Option<bool>,
    organization: Option<RorOrganization>,
}

#[derive(Debug, Deserialize)]
struct RorOrganization {
    id: String,
}

pub struct RorClient {
    client: Client,
    base_url: String,
    semaphore: Arc<Semaphore>,
}

impl RorClient {
    pub fn new(base_url: String, concurrency: usize, timeout_secs: u64) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url,
            semaphore: Arc::new(Semaphore::new(concurrency)),
        }
    }

    /// Returns Ok(Some(ror_id)) on match, Ok(None) on no match, Err on failure
    pub async fn query_affiliation(
        &self,
        affiliation: &str,
        fallback_multi: bool,
    ) -> Result<Option<String>> {
        let _permit = self.semaphore.acquire().await?;

        // Phase 1: Try quoted single_search
        let quoted_url = format!(
            "{}/v2/organizations?affiliation=\"{}\"\u{0026}single_search",
            self.base_url,
            encode(affiliation)
        );

        match self.make_request(&quoted_url).await {
            Ok(ror_id) => {
                if ror_id.is_some() {
                    return Ok(ror_id);
                }
            }
            Err(e) if e.to_string().contains("500") => {
                // Phase 2: Retry without quotes on 500
                let unquoted_url = format!(
                    "{}/v2/organizations?affiliation={}\u{0026}single_search",
                    self.base_url,
                    encode(affiliation)
                );

                match self.make_request(&unquoted_url).await {
                    Ok(ror_id) => {
                        if ror_id.is_some() {
                            return Ok(ror_id);
                        }
                    }
                    Err(e) => {
                        if !fallback_multi {
                            return Err(e);
                        }
                    }
                }
            }
            Err(e) => {
                if !fallback_multi {
                    return Err(e);
                }
            }
        }

        // Phase 3: Fallback to standard affiliation endpoint
        if fallback_multi {
            let multi_url = format!(
                "{}/v2/organizations?affiliation=\"{}\"",
                self.base_url,
                encode(affiliation)
            );

            match self.make_request(&multi_url).await {
                Ok(ror_id) => return Ok(ror_id),
                Err(_) => {
                    // Try unquoted multi
                    let unquoted_multi_url = format!(
                        "{}/v2/organizations?affiliation={}",
                        self.base_url,
                        encode(affiliation)
                    );
                    return self.make_request(&unquoted_multi_url).await;
                }
            }
        }

        Ok(None)
    }

    async fn make_request(&self, url: &str) -> Result<Option<String>> {
        let max_retries = 3;

        for attempt in 0..max_retries {
            match self.client.get(url).send().await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        let ror_response: RorResponse = response.json().await?;
                        return Ok(self.extract_chosen_ror_id(&ror_response));
                    } else if status.as_u16() >= 500 {
                        // Return error immediately for 500s - let caller handle fallback strategy
                        return Err(anyhow!("HTTP {}", status));
                    } else if status.as_u16() == 429 {
                        // Rate limited - retry with backoff
                        let wait = response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(2u64.pow(attempt as u32));
                        warn!("Rate limited, waiting {}s", wait);
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                        continue;
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

    fn extract_chosen_ror_id(&self, response: &RorResponse) -> Option<String> {
        response
            .items
            .iter()
            .find(|item| item.chosen == Some(true))
            .and_then(|item| item.organization.as_ref())
            .map(|org| org.id.clone())
    }
}
