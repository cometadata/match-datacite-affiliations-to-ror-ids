use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

pub mod extract;
pub mod query;
pub mod reconcile;

pub fn hash_affiliation(affiliation: &str) -> String {
    format!("{:016x}", xxh3_64(affiliation.as_bytes()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorAffiliationRecord {
    pub doi: String,
    pub author_idx: usize,
    pub author_name: String,
    pub affiliation_idx: usize,
    pub affiliation: String,
    pub affiliation_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_ror_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorMatch {
    pub affiliation: String,
    pub affiliation_hash: String,
    pub ror_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorMatchFailed {
    pub affiliation: String,
    pub affiliation_hash: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichedAffiliation {
    pub name: String,
    pub affiliation_identifier: String,
    pub affiliation_identifier_scheme: String,
    pub scheme_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedCreator {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub given_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family_name: Option<String>,
    pub affiliation: Vec<EnrichedAffiliation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedRecord {
    pub doi: String,
    pub creators: Vec<EnrichedCreator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExistingAssignment {
    pub doi: String,
    pub author_idx: usize,
    pub author_name: String,
    pub affiliation: String,
    pub ror_id: String,
    pub ror_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExistingAssignmentAggregated {
    pub affiliation: String,
    pub affiliation_hash: String,
    pub ror_id: String,
    pub ror_name: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorIdCount {
    pub ror_id: String,
    pub ror_name: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Disagreement {
    User {
        affiliation: String,
        affiliation_hash: String,
        ror_ids: Vec<RorIdCount>,
    },
    Match {
        affiliation: String,
        affiliation_hash: String,
        existing_ror_id: String,
        existing_ror_name: String,
        existing_count: usize,
        matched_ror_id: String,
        matched_ror_name: String,
    },
}
