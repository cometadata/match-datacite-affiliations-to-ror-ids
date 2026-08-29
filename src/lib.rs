use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

pub mod extract;
pub mod query;
pub mod reconcile;

pub fn hash_affiliation(affiliation: &str) -> String {
    format!("{:016x}", xxh3_64(affiliation.as_bytes()))
}

fn default_creators_field() -> RecordField {
    RecordField::Creators
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordField {
    Creators,
    Contributors,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartyType {
    Creator,
    Contributor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RorAssignmentStatus {
    Unassigned,
    Valid,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidRorReason {
    MissingIdentifier,
    NonStringIdentifier,
    UnrecognizedFormat,
    InvalidChecksum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffiliationOccurrenceRecord {
    pub doi: String,
    pub party_type: PartyType,
    pub party_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party_name_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party_given_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party_family_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party_raw: Option<serde_json::Value>,
    pub affiliation_index: usize,
    pub affiliation: String,
    pub affiliation_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affiliation_raw: Option<serde_json::Value>,
    pub ror_assignment_status: RorAssignmentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_ror_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invalid_ror_reason: Option<InvalidRorReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_ror_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedDataCiteRecord {
    pub doi: String,
    pub occurrences: Vec<AffiliationOccurrenceRecord>,
    pub excluded_zero_length_affiliations: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorAffiliationRecord {
    pub doi: String,
    #[serde(default = "default_creators_field")]
    pub field: RecordField,
    pub idx: usize,
    pub source_raw: serde_json::Value,
    pub affiliation_idx: usize,
    pub affiliation: String,
    pub affiliation_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affiliation_raw: Option<serde_json::Value>,
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
pub struct EnrichedContributor {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub given_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family_name: Option<String>,
    pub contributor_type: String,
    pub affiliation: Vec<EnrichedAffiliation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedRecord {
    pub doi: String,
    pub creators: Vec<EnrichedCreator>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributors: Vec<EnrichedContributor>,
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
#[serde(rename_all = "camelCase")]
pub struct EnrichmentContributor {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name_type: Option<String>,
    pub contributor_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichmentResource {
    pub related_identifier: String,
    pub related_identifier_type: String,
    pub relation_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type_general: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentConfig {
    pub contributors: Vec<EnrichmentContributor>,
    pub resources: Vec<EnrichmentResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichmentOutputRecord {
    pub doi: String,
    pub contributors: Vec<EnrichmentContributor>,
    pub resources: Vec<EnrichmentResource>,
    pub field: String,
    pub action: String,
    pub original_value: serde_json::Value,
    pub enriched_value: serde_json::Value,
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
