use crate::{hash_affiliation, AuthorAffiliationRecord};
use serde_json::Value;

fn extract_doi(record: &Value) -> Option<String> {
    record
        .get("id")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            record
                .pointer("/attributes/doi")
                .and_then(Value::as_str)
                .map(String::from)
        })
}

fn extract_author_name(creator: &Value) -> Option<String> {
    creator.get("name").and_then(Value::as_str).map(String::from)
}

/// Handles both object format {"name": "..."} and plain string format
fn extract_affiliation_name(affiliation: &Value) -> Option<String> {
    match affiliation {
        Value::String(s) => Some(s.clone()),
        Value::Object(_) => affiliation
            .get("name")
            .and_then(Value::as_str)
            .map(String::from),
        _ => None,
    }
}

fn extract_existing_ror_id(affiliation: &Value) -> Option<String> {
    match affiliation {
        Value::Object(_) => {
            let scheme = affiliation
                .get("affiliationIdentifierScheme")
                .and_then(Value::as_str)?;
            if scheme.eq_ignore_ascii_case("ROR") {
                affiliation
                    .get("affiliationIdentifier")
                    .and_then(Value::as_str)
                    .map(String::from)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn parse_affiliations(record: &Value) -> Vec<AuthorAffiliationRecord> {
    let mut results = Vec::new();

    let doi = match extract_doi(record) {
        Some(d) => d,
        None => return results,
    };

    let creators = match record.pointer("/attributes/creators") {
        Some(Value::Array(arr)) => arr,
        _ => return results,
    };

    for (author_idx, creator) in creators.iter().enumerate() {
        let author_name = match extract_author_name(creator) {
            Some(n) => n,
            None => continue,
        };

        let author_name_type = creator
            .get("nameType")
            .and_then(Value::as_str)
            .map(String::from);
        let author_given_name = creator
            .get("givenName")
            .and_then(Value::as_str)
            .map(String::from);
        let author_family_name = creator
            .get("familyName")
            .and_then(Value::as_str)
            .map(String::from);
        let author_name_identifiers: Option<Vec<serde_json::Value>> = creator
            .get("nameIdentifiers")
            .and_then(Value::as_array)
            .cloned();

        let affiliations = match creator.get("affiliation") {
            Some(Value::Array(arr)) => arr,
            _ => continue,
        };

        for (affiliation_idx, affiliation) in affiliations.iter().enumerate() {
            if let Some(affiliation_name) = extract_affiliation_name(affiliation) {
                if !affiliation_name.is_empty() {
                    let affiliation_raw = match affiliation {
                        Value::Object(_) => Some(affiliation.clone()),
                        _ => None,
                    };
                    results.push(AuthorAffiliationRecord {
                        doi: doi.clone(),
                        author_idx,
                        author_name: author_name.clone(),
                        author_name_type: author_name_type.clone(),
                        author_given_name: author_given_name.clone(),
                        author_family_name: author_family_name.clone(),
                        author_name_identifiers: author_name_identifiers.clone(),
                        affiliation_idx,
                        affiliation: affiliation_name.clone(),
                        affiliation_hash: hash_affiliation(&affiliation_name),
                        affiliation_raw,
                        existing_ror_id: extract_existing_ror_id(affiliation),
                    });
                }
            }
        }
    }

    results
}
