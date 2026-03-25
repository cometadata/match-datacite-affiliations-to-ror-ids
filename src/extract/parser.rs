use crate::{hash_affiliation, AuthorAffiliationRecord, RecordField};
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

fn extract_person_affiliations(
    doi: &str,
    persons: &[Value],
    field: RecordField,
) -> Vec<AuthorAffiliationRecord> {
    let mut results = Vec::new();

    for (idx, person) in persons.iter().enumerate() {
        if person.get("name").and_then(Value::as_str).is_none() {
            continue;
        }

        let source_raw = {
            let mut raw = person.clone();
            if let Some(obj) = raw.as_object_mut() {
                obj.remove("affiliation");
            }
            raw
        };

        let affiliations = match person.get("affiliation") {
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
                        doi: doi.to_string(),
                        field: field.clone(),
                        idx,
                        source_raw: source_raw.clone(),
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

pub fn parse_affiliations(record: &Value) -> Vec<AuthorAffiliationRecord> {
    let doi = match extract_doi(record) {
        Some(d) => d,
        None => return Vec::new(),
    };

    let mut results = Vec::new();

    if let Some(Value::Array(creators)) = record.pointer("/attributes/creators") {
        results.extend(extract_person_affiliations(&doi, creators, RecordField::Creators));
    }

    if let Some(Value::Array(contributors)) = record.pointer("/attributes/contributors") {
        results.extend(extract_person_affiliations(&doi, contributors, RecordField::Contributors));
    }

    results
}
