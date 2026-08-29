use super::ror::{parse_explicit_ror, ParsedRorAssignment};
use crate::{
    hash_affiliation, AffiliationOccurrenceRecord, ParsedDataCiteRecord, PartyType,
    RorAssignmentStatus,
};
use anyhow::{anyhow, Result};
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

fn extract_affiliation_name(affiliation: &Value) -> Result<String> {
    match affiliation {
        Value::String(s) => Ok(s.clone()),
        Value::Object(_) => affiliation
            .get("name")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| anyhow!("affiliation objects must contain a string name")),
        _ => Err(anyhow!(
            "affiliations must be strings or objects with a string name"
        )),
    }
}

fn extract_party_occurrences(
    record: &Value,
    doi: &str,
    party_type: PartyType,
    party_pointer: &str,
) -> Result<(Vec<AffiliationOccurrenceRecord>, u64)> {
    let Some(parties) = record.pointer(party_pointer) else {
        return Ok((Vec::new(), 0));
    };
    let parties = parties
        .as_array()
        .ok_or_else(|| anyhow!("{party_pointer} must be an array when present"))?;

    let mut occurrences = Vec::new();
    let mut excluded_zero_length_affiliations = 0;

    for (party_index, party) in parties.iter().enumerate() {
        let party_raw = match party {
            Value::Object(_) => {
                let mut raw = party.clone();
                if let Some(object) = raw.as_object_mut() {
                    object.remove("affiliation");
                }
                Some(raw)
            }
            _ => None,
        };
        let party_name = party.get("name").and_then(Value::as_str).map(String::from);
        let party_name_type = party
            .get("nameType")
            .and_then(Value::as_str)
            .map(String::from);
        let party_given_name = party
            .get("givenName")
            .and_then(Value::as_str)
            .map(String::from);
        let party_family_name = party
            .get("familyName")
            .and_then(Value::as_str)
            .map(String::from);

        let Some(affiliations) = party.get("affiliation") else {
            continue;
        };
        let affiliations = affiliations
            .as_array()
            .ok_or_else(|| anyhow!("affiliation must be an array when present"))?;

        for (affiliation_index, affiliation) in affiliations.iter().enumerate() {
            let affiliation_name = extract_affiliation_name(affiliation)?;
            if affiliation_name.is_empty() {
                excluded_zero_length_affiliations += 1;
                continue;
            }
            let ror_assignment = match affiliation {
                Value::Object(_) => parse_explicit_ror(affiliation),
                _ => ParsedRorAssignment {
                    status: RorAssignmentStatus::Unassigned,
                    canonical_id: None,
                    invalid_reason: None,
                },
            };

            occurrences.push(AffiliationOccurrenceRecord {
                doi: doi.to_string(),
                party_type,
                party_index,
                party_name: party_name.clone(),
                party_name_type: party_name_type.clone(),
                party_given_name: party_given_name.clone(),
                party_family_name: party_family_name.clone(),
                party_raw: party_raw.clone(),
                affiliation_index,
                affiliation_hash: hash_affiliation(&affiliation_name),
                affiliation: affiliation_name,
                affiliation_raw: match affiliation {
                    Value::Object(_) => Some(affiliation.clone()),
                    _ => None,
                },
                ror_assignment_status: ror_assignment.status,
                canonical_ror_id: ror_assignment.canonical_id.clone(),
                invalid_ror_reason: ror_assignment.invalid_reason,
                existing_ror_id: ror_assignment.canonical_id,
            });
        }
    }

    Ok((occurrences, excluded_zero_length_affiliations))
}

pub fn parse_affiliations(record: &Value) -> Result<ParsedDataCiteRecord> {
    let doi = extract_doi(record).ok_or_else(|| anyhow!("record is missing a DOI"))?;
    let (mut occurrences, mut excluded_zero_length_affiliations) =
        extract_party_occurrences(record, &doi, PartyType::Creator, "/attributes/creators")?;
    let (contributor_occurrences, contributor_excluded_zero_length_affiliations) =
        extract_party_occurrences(
            record,
            &doi,
            PartyType::Contributor,
            "/attributes/contributors",
        )?;

    occurrences.extend(contributor_occurrences);
    excluded_zero_length_affiliations += contributor_excluded_zero_length_affiliations;

    Ok(ParsedDataCiteRecord {
        doi,
        occurrences,
        excluded_zero_length_affiliations,
    })
}
