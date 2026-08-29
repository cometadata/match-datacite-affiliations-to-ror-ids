use crate::{InvalidRorReason, RorAssignmentStatus};
use serde_json::Value;

const CROCKFORD: &str = "0123456789abcdefghjkmnpqrstvwxyz";

pub(super) struct ParsedRorAssignment {
    pub status: RorAssignmentStatus,
    pub canonical_id: Option<String>,
    pub invalid_reason: Option<InvalidRorReason>,
}

fn checksum_is_valid(identifier: &str) -> bool {
    let bytes = identifier.as_bytes();
    if bytes.len() != 9 || bytes[0] != b'0' || !bytes[7..].iter().all(u8::is_ascii_digit) {
        return false;
    }
    let mut value = 0_u64;
    for byte in &bytes[..7] {
        let Some(position) = CROCKFORD
            .as_bytes()
            .iter()
            .position(|candidate| candidate == byte)
        else {
            return false;
        };
        value = value * 32 + position as u64;
    }
    let expected = 98 - ((value % 97) * 100 % 97);
    identifier[7..].parse::<u64>().ok() == Some(expected)
}

pub(super) fn parse_explicit_ror(affiliation: &Value) -> ParsedRorAssignment {
    let Some(scheme) = affiliation
        .get("affiliationIdentifierScheme")
        .and_then(Value::as_str)
    else {
        return ParsedRorAssignment {
            status: RorAssignmentStatus::Unassigned,
            canonical_id: None,
            invalid_reason: None,
        };
    };
    if !scheme.eq_ignore_ascii_case("ROR") {
        return ParsedRorAssignment {
            status: RorAssignmentStatus::Unassigned,
            canonical_id: None,
            invalid_reason: None,
        };
    }
    let Some(raw_value) = affiliation.get("affiliationIdentifier") else {
        return ParsedRorAssignment {
            status: RorAssignmentStatus::Invalid,
            canonical_id: None,
            invalid_reason: Some(InvalidRorReason::MissingIdentifier),
        };
    };
    let Some(raw_identifier) = raw_value.as_str() else {
        return ParsedRorAssignment {
            status: RorAssignmentStatus::Invalid,
            canonical_id: None,
            invalid_reason: Some(InvalidRorReason::NonStringIdentifier),
        };
    };
    let lower = raw_identifier.trim().to_ascii_lowercase();
    let identifier = lower
        .strip_prefix("https://ror.org/")
        .or_else(|| lower.strip_prefix("ror.org/"))
        .unwrap_or(&lower);
    let format_is_valid = identifier.len() == 9
        && identifier.starts_with('0')
        && identifier.as_bytes()[..7]
            .iter()
            .all(|byte| CROCKFORD.as_bytes().contains(byte))
        && identifier.as_bytes()[7..].iter().all(u8::is_ascii_digit);
    if !format_is_valid {
        return ParsedRorAssignment {
            status: RorAssignmentStatus::Invalid,
            canonical_id: None,
            invalid_reason: Some(InvalidRorReason::UnrecognizedFormat),
        };
    }
    if !checksum_is_valid(identifier) {
        return ParsedRorAssignment {
            status: RorAssignmentStatus::Invalid,
            canonical_id: None,
            invalid_reason: Some(InvalidRorReason::InvalidChecksum),
        };
    }
    ParsedRorAssignment {
        status: RorAssignmentStatus::Valid,
        canonical_id: Some(format!("https://ror.org/{identifier}")),
        invalid_reason: None,
    }
}
