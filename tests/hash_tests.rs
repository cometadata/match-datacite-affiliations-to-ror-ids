use datacite_ror::hash_affiliation;

#[test]
fn test_hash_affiliation_produces_16_char_hex() {
    let hash = hash_affiliation("University of Oxford");
    assert_eq!(hash.len(), 16);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_hash_affiliation_is_deterministic() {
    let hash1 = hash_affiliation("MIT");
    let hash2 = hash_affiliation("MIT");
    assert_eq!(hash1, hash2);
}

#[test]
fn test_hash_affiliation_differs_for_different_inputs() {
    let hash1 = hash_affiliation("MIT");
    let hash2 = hash_affiliation("Stanford University");
    assert_ne!(hash1, hash2);
}

#[test]
fn test_hash_affiliation_handles_unicode() {
    let hash = hash_affiliation("Université de Paris");
    assert_eq!(hash.len(), 16);
}

#[test]
fn test_hash_affiliation_handles_empty_string() {
    let hash = hash_affiliation("");
    assert_eq!(hash.len(), 16);
}
