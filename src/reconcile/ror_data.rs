use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct RorName {
    value: String,
    types: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RorRecord {
    id: String,
    names: Vec<RorName>,
}

/// Load ROR data file and build ID -> display name lookup
pub fn load_ror_data<P: AsRef<Path>>(path: P) -> Result<HashMap<String, String>> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);
    let records: Vec<RorRecord> = serde_json::from_reader(reader)?;

    let mut lookup = HashMap::new();

    for record in records {
        // Prefer ror_display name, fall back to first name
        let display_name = record
            .names
            .iter()
            .find(|n| n.types.contains(&"ror_display".to_string()))
            .or_else(|| record.names.first())
            .map(|n| n.value.clone());

        if let Some(name) = display_name {
            lookup.insert(record.id, name);
        }
    }

    Ok(lookup)
}
