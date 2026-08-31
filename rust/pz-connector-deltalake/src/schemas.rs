//! The two JSON Schema strings `Hello` hands the host, embedded at compile time so a released
//! binary never needs its schema files alongside it on disk.

pub const CONNECTION_CONFIG_SCHEMA: &str = include_str!("../schemas/connection.schema.json");
pub const DATASET_CONFIG_SCHEMA: &str = include_str!("../schemas/dataset.schema.json");
