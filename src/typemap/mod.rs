pub mod mysql;
pub mod postgres;
pub mod sqlite;

use std::collections::HashMap;

use crate::cli::DatabaseKind;
use crate::introspect::{ColumnInfo, SchemaInfo};

/// Resolved Rust type with its required imports.
#[derive(Debug, Clone)]
pub struct RustType {
    pub path: String,
    pub needs_import: Option<String>,
}

impl RustType {
    pub fn simple(path: &str) -> Self {
        Self {
            path: path.to_string(),
            needs_import: None,
        }
    }

    pub fn with_import(path: &str, import: &str) -> Self {
        Self {
            path: path.to_string(),
            needs_import: Some(import.to_string()),
        }
    }

    pub fn wrap_option(self) -> Self {
        Self {
            path: format!("Option<{}>", self.path),
            needs_import: self.needs_import,
        }
    }

    pub fn wrap_vec(self) -> Self {
        Self {
            path: format!("Vec<{}>", self.path),
            needs_import: self.needs_import,
        }
    }
}

pub fn map_column(
    col: &ColumnInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    overrides: &HashMap<String, String>,
) -> RustType {
    // Check type overrides first
    if let Some(override_type) = overrides.get(&col.udt_name) {
        let rt = RustType::simple(override_type);
        return if col.is_nullable { rt.wrap_option() } else { rt };
    }

    let base = match db_kind {
        DatabaseKind::Postgres => postgres::map_type(&col.udt_name, schema_info),
        DatabaseKind::Mysql => mysql::map_type(&col.data_type, &col.udt_name),
        DatabaseKind::Sqlite => sqlite::map_type(&col.udt_name),
    };

    if col.is_nullable {
        base.wrap_option()
    } else {
        base
    }
}

