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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::introspect::SchemaInfo;
    use std::collections::HashMap;

    fn make_col(udt_name: &str, data_type: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: "test".to_string(),
            data_type: data_type.to_string(),
            udt_name: udt_name.to_string(),
            is_nullable: nullable,
            ordinal_position: 0,
            schema_name: "public".to_string(),
        }
    }

    // --- RustType::simple ---

    #[test]
    fn test_simple_creates_without_import() {
        let rt = RustType::simple("i32");
        assert_eq!(rt.path, "i32");
        assert!(rt.needs_import.is_none());
    }

    #[test]
    fn test_simple_path_correct() {
        let rt = RustType::simple("String");
        assert_eq!(rt.path, "String");
    }

    #[test]
    fn test_simple_no_import() {
        let rt = RustType::simple("bool");
        assert_eq!(rt.needs_import, None);
    }

    // --- RustType::with_import ---

    #[test]
    fn test_with_import_creates_with_import() {
        let rt = RustType::with_import("Uuid", "use uuid::Uuid;");
        assert_eq!(rt.path, "Uuid");
        assert_eq!(rt.needs_import, Some("use uuid::Uuid;".to_string()));
    }

    #[test]
    fn test_with_import_path_correct() {
        let rt = RustType::with_import("DateTime<Utc>", "use chrono::{DateTime, Utc};");
        assert_eq!(rt.path, "DateTime<Utc>");
    }

    #[test]
    fn test_with_import_import_present() {
        let rt = RustType::with_import("Value", "use serde_json::Value;");
        assert!(rt.needs_import.is_some());
    }

    // --- RustType::wrap_option ---

    #[test]
    fn test_wrap_option_wraps_path() {
        let rt = RustType::simple("i32").wrap_option();
        assert_eq!(rt.path, "Option<i32>");
    }

    #[test]
    fn test_wrap_option_preserves_import() {
        let rt = RustType::with_import("Uuid", "use uuid::Uuid;").wrap_option();
        assert_eq!(rt.path, "Option<Uuid>");
        assert_eq!(rt.needs_import, Some("use uuid::Uuid;".to_string()));
    }

    #[test]
    fn test_wrap_option_double_wrap() {
        let rt = RustType::simple("i32").wrap_option().wrap_option();
        assert_eq!(rt.path, "Option<Option<i32>>");
    }

    // --- RustType::wrap_vec ---

    #[test]
    fn test_wrap_vec_wraps_path() {
        let rt = RustType::simple("i32").wrap_vec();
        assert_eq!(rt.path, "Vec<i32>");
    }

    #[test]
    fn test_wrap_vec_preserves_import() {
        let rt = RustType::with_import("Uuid", "use uuid::Uuid;").wrap_vec();
        assert_eq!(rt.path, "Vec<Uuid>");
        assert_eq!(rt.needs_import, Some("use uuid::Uuid;".to_string()));
    }

    // --- map_column ---

    #[test]
    fn test_override_takes_precedence() {
        let col = make_col("uuid", "uuid", false);
        let schema = SchemaInfo::default();
        let mut overrides = HashMap::new();
        overrides.insert("uuid".to_string(), "MyUuid".to_string());
        let rt = map_column(&col, DatabaseKind::Postgres, &schema, &overrides);
        assert_eq!(rt.path, "MyUuid");
        assert!(rt.needs_import.is_none());
    }

    #[test]
    fn test_override_with_nullable() {
        let col = make_col("uuid", "uuid", true);
        let schema = SchemaInfo::default();
        let mut overrides = HashMap::new();
        overrides.insert("uuid".to_string(), "MyUuid".to_string());
        let rt = map_column(&col, DatabaseKind::Postgres, &schema, &overrides);
        assert_eq!(rt.path, "Option<MyUuid>");
    }

    #[test]
    fn test_no_override_dispatches_postgres() {
        let col = make_col("int4", "integer", false);
        let schema = SchemaInfo::default();
        let overrides = HashMap::new();
        let rt = map_column(&col, DatabaseKind::Postgres, &schema, &overrides);
        assert_eq!(rt.path, "i32");
    }

    #[test]
    fn test_nullable_without_override() {
        let col = make_col("int4", "integer", true);
        let schema = SchemaInfo::default();
        let overrides = HashMap::new();
        let rt = map_column(&col, DatabaseKind::Postgres, &schema, &overrides);
        assert_eq!(rt.path, "Option<i32>");
    }
}

