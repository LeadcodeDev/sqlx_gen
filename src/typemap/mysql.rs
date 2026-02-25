use heck::ToUpperCamelCase;

use super::RustType;

pub fn map_type(data_type: &str, column_type: &str) -> RustType {
    let dt = data_type.to_lowercase();
    let ct = column_type.to_lowercase();
    let is_unsigned = ct.contains("unsigned");

    // Handle ENUM columns → generate a Rust enum reference
    if ct.starts_with("enum(") {
        // The enum name will be derived from table_name + column_name in codegen
        // For now, we can't know the full name here. Return a placeholder.
        // The actual type will be resolved in codegen.
        return RustType::simple("String");
    }

    match dt.as_str() {
        "tinyint" => {
            if ct == "tinyint(1)" {
                RustType::simple("bool")
            } else if is_unsigned {
                RustType::simple("u8")
            } else {
                RustType::simple("i8")
            }
        }
        "smallint" => {
            if is_unsigned {
                RustType::simple("u16")
            } else {
                RustType::simple("i16")
            }
        }
        "mediumint" | "int" => {
            if is_unsigned {
                RustType::simple("u32")
            } else {
                RustType::simple("i32")
            }
        }
        "bigint" => {
            if is_unsigned {
                RustType::simple("u64")
            } else {
                RustType::simple("i64")
            }
        }
        "float" => RustType::simple("f32"),
        "double" => RustType::simple("f64"),
        "decimal" | "numeric" => {
            RustType::with_import("Decimal", "use rust_decimal::Decimal;")
        }
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set" => {
            RustType::simple("String")
        }
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            RustType::simple("Vec<u8>")
        }
        "date" => RustType::with_import("NaiveDate", "use chrono::NaiveDate;"),
        "time" => RustType::with_import("NaiveTime", "use chrono::NaiveTime;"),
        "datetime" => {
            RustType::with_import("NaiveDateTime", "use chrono::NaiveDateTime;")
        }
        "timestamp" => {
            RustType::with_import("DateTime<Utc>", "use chrono::{DateTime, Utc};")
        }
        "json" => RustType::with_import("Value", "use serde_json::Value;"),
        "year" => RustType::simple("i16"),
        "bit" => RustType::simple("Vec<u8>"),
        _ => RustType::simple("String"),
    }
}

/// Resolve an inline MySQL ENUM column to the correct generated enum type name.
pub fn resolve_enum_type(table_name: &str, column_name: &str) -> String {
    let enum_name = format!("{}_{}", table_name, column_name);
    enum_name.to_upper_camel_case()
}
