use heck::ToUpperCamelCase;

use super::RustType;
use crate::introspect::SchemaInfo;

pub fn map_type(udt_name: &str, schema_info: &SchemaInfo) -> RustType {
    // Handle array types (prefixed with '_' in PG)
    if let Some(inner) = udt_name.strip_prefix('_') {
        let inner_type = map_type(inner, schema_info);
        return inner_type.wrap_vec();
    }

    // Check if it's a known enum
    if schema_info.enums.iter().any(|e| e.name == udt_name) {
        let name = udt_name.to_upper_camel_case();
        return RustType::with_import(&name, &format!("use super::types::{};", name));
    }

    // Check if it's a known composite type
    if schema_info.composite_types.iter().any(|c| c.name == udt_name) {
        let name = udt_name.to_upper_camel_case();
        return RustType::with_import(&name, &format!("use super::types::{};", name));
    }

    // Check if it's a known domain
    if let Some(domain) = schema_info.domains.iter().find(|d| d.name == udt_name) {
        // Map to the domain's base type
        return map_type(&domain.base_type, schema_info);
    }

    match udt_name {
        "bool" => RustType::simple("bool"),
        "int2" | "smallint" | "smallserial" => RustType::simple("i16"),
        "int4" | "int" | "integer" | "serial" => RustType::simple("i32"),
        "int8" | "bigint" | "bigserial" => RustType::simple("i64"),
        "float4" | "real" => RustType::simple("f32"),
        "float8" | "double precision" => RustType::simple("f64"),
        "numeric" | "decimal" => {
            RustType::with_import("Decimal", "use rust_decimal::Decimal;")
        }
        "varchar" | "text" | "bpchar" | "char" | "name" | "citext" => RustType::simple("String"),
        "bytea" => RustType::simple("Vec<u8>"),
        "timestamp" | "timestamp without time zone" => {
            RustType::with_import("NaiveDateTime", "use chrono::NaiveDateTime;")
        }
        "timestamptz" | "timestamp with time zone" => {
            RustType::with_import("DateTime<Utc>", "use chrono::{DateTime, Utc};")
        }
        "date" => RustType::with_import("NaiveDate", "use chrono::NaiveDate;"),
        "time" | "time without time zone" => {
            RustType::with_import("NaiveTime", "use chrono::NaiveTime;")
        }
        "timetz" | "time with time zone" => {
            RustType::with_import("NaiveTime", "use chrono::NaiveTime;")
        }
        "uuid" => RustType::with_import("Uuid", "use uuid::Uuid;"),
        "json" | "jsonb" => {
            RustType::with_import("Value", "use serde_json::Value;")
        }
        "inet" | "cidr" => {
            RustType::with_import("IpNetwork", "use ipnetwork::IpNetwork;")
        }
        "oid" => RustType::simple("u32"),
        _ => RustType::simple("String"), // fallback
    }
}
