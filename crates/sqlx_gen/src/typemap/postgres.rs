use heck::ToUpperCamelCase;

use super::RustType;
use crate::cli::TimeCrate;
use crate::introspect::SchemaInfo;

/// Returns true if the udt_name is a known PostgreSQL builtin type
/// (i.e., not a fallback to String).
pub fn is_builtin(udt_name: &str) -> bool {
    matches!(
        udt_name,
        "bool"
            | "int2" | "smallint" | "smallserial"
            | "int4" | "int" | "integer" | "serial"
            | "int8" | "bigint" | "bigserial"
            | "float4" | "real"
            | "float8" | "double precision"
            | "numeric" | "decimal"
            | "varchar" | "text" | "bpchar" | "char" | "name" | "citext"
            | "bytea"
            | "timestamp" | "timestamp without time zone"
            | "timestamptz" | "timestamp with time zone"
            | "date"
            | "time" | "time without time zone"
            | "timetz" | "time with time zone"
            | "uuid"
            | "json" | "jsonb"
            | "inet" | "cidr"
            | "oid"
    )
}

pub fn map_type(udt_name: &str, schema_info: &SchemaInfo, time_crate: TimeCrate) -> RustType {
    // Handle array types (prefixed with '_' in PG)
    if let Some(inner) = udt_name.strip_prefix('_') {
        let inner_type = map_type(inner, schema_info, time_crate);
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
        return map_type(&domain.base_type, schema_info, time_crate);
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
        "timestamp" | "timestamp without time zone" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveDateTime", "use chrono::NaiveDateTime;"),
            TimeCrate::Time => RustType::with_import("PrimitiveDateTime", "use time::PrimitiveDateTime;"),
        },
        "timestamptz" | "timestamp with time zone" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("DateTime<Utc>", "use chrono::{DateTime, Utc};"),
            TimeCrate::Time => RustType::with_import("OffsetDateTime", "use time::OffsetDateTime;"),
        },
        "date" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveDate", "use chrono::NaiveDate;"),
            TimeCrate::Time => RustType::with_import("Date", "use time::Date;"),
        },
        "time" | "time without time zone" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveTime", "use chrono::NaiveTime;"),
            TimeCrate::Time => RustType::with_import("Time", "use time::Time;"),
        },
        "timetz" | "time with time zone" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveTime", "use chrono::NaiveTime;"),
            TimeCrate::Time => RustType::with_import("Time", "use time::Time;"),
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TimeCrate;
    use crate::introspect::{CompositeTypeInfo, DomainInfo, EnumInfo};

    fn empty_schema() -> SchemaInfo {
        SchemaInfo::default()
    }

    fn schema_with_enum(name: &str) -> SchemaInfo {
        SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: name.to_string(),
                variants: vec!["a".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        }
    }

    fn schema_with_composite(name: &str) -> SchemaInfo {
        SchemaInfo {
            composite_types: vec![CompositeTypeInfo {
                schema_name: "public".to_string(),
                name: name.to_string(),
                fields: vec![],
            }],
            ..Default::default()
        }
    }

    fn schema_with_domain(name: &str, base: &str) -> SchemaInfo {
        SchemaInfo {
            domains: vec![DomainInfo {
                schema_name: "public".to_string(),
                name: name.to_string(),
                base_type: base.to_string(),
            }],
            ..Default::default()
        }
    }

    // --- builtins ---

    #[test]
    fn test_bool() {
        assert_eq!(map_type("bool", &empty_schema(), TimeCrate::Chrono).path, "bool");
    }

    #[test]
    fn test_int2() {
        assert_eq!(map_type("int2", &empty_schema(), TimeCrate::Chrono).path, "i16");
    }

    #[test]
    fn test_smallint() {
        assert_eq!(map_type("smallint", &empty_schema(), TimeCrate::Chrono).path, "i16");
    }

    #[test]
    fn test_smallserial() {
        assert_eq!(map_type("smallserial", &empty_schema(), TimeCrate::Chrono).path, "i16");
    }

    #[test]
    fn test_int4() {
        assert_eq!(map_type("int4", &empty_schema(), TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_integer() {
        assert_eq!(map_type("integer", &empty_schema(), TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_serial() {
        assert_eq!(map_type("serial", &empty_schema(), TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_int8() {
        assert_eq!(map_type("int8", &empty_schema(), TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_bigint() {
        assert_eq!(map_type("bigint", &empty_schema(), TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_bigserial() {
        assert_eq!(map_type("bigserial", &empty_schema(), TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_float4() {
        assert_eq!(map_type("float4", &empty_schema(), TimeCrate::Chrono).path, "f32");
    }

    #[test]
    fn test_real() {
        assert_eq!(map_type("real", &empty_schema(), TimeCrate::Chrono).path, "f32");
    }

    #[test]
    fn test_float8() {
        assert_eq!(map_type("float8", &empty_schema(), TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_double_precision() {
        assert_eq!(map_type("double precision", &empty_schema(), TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_numeric() {
        let rt = map_type("numeric", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Decimal");
        assert!(rt.needs_import.as_ref().unwrap().contains("rust_decimal"));
    }

    #[test]
    fn test_decimal() {
        let rt = map_type("decimal", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Decimal");
    }

    #[test]
    fn test_varchar() {
        assert_eq!(map_type("varchar", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_text() {
        assert_eq!(map_type("text", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_bpchar() {
        assert_eq!(map_type("bpchar", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_citext() {
        assert_eq!(map_type("citext", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_name() {
        assert_eq!(map_type("name", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_bytea() {
        assert_eq!(map_type("bytea", &empty_schema(), TimeCrate::Chrono).path, "Vec<u8>");
    }

    #[test]
    fn test_uuid() {
        let rt = map_type("uuid", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Uuid");
        assert!(rt.needs_import.as_ref().unwrap().contains("uuid::Uuid"));
    }

    #[test]
    fn test_json() {
        let rt = map_type("json", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Value");
        assert!(rt.needs_import.as_ref().unwrap().contains("serde_json"));
    }

    #[test]
    fn test_jsonb() {
        let rt = map_type("jsonb", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Value");
    }

    #[test]
    fn test_timestamp() {
        let rt = map_type("timestamp", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_timestamptz() {
        let rt = map_type("timestamptz", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "DateTime<Utc>");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_date() {
        let rt = map_type("date", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDate");
    }

    #[test]
    fn test_time() {
        let rt = map_type("time", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveTime");
    }

    #[test]
    fn test_timetz() {
        let rt = map_type("timetz", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveTime");
    }

    #[test]
    fn test_inet() {
        let rt = map_type("inet", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "IpNetwork");
        assert!(rt.needs_import.as_ref().unwrap().contains("ipnetwork"));
    }

    #[test]
    fn test_cidr() {
        let rt = map_type("cidr", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "IpNetwork");
    }

    #[test]
    fn test_oid() {
        assert_eq!(map_type("oid", &empty_schema(), TimeCrate::Chrono).path, "u32");
    }

    // --- arrays ---

    #[test]
    fn test_array_int4() {
        assert_eq!(map_type("_int4", &empty_schema(), TimeCrate::Chrono).path, "Vec<i32>");
    }

    #[test]
    fn test_array_text() {
        assert_eq!(map_type("_text", &empty_schema(), TimeCrate::Chrono).path, "Vec<String>");
    }

    #[test]
    fn test_array_uuid() {
        let rt = map_type("_uuid", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Vec<Uuid>");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_array_bool() {
        assert_eq!(map_type("_bool", &empty_schema(), TimeCrate::Chrono).path, "Vec<bool>");
    }

    #[test]
    fn test_array_jsonb() {
        let rt = map_type("_jsonb", &empty_schema(), TimeCrate::Chrono);
        assert_eq!(rt.path, "Vec<Value>");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_array_bytea() {
        assert_eq!(map_type("_bytea", &empty_schema(), TimeCrate::Chrono).path, "Vec<Vec<u8>>");
    }

    // --- enums/composites/domains ---

    #[test]
    fn test_enum_status() {
        let schema = schema_with_enum("status");
        let rt = map_type("status", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "Status");
        assert!(rt.needs_import.as_ref().unwrap().contains("super::types::Status"));
    }

    #[test]
    fn test_enum_user_role() {
        let schema = schema_with_enum("user_role");
        let rt = map_type("user_role", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "UserRole");
    }

    #[test]
    fn test_composite_address() {
        let schema = schema_with_composite("address");
        let rt = map_type("address", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "Address");
        assert!(rt.needs_import.as_ref().unwrap().contains("super::types::Address"));
    }

    #[test]
    fn test_composite_geo_point() {
        let schema = schema_with_composite("geo_point");
        let rt = map_type("geo_point", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "GeoPoint");
    }

    #[test]
    fn test_domain_text() {
        let schema = schema_with_domain("email", "text");
        let rt = map_type("email", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "String");
    }

    #[test]
    fn test_domain_int4() {
        let schema = schema_with_domain("positive_int", "int4");
        let rt = map_type("positive_int", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "i32");
    }

    #[test]
    fn test_domain_uuid() {
        let schema = schema_with_domain("my_uuid", "uuid");
        let rt = map_type("my_uuid", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "Uuid");
        assert!(rt.needs_import.is_some());
    }

    // --- arrays of custom types ---

    #[test]
    fn test_array_enum() {
        let schema = schema_with_enum("status");
        let rt = map_type("_status", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "Vec<Status>");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_array_composite() {
        let schema = schema_with_composite("address");
        let rt = map_type("_address", &schema, TimeCrate::Chrono);
        assert_eq!(rt.path, "Vec<Address>");
    }

    // --- fallback ---

    #[test]
    fn test_geometry_fallback() {
        assert_eq!(map_type("geometry", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_hstore_fallback() {
        assert_eq!(map_type("hstore", &empty_schema(), TimeCrate::Chrono).path, "String");
    }

    // --- time crate ---

    #[test]
    fn test_timestamptz_time_crate() {
        let rt = map_type("timestamptz", &empty_schema(), TimeCrate::Time);
        assert_eq!(rt.path, "OffsetDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::OffsetDateTime"));
    }

    #[test]
    fn test_timestamp_time_crate() {
        let rt = map_type("timestamp", &empty_schema(), TimeCrate::Time);
        assert_eq!(rt.path, "PrimitiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::PrimitiveDateTime"));
    }

    #[test]
    fn test_date_time_crate() {
        let rt = map_type("date", &empty_schema(), TimeCrate::Time);
        assert_eq!(rt.path, "Date");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::Date"));
    }

    #[test]
    fn test_time_time_crate() {
        let rt = map_type("time", &empty_schema(), TimeCrate::Time);
        assert_eq!(rt.path, "Time");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::Time"));
    }
}
