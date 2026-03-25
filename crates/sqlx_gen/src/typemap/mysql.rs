use heck::ToUpperCamelCase;

use super::RustType;
use crate::cli::TimeCrate;

pub fn map_type(data_type: &str, column_type: &str, time_crate: TimeCrate) -> RustType {
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
        "date" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveDate", "use chrono::NaiveDate;"),
            TimeCrate::Time => RustType::with_import("Date", "use time::Date;"),
        },
        "time" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveTime", "use chrono::NaiveTime;"),
            TimeCrate::Time => RustType::with_import("Time", "use time::Time;"),
        },
        "datetime" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveDateTime", "use chrono::NaiveDateTime;"),
            TimeCrate::Time => RustType::with_import("PrimitiveDateTime", "use time::PrimitiveDateTime;"),
        },
        "timestamp" => match time_crate {
            TimeCrate::Chrono => RustType::with_import("DateTime<Utc>", "use chrono::{DateTime, Utc};"),
            TimeCrate::Time => RustType::with_import("OffsetDateTime", "use time::OffsetDateTime;"),
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TimeCrate;

    // --- tinyint ---

    #[test]
    fn test_tinyint1_is_bool() {
        assert_eq!(map_type("tinyint", "tinyint(1)", TimeCrate::Chrono).path, "bool");
    }

    #[test]
    fn test_tinyint_signed() {
        assert_eq!(map_type("tinyint", "tinyint", TimeCrate::Chrono).path, "i8");
    }

    #[test]
    fn test_tinyint_unsigned() {
        assert_eq!(map_type("tinyint", "tinyint unsigned", TimeCrate::Chrono).path, "u8");
    }

    #[test]
    fn test_tinyint3_signed() {
        assert_eq!(map_type("tinyint", "tinyint(3)", TimeCrate::Chrono).path, "i8");
    }

    #[test]
    fn test_tinyint3_unsigned() {
        assert_eq!(map_type("tinyint", "tinyint(3) unsigned", TimeCrate::Chrono).path, "u8");
    }

    // --- smallint ---

    #[test]
    fn test_smallint_signed() {
        assert_eq!(map_type("smallint", "smallint", TimeCrate::Chrono).path, "i16");
    }

    #[test]
    fn test_smallint_unsigned() {
        assert_eq!(map_type("smallint", "smallint unsigned", TimeCrate::Chrono).path, "u16");
    }

    // --- int/mediumint ---

    #[test]
    fn test_int_signed() {
        assert_eq!(map_type("int", "int", TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_int_unsigned() {
        assert_eq!(map_type("int", "int unsigned", TimeCrate::Chrono).path, "u32");
    }

    #[test]
    fn test_mediumint_signed() {
        assert_eq!(map_type("mediumint", "mediumint", TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_mediumint_unsigned() {
        assert_eq!(map_type("mediumint", "mediumint unsigned", TimeCrate::Chrono).path, "u32");
    }

    #[test]
    fn test_int11_signed() {
        assert_eq!(map_type("int", "int(11)", TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_int11_unsigned() {
        assert_eq!(map_type("int", "int(11) unsigned", TimeCrate::Chrono).path, "u32");
    }

    // --- bigint ---

    #[test]
    fn test_bigint_signed() {
        assert_eq!(map_type("bigint", "bigint", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_bigint_unsigned() {
        assert_eq!(map_type("bigint", "bigint unsigned", TimeCrate::Chrono).path, "u64");
    }

    #[test]
    fn test_bigint20_signed() {
        assert_eq!(map_type("bigint", "bigint(20)", TimeCrate::Chrono).path, "i64");
    }

    // --- floats ---

    #[test]
    fn test_float() {
        assert_eq!(map_type("float", "float", TimeCrate::Chrono).path, "f32");
    }

    #[test]
    fn test_double() {
        assert_eq!(map_type("double", "double", TimeCrate::Chrono).path, "f64");
    }

    // --- decimal ---

    #[test]
    fn test_decimal() {
        let rt = map_type("decimal", "decimal(10,2)", TimeCrate::Chrono);
        assert_eq!(rt.path, "Decimal");
        assert!(rt.needs_import.as_ref().unwrap().contains("rust_decimal"));
    }

    #[test]
    fn test_numeric() {
        let rt = map_type("numeric", "numeric", TimeCrate::Chrono);
        assert_eq!(rt.path, "Decimal");
        assert!(rt.needs_import.is_some());
    }

    // --- strings ---

    #[test]
    fn test_varchar() {
        assert_eq!(map_type("varchar", "varchar(255)", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_char() {
        assert_eq!(map_type("char", "char(1)", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_text() {
        assert_eq!(map_type("text", "text", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_tinytext() {
        assert_eq!(map_type("tinytext", "tinytext", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_mediumtext() {
        assert_eq!(map_type("mediumtext", "mediumtext", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_longtext() {
        assert_eq!(map_type("longtext", "longtext", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_set() {
        assert_eq!(map_type("set", "set('a','b')", TimeCrate::Chrono).path, "String");
    }

    // --- binary ---

    #[test]
    fn test_binary() {
        assert_eq!(map_type("binary", "binary(16)", TimeCrate::Chrono).path, "Vec<u8>");
    }

    #[test]
    fn test_varbinary() {
        assert_eq!(map_type("varbinary", "varbinary(255)", TimeCrate::Chrono).path, "Vec<u8>");
    }

    #[test]
    fn test_blob() {
        assert_eq!(map_type("blob", "blob", TimeCrate::Chrono).path, "Vec<u8>");
    }

    #[test]
    fn test_tinyblob() {
        assert_eq!(map_type("tinyblob", "tinyblob", TimeCrate::Chrono).path, "Vec<u8>");
    }

    // --- dates ---

    #[test]
    fn test_date() {
        let rt = map_type("date", "date", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDate");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_time() {
        let rt = map_type("time", "time", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_datetime() {
        let rt = map_type("datetime", "datetime", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_timestamp() {
        let rt = map_type("timestamp", "timestamp", TimeCrate::Chrono);
        assert_eq!(rt.path, "DateTime<Utc>");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    // --- misc ---

    #[test]
    fn test_json() {
        let rt = map_type("json", "json", TimeCrate::Chrono);
        assert_eq!(rt.path, "Value");
        assert!(rt.needs_import.as_ref().unwrap().contains("serde_json"));
    }

    #[test]
    fn test_year() {
        assert_eq!(map_type("year", "year", TimeCrate::Chrono).path, "i16");
    }

    #[test]
    fn test_bit() {
        assert_eq!(map_type("bit", "bit(1)", TimeCrate::Chrono).path, "Vec<u8>");
    }

    // --- enum placeholder ---

    #[test]
    fn test_enum_placeholder() {
        assert_eq!(map_type("enum", "enum('a','b','c')", TimeCrate::Chrono).path, "String");
    }

    // --- case insensitive ---

    #[test]
    fn test_case_insensitive_int() {
        assert_eq!(map_type("INT", "INT", TimeCrate::Chrono).path, "i32");
    }

    #[test]
    fn test_case_insensitive_tinyint1() {
        assert_eq!(map_type("TINYINT", "TINYINT(1)", TimeCrate::Chrono).path, "bool");
    }

    // --- fallback ---

    #[test]
    fn test_geometry_fallback() {
        assert_eq!(map_type("geometry", "geometry", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_point_fallback() {
        assert_eq!(map_type("point", "point", TimeCrate::Chrono).path, "String");
    }

    // --- resolve_enum_type ---

    #[test]
    fn test_resolve_enum_users_status() {
        assert_eq!(resolve_enum_type("users", "status"), "UsersStatus");
    }

    #[test]
    fn test_resolve_enum_user_roles_role_type() {
        assert_eq!(resolve_enum_type("user_roles", "role_type"), "UserRolesRoleType");
    }

    #[test]
    fn test_resolve_enum_short_names() {
        assert_eq!(resolve_enum_type("t", "c"), "TC");
    }

    #[test]
    fn test_resolve_enum_order_items_size() {
        assert_eq!(resolve_enum_type("order_items", "size"), "OrderItemsSize");
    }

    // --- time crate ---

    #[test]
    fn test_timestamp_time_crate() {
        let rt = map_type("timestamp", "timestamp", TimeCrate::Time);
        assert_eq!(rt.path, "OffsetDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::OffsetDateTime"));
    }

    #[test]
    fn test_datetime_time_crate() {
        let rt = map_type("datetime", "datetime", TimeCrate::Time);
        assert_eq!(rt.path, "PrimitiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::PrimitiveDateTime"));
    }

    #[test]
    fn test_date_time_crate() {
        let rt = map_type("date", "date", TimeCrate::Time);
        assert_eq!(rt.path, "Date");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::Date"));
    }

    #[test]
    fn test_time_time_crate() {
        let rt = map_type("time", "time", TimeCrate::Time);
        assert_eq!(rt.path, "Time");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::Time"));
    }
}
