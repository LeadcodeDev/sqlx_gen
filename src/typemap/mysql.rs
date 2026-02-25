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

#[cfg(test)]
mod tests {
    use super::*;

    // --- tinyint ---

    #[test]
    fn test_tinyint1_is_bool() {
        assert_eq!(map_type("tinyint", "tinyint(1)").path, "bool");
    }

    #[test]
    fn test_tinyint_signed() {
        assert_eq!(map_type("tinyint", "tinyint").path, "i8");
    }

    #[test]
    fn test_tinyint_unsigned() {
        assert_eq!(map_type("tinyint", "tinyint unsigned").path, "u8");
    }

    #[test]
    fn test_tinyint3_signed() {
        assert_eq!(map_type("tinyint", "tinyint(3)").path, "i8");
    }

    #[test]
    fn test_tinyint3_unsigned() {
        assert_eq!(map_type("tinyint", "tinyint(3) unsigned").path, "u8");
    }

    // --- smallint ---

    #[test]
    fn test_smallint_signed() {
        assert_eq!(map_type("smallint", "smallint").path, "i16");
    }

    #[test]
    fn test_smallint_unsigned() {
        assert_eq!(map_type("smallint", "smallint unsigned").path, "u16");
    }

    // --- int/mediumint ---

    #[test]
    fn test_int_signed() {
        assert_eq!(map_type("int", "int").path, "i32");
    }

    #[test]
    fn test_int_unsigned() {
        assert_eq!(map_type("int", "int unsigned").path, "u32");
    }

    #[test]
    fn test_mediumint_signed() {
        assert_eq!(map_type("mediumint", "mediumint").path, "i32");
    }

    #[test]
    fn test_mediumint_unsigned() {
        assert_eq!(map_type("mediumint", "mediumint unsigned").path, "u32");
    }

    #[test]
    fn test_int11_signed() {
        assert_eq!(map_type("int", "int(11)").path, "i32");
    }

    #[test]
    fn test_int11_unsigned() {
        assert_eq!(map_type("int", "int(11) unsigned").path, "u32");
    }

    // --- bigint ---

    #[test]
    fn test_bigint_signed() {
        assert_eq!(map_type("bigint", "bigint").path, "i64");
    }

    #[test]
    fn test_bigint_unsigned() {
        assert_eq!(map_type("bigint", "bigint unsigned").path, "u64");
    }

    #[test]
    fn test_bigint20_signed() {
        assert_eq!(map_type("bigint", "bigint(20)").path, "i64");
    }

    // --- floats ---

    #[test]
    fn test_float() {
        assert_eq!(map_type("float", "float").path, "f32");
    }

    #[test]
    fn test_double() {
        assert_eq!(map_type("double", "double").path, "f64");
    }

    // --- decimal ---

    #[test]
    fn test_decimal() {
        let rt = map_type("decimal", "decimal(10,2)");
        assert_eq!(rt.path, "Decimal");
        assert!(rt.needs_import.as_ref().unwrap().contains("rust_decimal"));
    }

    #[test]
    fn test_numeric() {
        let rt = map_type("numeric", "numeric");
        assert_eq!(rt.path, "Decimal");
        assert!(rt.needs_import.is_some());
    }

    // --- strings ---

    #[test]
    fn test_varchar() {
        assert_eq!(map_type("varchar", "varchar(255)").path, "String");
    }

    #[test]
    fn test_char() {
        assert_eq!(map_type("char", "char(1)").path, "String");
    }

    #[test]
    fn test_text() {
        assert_eq!(map_type("text", "text").path, "String");
    }

    #[test]
    fn test_tinytext() {
        assert_eq!(map_type("tinytext", "tinytext").path, "String");
    }

    #[test]
    fn test_mediumtext() {
        assert_eq!(map_type("mediumtext", "mediumtext").path, "String");
    }

    #[test]
    fn test_longtext() {
        assert_eq!(map_type("longtext", "longtext").path, "String");
    }

    #[test]
    fn test_set() {
        assert_eq!(map_type("set", "set('a','b')").path, "String");
    }

    // --- binary ---

    #[test]
    fn test_binary() {
        assert_eq!(map_type("binary", "binary(16)").path, "Vec<u8>");
    }

    #[test]
    fn test_varbinary() {
        assert_eq!(map_type("varbinary", "varbinary(255)").path, "Vec<u8>");
    }

    #[test]
    fn test_blob() {
        assert_eq!(map_type("blob", "blob").path, "Vec<u8>");
    }

    #[test]
    fn test_tinyblob() {
        assert_eq!(map_type("tinyblob", "tinyblob").path, "Vec<u8>");
    }

    // --- dates ---

    #[test]
    fn test_date() {
        let rt = map_type("date", "date");
        assert_eq!(rt.path, "NaiveDate");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_time() {
        let rt = map_type("time", "time");
        assert_eq!(rt.path, "NaiveTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_datetime() {
        let rt = map_type("datetime", "datetime");
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_timestamp() {
        let rt = map_type("timestamp", "timestamp");
        assert_eq!(rt.path, "DateTime<Utc>");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    // --- misc ---

    #[test]
    fn test_json() {
        let rt = map_type("json", "json");
        assert_eq!(rt.path, "Value");
        assert!(rt.needs_import.as_ref().unwrap().contains("serde_json"));
    }

    #[test]
    fn test_year() {
        assert_eq!(map_type("year", "year").path, "i16");
    }

    #[test]
    fn test_bit() {
        assert_eq!(map_type("bit", "bit(1)").path, "Vec<u8>");
    }

    // --- enum placeholder ---

    #[test]
    fn test_enum_placeholder() {
        assert_eq!(map_type("enum", "enum('a','b','c')").path, "String");
    }

    // --- case insensitive ---

    #[test]
    fn test_case_insensitive_int() {
        assert_eq!(map_type("INT", "INT").path, "i32");
    }

    #[test]
    fn test_case_insensitive_tinyint1() {
        assert_eq!(map_type("TINYINT", "TINYINT(1)").path, "bool");
    }

    // --- fallback ---

    #[test]
    fn test_geometry_fallback() {
        assert_eq!(map_type("geometry", "geometry").path, "String");
    }

    #[test]
    fn test_point_fallback() {
        assert_eq!(map_type("point", "point").path, "String");
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
}
