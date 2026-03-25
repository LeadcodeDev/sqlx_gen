use super::RustType;
use crate::cli::TimeCrate;

pub fn map_type(declared_type: &str, time_crate: TimeCrate) -> RustType {
    let upper = declared_type.to_uppercase();

    if upper.contains("INT") {
        return RustType::simple("i64");
    }
    if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
        return RustType::simple("String");
    }
    if upper.contains("BLOB") || upper.is_empty() {
        return RustType::simple("Vec<u8>");
    }
    if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
        return RustType::simple("f64");
    }
    if upper.contains("BOOL") {
        return RustType::simple("bool");
    }
    if upper.contains("TIMESTAMP") || upper.contains("DATETIME") {
        return match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveDateTime", "use chrono::NaiveDateTime;"),
            TimeCrate::Time => RustType::with_import("PrimitiveDateTime", "use time::PrimitiveDateTime;"),
        };
    }
    if upper.contains("DATE") {
        return match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveDate", "use chrono::NaiveDate;"),
            TimeCrate::Time => RustType::with_import("Date", "use time::Date;"),
        };
    }
    if upper.contains("TIME") {
        return match time_crate {
            TimeCrate::Chrono => RustType::with_import("NaiveTime", "use chrono::NaiveTime;"),
            TimeCrate::Time => RustType::with_import("Time", "use time::Time;"),
        };
    }
    if upper.contains("NUMERIC") || upper.contains("DECIMAL") {
        return RustType::simple("f64");
    }

    // Default: SQLite is loosely typed
    RustType::simple("String")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TimeCrate;

    #[test]
    fn test_integer() {
        assert_eq!(map_type("INTEGER", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_int() {
        assert_eq!(map_type("INT", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_bigint() {
        assert_eq!(map_type("BIGINT", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_smallint() {
        assert_eq!(map_type("SMALLINT", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_tinyint() {
        assert_eq!(map_type("TINYINT", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_mediumint() {
        assert_eq!(map_type("MEDIUMINT", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_text() {
        assert_eq!(map_type("TEXT", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_varchar() {
        assert_eq!(map_type("VARCHAR(255)", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_character() {
        assert_eq!(map_type("CHARACTER(20)", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_clob() {
        assert_eq!(map_type("CLOB", TimeCrate::Chrono).path, "String");
    }

    #[test]
    fn test_blob() {
        assert_eq!(map_type("BLOB", TimeCrate::Chrono).path, "Vec<u8>");
    }

    #[test]
    fn test_empty_type() {
        assert_eq!(map_type("", TimeCrate::Chrono).path, "Vec<u8>");
    }

    #[test]
    fn test_real() {
        assert_eq!(map_type("REAL", TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_float() {
        assert_eq!(map_type("FLOAT", TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_double() {
        assert_eq!(map_type("DOUBLE", TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_double_precision() {
        assert_eq!(map_type("DOUBLE PRECISION", TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_boolean() {
        assert_eq!(map_type("BOOLEAN", TimeCrate::Chrono).path, "bool");
    }

    #[test]
    fn test_timestamp() {
        let rt = map_type("TIMESTAMP", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_datetime() {
        let rt = map_type("DATETIME", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_date() {
        let rt = map_type("DATE", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveDate");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_time() {
        let rt = map_type("TIME", TimeCrate::Chrono);
        assert_eq!(rt.path, "NaiveTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_numeric() {
        assert_eq!(map_type("NUMERIC", TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_decimal() {
        assert_eq!(map_type("DECIMAL", TimeCrate::Chrono).path, "f64");
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(map_type("integer", TimeCrate::Chrono).path, "i64");
    }

    #[test]
    fn test_fallback_unknown_type() {
        assert_eq!(map_type("JSON", TimeCrate::Chrono).path, "String");
    }

    // --- time crate ---

    #[test]
    fn test_timestamp_time_crate() {
        let rt = map_type("TIMESTAMP", TimeCrate::Time);
        assert_eq!(rt.path, "PrimitiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::PrimitiveDateTime"));
    }

    #[test]
    fn test_datetime_time_crate() {
        let rt = map_type("DATETIME", TimeCrate::Time);
        assert_eq!(rt.path, "PrimitiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::PrimitiveDateTime"));
    }

    #[test]
    fn test_date_time_crate() {
        let rt = map_type("DATE", TimeCrate::Time);
        assert_eq!(rt.path, "Date");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::Date"));
    }

    #[test]
    fn test_time_time_crate() {
        let rt = map_type("TIME", TimeCrate::Time);
        assert_eq!(rt.path, "Time");
        assert!(rt.needs_import.as_ref().unwrap().contains("time::Time"));
    }
}
