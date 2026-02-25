use super::RustType;

pub fn map_type(declared_type: &str) -> RustType {
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
        return RustType::with_import("NaiveDateTime", "use chrono::NaiveDateTime;");
    }
    if upper.contains("DATE") {
        return RustType::with_import("NaiveDate", "use chrono::NaiveDate;");
    }
    if upper.contains("TIME") {
        return RustType::with_import("NaiveTime", "use chrono::NaiveTime;");
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

    #[test]
    fn test_integer() {
        assert_eq!(map_type("INTEGER").path, "i64");
    }

    #[test]
    fn test_int() {
        assert_eq!(map_type("INT").path, "i64");
    }

    #[test]
    fn test_bigint() {
        assert_eq!(map_type("BIGINT").path, "i64");
    }

    #[test]
    fn test_smallint() {
        assert_eq!(map_type("SMALLINT").path, "i64");
    }

    #[test]
    fn test_tinyint() {
        assert_eq!(map_type("TINYINT").path, "i64");
    }

    #[test]
    fn test_mediumint() {
        assert_eq!(map_type("MEDIUMINT").path, "i64");
    }

    #[test]
    fn test_text() {
        assert_eq!(map_type("TEXT").path, "String");
    }

    #[test]
    fn test_varchar() {
        assert_eq!(map_type("VARCHAR(255)").path, "String");
    }

    #[test]
    fn test_character() {
        assert_eq!(map_type("CHARACTER(20)").path, "String");
    }

    #[test]
    fn test_clob() {
        assert_eq!(map_type("CLOB").path, "String");
    }

    #[test]
    fn test_blob() {
        assert_eq!(map_type("BLOB").path, "Vec<u8>");
    }

    #[test]
    fn test_empty_type() {
        assert_eq!(map_type("").path, "Vec<u8>");
    }

    #[test]
    fn test_real() {
        assert_eq!(map_type("REAL").path, "f64");
    }

    #[test]
    fn test_float() {
        assert_eq!(map_type("FLOAT").path, "f64");
    }

    #[test]
    fn test_double() {
        assert_eq!(map_type("DOUBLE").path, "f64");
    }

    #[test]
    fn test_double_precision() {
        assert_eq!(map_type("DOUBLE PRECISION").path, "f64");
    }

    #[test]
    fn test_boolean() {
        assert_eq!(map_type("BOOLEAN").path, "bool");
    }

    #[test]
    fn test_timestamp() {
        let rt = map_type("TIMESTAMP");
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_datetime() {
        let rt = map_type("DATETIME");
        assert_eq!(rt.path, "NaiveDateTime");
        assert!(rt.needs_import.is_some());
    }

    #[test]
    fn test_date() {
        let rt = map_type("DATE");
        assert_eq!(rt.path, "NaiveDate");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_time() {
        let rt = map_type("TIME");
        assert_eq!(rt.path, "NaiveTime");
        assert!(rt.needs_import.as_ref().unwrap().contains("chrono"));
    }

    #[test]
    fn test_numeric() {
        assert_eq!(map_type("NUMERIC").path, "f64");
    }

    #[test]
    fn test_decimal() {
        assert_eq!(map_type("DECIMAL").path, "f64");
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(map_type("integer").path, "i64");
    }

    #[test]
    fn test_fallback_unknown_type() {
        assert_eq!(map_type("JSON").path, "String");
    }
}
