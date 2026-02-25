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
