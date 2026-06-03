use std::collections::HashMap;

use crate::error::Result;
use sqlx::SqlitePool;

use super::{ColumnInfo, EnumInfo, SchemaInfo, TableInfo};

pub async fn introspect(pool: &SqlitePool, include_views: bool) -> Result<SchemaInfo> {
    let mut tables = fetch_tables(pool).await?;
    let mut views = if include_views {
        fetch_views(pool).await?
    } else {
        Vec::new()
    };

    if !views.is_empty() {
        resolve_view_nullability(&mut views, &tables);
        resolve_view_primary_keys(&mut views, &tables);
    }

    let enums = extract_check_enums(pool, &mut tables).await?;

    Ok(SchemaInfo {
        tables,
        views,
        enums,
        composite_types: Vec::new(),
        domains: Vec::new(),
    })
}

/// Detect SQLite "implicit enum" columns of the form
/// `TEXT CHECK (col IN ('a', 'b', 'c'))` by parsing the DDL stored in
/// `sqlite_master.sql`. Promotes the column's `udt_name` to the enum's
/// synthesised name (`<table>_<col>_enum`) so the rest of the pipeline
/// treats it like a real enum (with PgHasArrayType skipped for SQLite).
async fn extract_check_enums(pool: &SqlitePool, tables: &mut [TableInfo]) -> Result<Vec<EnumInfo>> {
    let mut enums = Vec::new();

    for table in tables.iter_mut() {
        let sql: Option<(Option<String>,)> =
            sqlx::query_as("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(&table.name)
                .fetch_optional(pool)
                .await?;
        let Some((Some(ddl),)) = sql else { continue };

        for col in table.columns.iter_mut() {
            if let Some(variants) = parse_check_in_variants(&ddl, &col.name) {
                if variants.is_empty() {
                    continue;
                }
                let enum_name = format!("{}_{}_enum", table.name, col.name);
                col.udt_name = enum_name.clone();
                enums.push(EnumInfo {
                    schema_name: "main".to_string(),
                    name: enum_name,
                    variants,
                    default_variant: None,
                });
            }
        }
    }

    Ok(enums)
}

/// Parse `CHECK (col IN ('a','b','c'))` for a given column from a SQLite
/// CREATE TABLE statement. Returns the parsed variants in declaration order
/// or `None` if the column has no IN-style CHECK constraint.
fn parse_check_in_variants(ddl: &str, column: &str) -> Option<Vec<String>> {
    let lower_ddl = ddl.to_ascii_lowercase();
    let lower_col = column.to_ascii_lowercase();
    let mut search_from = 0usize;

    while let Some(rel_check) = lower_ddl[search_from..].find("check") {
        let check_pos = search_from + rel_check;
        let after_check = &ddl[check_pos + 5..];
        let after_check_lower = &lower_ddl[check_pos + 5..];

        let open_rel = after_check.find('(')?;
        let mut depth = 1i32;
        let mut idx = open_rel + 1;
        let bytes = after_check.as_bytes();
        while idx < bytes.len() && depth > 0 {
            match bytes[idx] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'\'' => {
                    idx += 1;
                    while idx < bytes.len() && bytes[idx] != b'\'' {
                        idx += 1;
                    }
                }
                _ => {}
            }
            idx += 1;
        }
        if depth != 0 {
            return None;
        }
        let body = &after_check[open_rel + 1..idx - 1];
        let body_lower = &after_check_lower[open_rel + 1..idx - 1];

        search_from = check_pos + 5 + idx;

        if !body_lower.contains(&lower_col) || !body_lower.contains(" in ") {
            continue;
        }

        if let Some(in_pos) = body_lower.find(" in ") {
            let list_start = body[in_pos..].find('(')?;
            let list_body = &body[in_pos + list_start + 1..];
            let mut variants = Vec::new();
            let bytes = list_body.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    let start = i + 1;
                    let mut j = start;
                    while j < bytes.len() && bytes[j] != b'\'' {
                        j += 1;
                    }
                    variants.push(list_body[start..j].to_string());
                    i = j + 1;
                } else if bytes[i] == b')' {
                    break;
                } else {
                    i += 1;
                }
            }
            return Some(variants);
        }
    }

    None
}

async fn fetch_tables(pool: &SqlitePool) -> Result<Vec<TableInfo>> {
    let table_names: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await?;

    let mut tables = Vec::new();

    for (table_name,) in table_names {
        let columns = fetch_columns(pool, &table_name).await?;
        tables.push(TableInfo {
            schema_name: "main".to_string(),
            name: table_name,
            columns,
        });
    }

    Ok(tables)
}

async fn fetch_views(pool: &SqlitePool) -> Result<Vec<TableInfo>> {
    let view_names: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name")
            .fetch_all(pool)
            .await?;

    let mut views = Vec::new();

    for (view_name,) in view_names {
        let columns = fetch_columns(pool, &view_name).await?;
        views.push(TableInfo {
            schema_name: "main".to_string(),
            name: view_name,
            columns,
        });
    }

    Ok(views)
}

async fn fetch_columns(pool: &SqlitePool, table_name: &str) -> Result<Vec<ColumnInfo>> {
    // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    let pragma_query = format!("PRAGMA table_info(\"{}\")", table_name.replace('"', "\"\""));
    let rows: Vec<(i32, String, String, bool, Option<String>, i32)> =
        sqlx::query_as(&pragma_query).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|(cid, name, declared_type, notnull, dflt_value, pk)| {
            let upper = declared_type.to_uppercase();
            ColumnInfo {
                name,
                data_type: upper.clone(),
                udt_name: upper,
                is_nullable: !notnull,
                is_primary_key: pk > 0,
                ordinal_position: cid,
                schema_name: "main".to_string(),
                column_default: dflt_value,
            }
        })
        .collect())
}

/// Resolve view column nullability by matching column names against introspected tables.
/// If a column name is found in exactly one table and is NOT NULL, propagate that.
fn resolve_view_nullability(views: &mut [TableInfo], tables: &[TableInfo]) {
    // Build lookup: column_name -> Vec<is_nullable>
    let mut col_lookup: HashMap<&str, Vec<bool>> = HashMap::new();
    for table in tables {
        for col in &table.columns {
            col_lookup
                .entry(&col.name)
                .or_default()
                .push(col.is_nullable);
        }
    }

    for view in views.iter_mut() {
        for col in view.columns.iter_mut() {
            if let Some(nullable_flags) = col_lookup.get(col.name.as_str()) {
                // Only resolve if column name appears in exactly one table
                // and that column is NOT nullable
                if nullable_flags.len() == 1 && !nullable_flags[0] {
                    col.is_nullable = false;
                }
            }
        }
    }
}

/// Resolve view column primary keys by matching column names against introspected tables.
/// If a column name is found in exactly one table and is a PK, propagate that.
fn resolve_view_primary_keys(views: &mut [TableInfo], tables: &[TableInfo]) {
    // Build lookup: column_name -> Vec<is_primary_key>
    let mut col_lookup: HashMap<&str, Vec<bool>> = HashMap::new();
    for table in tables {
        for col in &table.columns {
            col_lookup
                .entry(&col.name)
                .or_default()
                .push(col.is_primary_key);
        }
    }

    for view in views.iter_mut() {
        for col in view.columns.iter_mut() {
            if let Some(pk_flags) = col_lookup.get(col.name.as_str()) {
                // Only resolve if column name appears in exactly one table
                // and that column is a PK
                if pk_flags.len() == 1 && pk_flags[0] {
                    col.is_primary_key = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table(name: &str, columns: Vec<(&str, bool)>) -> TableInfo {
        TableInfo {
            schema_name: "main".to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (col, nullable))| ColumnInfo {
                    name: col.to_string(),
                    data_type: "TEXT".to_string(),
                    udt_name: "TEXT".to_string(),
                    is_nullable: nullable,
                    is_primary_key: false,
                    ordinal_position: i as i32,
                    schema_name: "main".to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    fn make_view(name: &str, columns: Vec<&str>) -> TableInfo {
        TableInfo {
            schema_name: "main".to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, col)| ColumnInfo {
                    name: col.to_string(),
                    data_type: "TEXT".to_string(),
                    udt_name: "TEXT".to_string(),
                    is_nullable: true,
                    is_primary_key: false,
                    ordinal_position: i as i32,
                    schema_name: "main".to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    #[test]
    fn test_resolve_unique_not_null() {
        let tables = vec![make_table("users", vec![("id", false), ("name", false)])];
        let mut views = vec![make_view("my_view", vec!["id", "name"])];
        resolve_view_nullability(&mut views, &tables);
        assert!(!views[0].columns[0].is_nullable);
        assert!(!views[0].columns[1].is_nullable);
    }

    #[test]
    fn test_resolve_nullable_source() {
        let tables = vec![make_table("users", vec![("id", false), ("name", true)])];
        let mut views = vec![make_view("my_view", vec!["id", "name"])];
        resolve_view_nullability(&mut views, &tables);
        assert!(!views[0].columns[0].is_nullable);
        assert!(views[0].columns[1].is_nullable);
    }

    #[test]
    fn test_resolve_ambiguous_stays_nullable() {
        // "id" appears in two tables — ambiguous, stay nullable
        let tables = vec![
            make_table("users", vec![("id", false)]),
            make_table("orders", vec![("id", false)]),
        ];
        let mut views = vec![make_view("my_view", vec!["id"])];
        resolve_view_nullability(&mut views, &tables);
        assert!(views[0].columns[0].is_nullable);
    }

    #[test]
    fn test_resolve_no_match() {
        let tables = vec![make_table("users", vec![("id", false)])];
        let mut views = vec![make_view("my_view", vec!["computed"])];
        resolve_view_nullability(&mut views, &tables);
        assert!(views[0].columns[0].is_nullable);
    }

    #[test]
    fn test_resolve_empty_tables() {
        let mut views = vec![make_view("my_view", vec!["id"])];
        resolve_view_nullability(&mut views, &[]);
        assert!(views[0].columns[0].is_nullable);
    }

    // ========== resolve_view_primary_keys ==========

    fn make_table_with_pk(name: &str, columns: Vec<(&str, bool)>) -> TableInfo {
        TableInfo {
            schema_name: "main".to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (col, is_pk))| ColumnInfo {
                    name: col.to_string(),
                    data_type: "TEXT".to_string(),
                    udt_name: "TEXT".to_string(),
                    is_nullable: false,
                    is_primary_key: is_pk,
                    ordinal_position: i as i32,
                    schema_name: "main".to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    #[test]
    fn test_resolve_pk_unique_match() {
        let tables = vec![make_table_with_pk(
            "users",
            vec![("id", true), ("name", false)],
        )];
        let mut views = vec![make_view("my_view", vec!["id", "name"])];
        resolve_view_primary_keys(&mut views, &tables);
        assert!(views[0].columns[0].is_primary_key);
        assert!(!views[0].columns[1].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_ambiguous() {
        // "id" appears in two tables — ambiguous, don't mark as PK
        let tables = vec![
            make_table_with_pk("users", vec![("id", true)]),
            make_table_with_pk("orders", vec![("id", true)]),
        ];
        let mut views = vec![make_view("my_view", vec!["id"])];
        resolve_view_primary_keys(&mut views, &tables);
        assert!(!views[0].columns[0].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_no_match() {
        let tables = vec![make_table_with_pk("users", vec![("id", true)])];
        let mut views = vec![make_view("my_view", vec!["computed"])];
        resolve_view_primary_keys(&mut views, &tables);
        assert!(!views[0].columns[0].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_empty_tables() {
        let mut views = vec![make_view("my_view", vec!["id"])];
        resolve_view_primary_keys(&mut views, &[]);
        assert!(!views[0].columns[0].is_primary_key);
    }

    // ========== parse_check_in_variants ==========

    #[test]
    fn test_parse_check_in_simple() {
        let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT CHECK (status IN ('active', 'inactive')) NOT NULL)";
        assert_eq!(
            parse_check_in_variants(ddl, "status"),
            Some(vec!["active".to_string(), "inactive".to_string()])
        );
    }

    #[test]
    fn test_parse_check_in_three_variants() {
        let ddl = "CREATE TABLE t (priority TEXT CHECK (priority IN ('low','medium','high')))";
        assert_eq!(
            parse_check_in_variants(ddl, "priority"),
            Some(vec![
                "low".to_string(),
                "medium".to_string(),
                "high".to_string()
            ])
        );
    }

    #[test]
    fn test_parse_check_in_returns_none_for_other_column() {
        let ddl = "CREATE TABLE t (status TEXT CHECK (status IN ('a','b')))";
        assert_eq!(parse_check_in_variants(ddl, "other"), None);
    }

    #[test]
    fn test_parse_check_in_returns_none_without_check() {
        let ddl = "CREATE TABLE t (status TEXT)";
        assert_eq!(parse_check_in_variants(ddl, "status"), None);
    }

    #[test]
    fn test_parse_check_in_case_insensitive_keyword() {
        let ddl = "CREATE TABLE t (status TEXT check (Status in ('a','b')))";
        assert_eq!(
            parse_check_in_variants(ddl, "status"),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }
}
