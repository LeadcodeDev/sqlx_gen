use std::collections::HashMap;

use crate::error::Result;
use sqlx::SqlitePool;

use super::{ColumnInfo, SchemaInfo, TableInfo};

pub async fn introspect(pool: &SqlitePool, include_views: bool) -> Result<SchemaInfo> {
    let tables = fetch_tables(pool).await?;
    let mut views = if include_views {
        fetch_views(pool).await?
    } else {
        Vec::new()
    };

    if !views.is_empty() {
        resolve_view_nullability(&mut views, &tables);
    }

    Ok(SchemaInfo {
        tables,
        views,
        enums: Vec::new(),
        composite_types: Vec::new(),
        domains: Vec::new(),
    })
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
    let view_names: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name",
    )
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
            col_lookup.entry(&col.name).or_default().push(col.is_nullable);
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
}
