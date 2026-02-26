use crate::error::Result;
use sqlx::SqlitePool;

use super::{ColumnInfo, SchemaInfo, TableInfo};

pub async fn introspect(pool: &SqlitePool, include_views: bool) -> Result<SchemaInfo> {
    let tables = fetch_tables(pool).await?;
    let views = if include_views {
        fetch_views(pool).await?
    } else {
        Vec::new()
    };

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
