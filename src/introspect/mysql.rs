use anyhow::Result;
use sqlx::MySqlPool;

use super::{ColumnInfo, EnumInfo, SchemaInfo, TableInfo};

pub async fn introspect(pool: &MySqlPool, schemas: &[String]) -> Result<SchemaInfo> {
    let tables = fetch_tables(pool, schemas).await?;
    let enums = extract_enums(&tables);

    Ok(SchemaInfo {
        tables,
        enums,
        composite_types: Vec::new(),
        domains: Vec::new(),
    })
}

async fn fetch_tables(pool: &MySqlPool, schemas: &[String]) -> Result<Vec<TableInfo>> {
    // MySQL doesn't support binding arrays directly, so we build placeholders
    let placeholders: Vec<String> = (0..schemas.len()).map(|_| "?".to_string()).collect();
    let query = format!(
        r#"
        SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.COLUMN_TYPE,
            c.IS_NULLABLE,
            c.ORDINAL_POSITION
        FROM information_schema.COLUMNS c
        JOIN information_schema.TABLES t
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND t.TABLE_NAME = c.TABLE_NAME
            AND t.TABLE_TYPE = 'BASE TABLE'
        WHERE c.TABLE_SCHEMA IN ({})
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
        "#,
        placeholders.join(",")
    );

    let mut q = sqlx::query_as::<_, (String, String, String, String, String, String, u32)>(&query);
    for schema in schemas {
        q = q.bind(schema);
    }
    let rows = q.fetch_all(pool).await?;

    let mut tables: Vec<TableInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, table, col_name, data_type, column_type, nullable, ordinal) in rows {
        let key = (schema.clone(), table.clone());
        if current_key.as_ref() != Some(&key) {
            current_key = Some(key);
            tables.push(TableInfo {
                schema_name: schema.clone(),
                name: table.clone(),
                columns: Vec::new(),
            });
        }
        tables.last_mut().unwrap().columns.push(ColumnInfo {
            name: col_name,
            data_type,
            udt_name: column_type,
            is_nullable: nullable == "YES",
            ordinal_position: ordinal as i32,
            schema_name: schema,
        });
    }

    Ok(tables)
}

/// Extract inline ENUMs from column types.
/// MySQL ENUM('a','b','c') in COLUMN_TYPE gets extracted to an EnumInfo
/// keyed by table_name + column_name.
fn extract_enums(tables: &[TableInfo]) -> Vec<EnumInfo> {
    let mut enums = Vec::new();

    for table in tables {
        for col in &table.columns {
            if col.udt_name.starts_with("enum(") {
                let variants = parse_enum_variants(&col.udt_name);
                if !variants.is_empty() {
                    let enum_name = format!("{}_{}", table.name, col.name);
                    enums.push(EnumInfo {
                        schema_name: table.schema_name.clone(),
                        name: enum_name,
                        variants,
                    });
                }
            }
        }
    }

    enums
}

fn parse_enum_variants(column_type: &str) -> Vec<String> {
    // Parse "enum('a','b','c')" → ["a", "b", "c"]
    let inner = column_type
        .strip_prefix("enum(")
        .and_then(|s| s.strip_suffix(')'));
    match inner {
        Some(s) => s
            .split(',')
            .map(|v| v.trim().trim_matches('\'').to_string())
            .filter(|v| !v.is_empty())
            .collect(),
        None => Vec::new(),
    }
}
