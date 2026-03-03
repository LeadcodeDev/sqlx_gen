use std::collections::HashMap;

use crate::error::Result;
use sqlx::MySqlPool;

use super::{ColumnInfo, EnumInfo, SchemaInfo, TableInfo};

pub async fn introspect(
    pool: &MySqlPool,
    schemas: &[String],
    include_views: bool,
) -> Result<SchemaInfo> {
    let tables = fetch_tables(pool, schemas).await?;
    let mut views = if include_views {
        fetch_views(pool, schemas).await?
    } else {
        Vec::new()
    };

    if !views.is_empty() {
        let sources = fetch_view_column_sources(pool, schemas).await?;
        resolve_view_nullability(&mut views, &sources, &tables);
        resolve_view_primary_keys(&mut views, &sources, &tables);
    }

    let enums = extract_enums(&tables);

    Ok(SchemaInfo {
        tables,
        views,
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
            c.ORDINAL_POSITION,
            c.COLUMN_KEY
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

    let mut q = sqlx::query_as::<_, (String, String, String, String, String, String, u32, String)>(&query);
    for schema in schemas {
        q = q.bind(schema);
    }
    let rows = q.fetch_all(pool).await?;

    let mut tables: Vec<TableInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, table, col_name, data_type, column_type, nullable, ordinal, column_key) in rows {
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
            is_primary_key: column_key == "PRI",
            ordinal_position: ordinal as i32,
            schema_name: schema,
            column_default: None,
        });
    }

    Ok(tables)
}

async fn fetch_views(pool: &MySqlPool, schemas: &[String]) -> Result<Vec<TableInfo>> {
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
            AND t.TABLE_TYPE = 'VIEW'
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

    let mut views: Vec<TableInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, table, col_name, data_type, column_type, nullable, ordinal) in rows {
        let key = (schema.clone(), table.clone());
        if current_key.as_ref() != Some(&key) {
            current_key = Some(key);
            views.push(TableInfo {
                schema_name: schema.clone(),
                name: table.clone(),
                columns: Vec::new(),
            });
        }
        views.last_mut().unwrap().columns.push(ColumnInfo {
            name: col_name,
            data_type,
            udt_name: column_type,
            is_nullable: nullable == "YES",
            is_primary_key: false,
            ordinal_position: ordinal as i32,
            schema_name: schema,
            column_default: None,
        });
    }

    Ok(views)
}

struct ViewColumnSource {
    view_schema: String,
    view_name: String,
    table_schema: String,
    table_name: String,
    column_name: String,
}

async fn fetch_view_column_sources(
    pool: &MySqlPool,
    schemas: &[String],
) -> Result<Vec<ViewColumnSource>> {
    let placeholders: Vec<String> = (0..schemas.len()).map(|_| "?".to_string()).collect();
    let query = format!(
        r#"
        SELECT
            vcu.VIEW_SCHEMA,
            vcu.VIEW_NAME,
            vcu.TABLE_SCHEMA,
            vcu.TABLE_NAME,
            vcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.VIEW_COLUMN_USAGE vcu
        WHERE vcu.VIEW_SCHEMA IN ({})
        "#,
        placeholders.join(",")
    );

    let mut q = sqlx::query_as::<_, (String, String, String, String, String)>(&query);
    for schema in schemas {
        q = q.bind(schema);
    }

    match q.fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(
                |(view_schema, view_name, table_schema, table_name, column_name)| {
                    ViewColumnSource {
                        view_schema,
                        view_name,
                        table_schema,
                        table_name,
                        column_name,
                    }
                },
            )
            .collect()),
        Err(_) => {
            // VIEW_COLUMN_USAGE may not exist on older MySQL versions
            Ok(Vec::new())
        }
    }
}

fn resolve_view_nullability(
    views: &mut [TableInfo],
    sources: &[ViewColumnSource],
    tables: &[TableInfo],
) {
    // Build table column lookup: (schema, table, column) -> is_nullable
    let mut table_lookup: HashMap<(&str, &str, &str), bool> = HashMap::new();
    for table in tables {
        for col in &table.columns {
            table_lookup.insert(
                (&table.schema_name, &table.name, &col.name),
                col.is_nullable,
            );
        }
    }

    // Build view column source lookup: (view_schema, view_name, column_name) -> Vec<is_nullable>
    let mut view_lookup: HashMap<(&str, &str, &str), Vec<bool>> = HashMap::new();
    for src in sources {
        if let Some(&is_nullable) =
            table_lookup.get(&(src.table_schema.as_str(), src.table_name.as_str(), src.column_name.as_str()))
        {
            view_lookup
                .entry((&src.view_schema, &src.view_name, &src.column_name))
                .or_default()
                .push(is_nullable);
        }
    }

    for view in views.iter_mut() {
        for col in view.columns.iter_mut() {
            if let Some(nullable_flags) = view_lookup.get(&(
                view.schema_name.as_str(),
                view.name.as_str(),
                col.name.as_str(),
            )) {
                // Only mark as non-nullable if ALL sources are NOT nullable
                if !nullable_flags.is_empty() && nullable_flags.iter().all(|&n| !n) {
                    col.is_nullable = false;
                }
            }
        }
    }
}

fn resolve_view_primary_keys(
    views: &mut [TableInfo],
    sources: &[ViewColumnSource],
    tables: &[TableInfo],
) {
    // Build table column lookup: (schema, table, column) -> is_primary_key
    let mut table_lookup: HashMap<(&str, &str, &str), bool> = HashMap::new();
    for table in tables {
        for col in &table.columns {
            table_lookup.insert(
                (&table.schema_name, &table.name, &col.name),
                col.is_primary_key,
            );
        }
    }

    // Build view column source lookup: (view_schema, view_name, column_name) -> Vec<is_pk>
    let mut view_lookup: HashMap<(&str, &str, &str), Vec<bool>> = HashMap::new();
    for src in sources {
        if let Some(&is_pk) =
            table_lookup.get(&(src.table_schema.as_str(), src.table_name.as_str(), src.column_name.as_str()))
        {
            view_lookup
                .entry((&src.view_schema, &src.view_name, &src.column_name))
                .or_default()
                .push(is_pk);
        }
    }

    for view in views.iter_mut() {
        for col in view.columns.iter_mut() {
            if let Some(pk_flags) = view_lookup.get(&(
                view.schema_name.as_str(),
                view.name.as_str(),
                col.name.as_str(),
            )) {
                // Only mark as PK if ALL sources are PKs
                if !pk_flags.is_empty() && pk_flags.iter().all(|&pk| pk) {
                    col.is_primary_key = true;
                }
            }
        }
    }
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
                        default_variant: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema_name: "test_db".to_string(),
            name: name.to_string(),
            columns,
        }
    }

    fn make_col(name: &str, udt_name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: "varchar".to_string(),
            udt_name: udt_name.to_string(),
            is_nullable: false,
            is_primary_key: false,
            ordinal_position: 0,
            schema_name: "test_db".to_string(),
            column_default: None,
        }
    }

    // ========== parse_enum_variants ==========

    #[test]
    fn test_parse_simple() {
        assert_eq!(
            parse_enum_variants("enum('a','b','c')"),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn test_parse_single_variant() {
        assert_eq!(parse_enum_variants("enum('only')"), vec!["only"]);
    }

    #[test]
    fn test_parse_with_spaces() {
        assert_eq!(
            parse_enum_variants("enum( 'a' , 'b' )"),
            vec!["a", "b"]
        );
    }

    #[test]
    fn test_parse_empty_parens() {
        let result = parse_enum_variants("enum()");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_varchar_not_enum() {
        let result = parse_enum_variants("varchar(255)");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_int_not_enum() {
        let result = parse_enum_variants("int");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_with_spaces_in_value() {
        assert_eq!(
            parse_enum_variants("enum('with space','no')"),
            vec!["with space", "no"]
        );
    }

    #[test]
    fn test_parse_empty_variant_filtered() {
        let result = parse_enum_variants("enum('a','','c')");
        assert_eq!(result, vec!["a", "c"]);
    }

    #[test]
    fn test_parse_uppercase_enum_not_matched() {
        // "ENUM(" doesn't match "enum(" prefix
        let result = parse_enum_variants("ENUM('a','b')");
        assert!(result.is_empty());
    }

    // ========== extract_enums ==========

    #[test]
    fn test_extract_from_enum_column() {
        let tables = vec![make_table(
            "users",
            vec![make_col("status", "enum('active','inactive')")],
        )];
        let enums = extract_enums(&tables);
        assert_eq!(enums.len(), 1);
        assert_eq!(enums[0].variants, vec!["active", "inactive"]);
    }

    #[test]
    fn test_extract_enum_name_format() {
        let tables = vec![make_table(
            "users",
            vec![make_col("status", "enum('a')")],
        )];
        let enums = extract_enums(&tables);
        assert_eq!(enums[0].name, "users_status");
    }

    #[test]
    fn test_extract_no_enums() {
        let tables = vec![make_table(
            "users",
            vec![make_col("id", "int"), make_col("name", "varchar(255)")],
        )];
        let enums = extract_enums(&tables);
        assert!(enums.is_empty());
    }

    #[test]
    fn test_extract_two_enum_columns_same_table() {
        let tables = vec![make_table(
            "users",
            vec![
                make_col("status", "enum('active','inactive')"),
                make_col("role", "enum('admin','user')"),
            ],
        )];
        let enums = extract_enums(&tables);
        assert_eq!(enums.len(), 2);
        assert_eq!(enums[0].name, "users_status");
        assert_eq!(enums[1].name, "users_role");
    }

    #[test]
    fn test_extract_enums_from_multiple_tables() {
        let tables = vec![
            make_table("users", vec![make_col("status", "enum('a')")]),
            make_table("posts", vec![make_col("state", "enum('b')")]),
        ];
        let enums = extract_enums(&tables);
        assert_eq!(enums.len(), 2);
    }

    #[test]
    fn test_extract_non_enum_column_ignored() {
        let tables = vec![make_table(
            "users",
            vec![
                make_col("id", "int(11)"),
                make_col("status", "enum('a')"),
            ],
        )];
        let enums = extract_enums(&tables);
        assert_eq!(enums.len(), 1);
    }

    // ========== resolve_view_nullability ==========

    fn make_view(schema: &str, name: &str, columns: Vec<&str>) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, col)| ColumnInfo {
                    name: col.to_string(),
                    data_type: "varchar".to_string(),
                    udt_name: "varchar(255)".to_string(),
                    is_nullable: true,
                    is_primary_key: false,
                    ordinal_position: i as i32,
                    schema_name: schema.to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    fn make_table_with_nullability(
        schema: &str,
        name: &str,
        columns: Vec<(&str, bool)>,
    ) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (col, nullable))| ColumnInfo {
                    name: col.to_string(),
                    data_type: "varchar".to_string(),
                    udt_name: "varchar(255)".to_string(),
                    is_nullable: nullable,
                    is_primary_key: false,
                    ordinal_position: i as i32,
                    schema_name: schema.to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    fn make_source(
        view_schema: &str,
        view_name: &str,
        table_schema: &str,
        table_name: &str,
        column_name: &str,
    ) -> ViewColumnSource {
        ViewColumnSource {
            view_schema: view_schema.to_string(),
            view_name: view_name.to_string(),
            table_schema: table_schema.to_string(),
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
        }
    }

    #[test]
    fn test_resolve_not_null_column() {
        let tables = vec![make_table_with_nullability(
            "db",
            "users",
            vec![("id", false), ("name", false)],
        )];
        let mut views = vec![make_view("db", "my_view", vec!["id", "name"])];
        let sources = vec![
            make_source("db", "my_view", "db", "users", "id"),
            make_source("db", "my_view", "db", "users", "name"),
        ];
        resolve_view_nullability(&mut views, &sources, &tables);
        assert!(!views[0].columns[0].is_nullable);
        assert!(!views[0].columns[1].is_nullable);
    }

    #[test]
    fn test_resolve_nullable_source() {
        let tables = vec![make_table_with_nullability(
            "db",
            "users",
            vec![("id", false), ("name", true)],
        )];
        let mut views = vec![make_view("db", "my_view", vec!["id", "name"])];
        let sources = vec![
            make_source("db", "my_view", "db", "users", "id"),
            make_source("db", "my_view", "db", "users", "name"),
        ];
        resolve_view_nullability(&mut views, &sources, &tables);
        assert!(!views[0].columns[0].is_nullable);
        assert!(views[0].columns[1].is_nullable);
    }

    #[test]
    fn test_resolve_no_match_stays_nullable() {
        let tables = vec![make_table_with_nullability(
            "db",
            "users",
            vec![("id", false)],
        )];
        let mut views = vec![make_view("db", "my_view", vec!["computed"])];
        let sources = vec![];
        resolve_view_nullability(&mut views, &sources, &tables);
        assert!(views[0].columns[0].is_nullable);
    }

    #[test]
    fn test_resolve_empty_sources() {
        let tables = vec![];
        let mut views = vec![make_view("db", "my_view", vec!["id"])];
        resolve_view_nullability(&mut views, &[], &tables);
        assert!(views[0].columns[0].is_nullable);
    }

    // ========== resolve_view_primary_keys ==========

    fn make_table_with_pk(
        schema: &str,
        name: &str,
        columns: Vec<(&str, bool)>,
    ) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (col, is_pk))| ColumnInfo {
                    name: col.to_string(),
                    data_type: "varchar".to_string(),
                    udt_name: "varchar(255)".to_string(),
                    is_nullable: false,
                    is_primary_key: is_pk,
                    ordinal_position: i as i32,
                    schema_name: schema.to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    #[test]
    fn test_resolve_pk_column() {
        let tables = vec![make_table_with_pk("db", "users", vec![("id", true), ("name", false)])];
        let mut views = vec![make_view("db", "my_view", vec!["id", "name"])];
        let sources = vec![
            make_source("db", "my_view", "db", "users", "id"),
            make_source("db", "my_view", "db", "users", "name"),
        ];
        resolve_view_primary_keys(&mut views, &sources, &tables);
        assert!(views[0].columns[0].is_primary_key);
        assert!(!views[0].columns[1].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_no_sources() {
        let tables = vec![make_table_with_pk("db", "users", vec![("id", true)])];
        let mut views = vec![make_view("db", "my_view", vec!["id"])];
        resolve_view_primary_keys(&mut views, &[], &tables);
        assert!(!views[0].columns[0].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_no_match() {
        let tables = vec![make_table_with_pk("db", "users", vec![("id", true)])];
        let mut views = vec![make_view("db", "my_view", vec!["computed"])];
        let sources = vec![];
        resolve_view_primary_keys(&mut views, &sources, &tables);
        assert!(!views[0].columns[0].is_primary_key);
    }
}
