use std::collections::HashMap;

use crate::error::Result;
use sqlx::PgPool;

use super::{ColumnInfo, CompositeTypeInfo, DomainInfo, EnumInfo, SchemaInfo, TableInfo};

pub async fn introspect(
    pool: &PgPool,
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
        let nullability_info = fetch_view_column_nullability(pool, schemas).await?;
        resolve_view_nullability(&mut views, &nullability_info);

        let pk_info = fetch_view_column_primary_keys(pool, schemas).await?;
        resolve_view_primary_keys(&mut views, &pk_info);
    }

    let enums = fetch_enums(pool, schemas).await?;
    let composite_types = fetch_composite_types(pool, schemas).await?;
    let domains = fetch_domains(pool, schemas).await?;

    Ok(SchemaInfo {
        tables,
        views,
        enums,
        composite_types,
        domains,
    })
}

async fn fetch_tables(pool: &PgPool, schemas: &[String]) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, String, String, i32, bool, Option<String>)>(
        r#"
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            COALESCE(c.udt_name, c.data_type) as udt_name,
            c.is_nullable,
            c.ordinal_position,
            CASE WHEN kcu.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key,
            c.column_default
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
            AND t.table_type = 'BASE TABLE'
        LEFT JOIN information_schema.table_constraints tc
            ON tc.table_schema = c.table_schema
            AND tc.table_name = c.table_name
            AND tc.constraint_type = 'PRIMARY KEY'
        LEFT JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.constraint_schema = tc.constraint_schema
            AND kcu.column_name = c.column_name
        WHERE c.table_schema = ANY($1)
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    let mut tables: Vec<TableInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, table, col_name, data_type, udt_name, nullable, ordinal, is_pk, column_default) in rows {
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
            udt_name,
            is_nullable: nullable == "YES",
            is_primary_key: is_pk,
            ordinal_position: ordinal,
            schema_name: schema,
            column_default,
        });
    }

    Ok(tables)
}

async fn fetch_views(pool: &PgPool, schemas: &[String]) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, String, String, i32, Option<String>)>(
        r#"
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            COALESCE(c.udt_name, c.data_type) as udt_name,
            c.is_nullable,
            c.ordinal_position,
            c.column_default
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
            AND t.table_type = 'VIEW'
        WHERE c.table_schema = ANY($1)
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    let mut views: Vec<TableInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, table, col_name, data_type, udt_name, nullable, ordinal, column_default) in rows {
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
            udt_name,
            is_nullable: nullable == "YES",
            is_primary_key: false,
            ordinal_position: ordinal,
            schema_name: schema,
            column_default,
        });
    }

    Ok(views)
}

struct ViewColumnNullability {
    view_schema: String,
    view_name: String,
    source_column_name: String,
    source_not_null: bool,
}

async fn fetch_view_column_nullability(
    pool: &PgPool,
    schemas: &[String],
) -> Result<Vec<ViewColumnNullability>> {
    let rows = sqlx::query_as::<_, (String, String, String, bool)>(
        r#"
        SELECT DISTINCT
            v_ns.nspname AS view_schema,
            v.relname AS view_name,
            src_attr.attname AS source_column_name,
            src_attr.attnotnull AS source_not_null
        FROM pg_class v
        JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
        JOIN pg_rewrite rw ON rw.ev_class = v.oid
        JOIN pg_depend d ON d.objid = rw.oid
            AND d.classid = 'pg_rewrite'::regclass
            AND d.refobjsubid > 0
            AND d.deptype = 'n'
        JOIN pg_attribute src_attr ON src_attr.attrelid = d.refobjid
            AND src_attr.attnum = d.refobjsubid
            AND NOT src_attr.attisdropped
        WHERE v_ns.nspname = ANY($1)
          AND v.relkind = 'v'
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(view_schema, view_name, source_column_name, source_not_null)| {
                ViewColumnNullability {
                    view_schema,
                    view_name,
                    source_column_name,
                    source_not_null,
                }
            },
        )
        .collect())
}

fn resolve_view_nullability(
    views: &mut [TableInfo],
    nullability_info: &[ViewColumnNullability],
) {
    // Build lookup: (view_schema, view_name, column_name) -> Vec<is_not_null>
    let mut lookup: HashMap<(&str, &str, &str), Vec<bool>> = HashMap::new();
    for info in nullability_info {
        lookup
            .entry((&info.view_schema, &info.view_name, &info.source_column_name))
            .or_default()
            .push(info.source_not_null);
    }

    for view in views.iter_mut() {
        for col in view.columns.iter_mut() {
            if let Some(not_null_flags) = lookup.get(&(
                view.schema_name.as_str(),
                view.name.as_str(),
                col.name.as_str(),
            )) {
                // Only mark as non-nullable if ALL source columns are NOT NULL
                if !not_null_flags.is_empty() && not_null_flags.iter().all(|&nn| nn) {
                    col.is_nullable = false;
                }
            }
        }
    }
}

struct ViewColumnPrimaryKey {
    view_schema: String,
    view_name: String,
    source_column_name: String,
    source_is_pk: bool,
}

async fn fetch_view_column_primary_keys(
    pool: &PgPool,
    schemas: &[String],
) -> Result<Vec<ViewColumnPrimaryKey>> {
    let rows = sqlx::query_as::<_, (String, String, String, bool)>(
        r#"
        SELECT DISTINCT
            v_ns.nspname AS view_schema,
            v.relname AS view_name,
            src_attr.attname AS source_column_name,
            COALESCE(
                EXISTS (
                    SELECT 1
                    FROM pg_constraint con
                    WHERE con.conrelid = src_attr.attrelid
                      AND con.contype = 'p'
                      AND src_attr.attnum = ANY(con.conkey)
                ),
                false
            ) AS source_is_pk
        FROM pg_class v
        JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
        JOIN pg_rewrite rw ON rw.ev_class = v.oid
        JOIN pg_depend d ON d.objid = rw.oid
            AND d.classid = 'pg_rewrite'::regclass
            AND d.refobjsubid > 0
            AND d.deptype = 'n'
        JOIN pg_attribute src_attr ON src_attr.attrelid = d.refobjid
            AND src_attr.attnum = d.refobjsubid
            AND NOT src_attr.attisdropped
        WHERE v_ns.nspname = ANY($1)
          AND v.relkind = 'v'
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(view_schema, view_name, source_column_name, source_is_pk)| ViewColumnPrimaryKey {
                view_schema,
                view_name,
                source_column_name,
                source_is_pk,
            },
        )
        .collect())
}

fn resolve_view_primary_keys(
    views: &mut [TableInfo],
    pk_info: &[ViewColumnPrimaryKey],
) {
    // Build lookup: (view_schema, view_name, column_name) -> Vec<is_pk>
    let mut lookup: HashMap<(&str, &str, &str), Vec<bool>> = HashMap::new();
    for info in pk_info {
        lookup
            .entry((&info.view_schema, &info.view_name, &info.source_column_name))
            .or_default()
            .push(info.source_is_pk);
    }

    for view in views.iter_mut() {
        for col in view.columns.iter_mut() {
            if let Some(pk_flags) = lookup.get(&(
                view.schema_name.as_str(),
                view.name.as_str(),
                col.name.as_str(),
            )) {
                // Only mark as PK if ALL source columns are PKs
                if !pk_flags.is_empty() && pk_flags.iter().all(|&pk| pk) {
                    col.is_primary_key = true;
                }
            }
        }
    }
}

async fn fetch_enums(pool: &PgPool, schemas: &[String]) -> Result<Vec<EnumInfo>> {
    let rows = sqlx::query_as::<_, (String, String, String)>(
        r#"
        SELECT
            n.nspname AS schema_name,
            t.typname AS enum_name,
            e.enumlabel AS variant
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = ANY($1)
        ORDER BY n.nspname, t.typname, e.enumsortorder
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    let mut enums: Vec<EnumInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, name, variant) in rows {
        let key = (schema.clone(), name.clone());
        if current_key.as_ref() != Some(&key) {
            current_key = Some(key);
            enums.push(EnumInfo {
                schema_name: schema,
                name,
                variants: Vec::new(),
                default_variant: None,
            });
        }
        enums.last_mut().unwrap().variants.push(variant);
    }

    Ok(enums)
}

async fn fetch_composite_types(
    pool: &PgPool,
    schemas: &[String],
) -> Result<Vec<CompositeTypeInfo>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, String, i32)>(
        r#"
        SELECT
            n.nspname AS schema_name,
            t.typname AS type_name,
            a.attname AS field_name,
            COALESCE(ft.typname, '') AS field_type,
            CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
            a.attnum AS ordinal
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        JOIN pg_catalog.pg_type ft ON ft.oid = a.atttypid
        WHERE t.typtype = 'c'
            AND n.nspname = ANY($1)
            AND NOT EXISTS (
                SELECT 1 FROM information_schema.tables it
                WHERE it.table_schema = n.nspname AND it.table_name = t.typname
            )
        ORDER BY n.nspname, t.typname, a.attnum
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    let mut composites: Vec<CompositeTypeInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, type_name, field_name, field_type, nullable, ordinal) in rows {
        let key = (schema.clone(), type_name.clone());
        if current_key.as_ref() != Some(&key) {
            current_key = Some(key);
            composites.push(CompositeTypeInfo {
                schema_name: schema.clone(),
                name: type_name,
                fields: Vec::new(),
            });
        }
        composites.last_mut().unwrap().fields.push(ColumnInfo {
            name: field_name,
            data_type: field_type.clone(),
            udt_name: field_type,
            is_nullable: nullable == "YES",
            is_primary_key: false,
            ordinal_position: ordinal,
            schema_name: schema,
            column_default: None,
        });
    }

    Ok(composites)
}

async fn fetch_domains(pool: &PgPool, schemas: &[String]) -> Result<Vec<DomainInfo>> {
    let rows = sqlx::query_as::<_, (String, String, String)>(
        r#"
        SELECT
            n.nspname AS schema_name,
            t.typname AS domain_name,
            bt.typname AS base_type
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_catalog.pg_type bt ON bt.oid = t.typbasetype
        WHERE t.typtype = 'd'
            AND n.nspname = ANY($1)
        ORDER BY n.nspname, t.typname
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(schema, name, base_type)| DomainInfo {
            schema_name: schema,
            name,
            base_type,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_view(schema: &str, name: &str, columns: Vec<&str>) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            name: name.to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, col)| ColumnInfo {
                    name: col.to_string(),
                    data_type: "text".to_string(),
                    udt_name: "text".to_string(),
                    is_nullable: true,
                    is_primary_key: false,
                    ordinal_position: i as i32,
                    schema_name: schema.to_string(),
                    column_default: None,
                })
                .collect(),
        }
    }

    fn make_nullability(
        view_schema: &str,
        view_name: &str,
        source_column: &str,
        not_null: bool,
    ) -> ViewColumnNullability {
        ViewColumnNullability {
            view_schema: view_schema.to_string(),
            view_name: view_name.to_string(),
            source_column_name: source_column.to_string(),
            source_not_null: not_null,
        }
    }

    #[test]
    fn test_resolve_not_null_column() {
        let mut views = vec![make_view("public", "my_view", vec!["id", "name"])];
        let info = vec![
            make_nullability("public", "my_view", "id", true),
            make_nullability("public", "my_view", "name", true),
        ];
        resolve_view_nullability(&mut views, &info);
        assert!(!views[0].columns[0].is_nullable);
        assert!(!views[0].columns[1].is_nullable);
    }

    #[test]
    fn test_resolve_mixed_sources() {
        let mut views = vec![make_view("public", "my_view", vec!["id"])];
        let info = vec![
            make_nullability("public", "my_view", "id", true),
            make_nullability("public", "my_view", "id", false),
        ];
        resolve_view_nullability(&mut views, &info);
        assert!(views[0].columns[0].is_nullable);
    }

    #[test]
    fn test_resolve_no_match_stays_nullable() {
        let mut views = vec![make_view("public", "my_view", vec!["computed_col"])];
        let info = vec![make_nullability("public", "my_view", "id", true)];
        resolve_view_nullability(&mut views, &info);
        assert!(views[0].columns[0].is_nullable);
    }

    #[test]
    fn test_resolve_empty_info() {
        let mut views = vec![make_view("public", "my_view", vec!["id"])];
        resolve_view_nullability(&mut views, &[]);
        assert!(views[0].columns[0].is_nullable);
    }

    #[test]
    fn test_resolve_cross_schema() {
        let mut views = vec![
            make_view("public", "v1", vec!["id"]),
            make_view("auth", "v2", vec!["id"]),
        ];
        let info = vec![
            make_nullability("public", "v1", "id", true),
            make_nullability("auth", "v2", "id", false),
        ];
        resolve_view_nullability(&mut views, &info);
        assert!(!views[0].columns[0].is_nullable);
        assert!(views[1].columns[0].is_nullable);
    }

    // --- resolve_view_primary_keys tests ---

    fn make_pk_info(
        view_schema: &str,
        view_name: &str,
        source_column: &str,
        is_pk: bool,
    ) -> ViewColumnPrimaryKey {
        ViewColumnPrimaryKey {
            view_schema: view_schema.to_string(),
            view_name: view_name.to_string(),
            source_column_name: source_column.to_string(),
            source_is_pk: is_pk,
        }
    }

    #[test]
    fn test_resolve_pk_column() {
        let mut views = vec![make_view("public", "my_view", vec!["id", "name"])];
        let info = vec![
            make_pk_info("public", "my_view", "id", true),
            make_pk_info("public", "my_view", "name", false),
        ];
        resolve_view_primary_keys(&mut views, &info);
        assert!(views[0].columns[0].is_primary_key);
        assert!(!views[0].columns[1].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_mixed_sources() {
        let mut views = vec![make_view("public", "my_view", vec!["id"])];
        let info = vec![
            make_pk_info("public", "my_view", "id", true),
            make_pk_info("public", "my_view", "id", false),
        ];
        resolve_view_primary_keys(&mut views, &info);
        assert!(!views[0].columns[0].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_no_match() {
        let mut views = vec![make_view("public", "my_view", vec!["computed_col"])];
        let info = vec![make_pk_info("public", "my_view", "id", true)];
        resolve_view_primary_keys(&mut views, &info);
        assert!(!views[0].columns[0].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_empty_info() {
        let mut views = vec![make_view("public", "my_view", vec!["id"])];
        resolve_view_primary_keys(&mut views, &[]);
        assert!(!views[0].columns[0].is_primary_key);
    }

    #[test]
    fn test_resolve_pk_cross_schema() {
        let mut views = vec![
            make_view("public", "v1", vec!["id"]),
            make_view("auth", "v2", vec!["id"]),
        ];
        let info = vec![
            make_pk_info("public", "v1", "id", true),
            make_pk_info("auth", "v2", "id", false),
        ];
        resolve_view_primary_keys(&mut views, &info);
        assert!(views[0].columns[0].is_primary_key);
        assert!(!views[1].columns[0].is_primary_key);
    }
}
