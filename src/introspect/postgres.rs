use anyhow::Result;
use sqlx::PgPool;

use super::{ColumnInfo, CompositeTypeInfo, DomainInfo, EnumInfo, SchemaInfo, TableInfo};

pub async fn introspect(pool: &PgPool, schemas: &[String]) -> Result<SchemaInfo> {
    let tables = fetch_tables(pool, schemas).await?;
    let enums = fetch_enums(pool, schemas).await?;
    let composite_types = fetch_composite_types(pool, schemas).await?;
    let domains = fetch_domains(pool, schemas).await?;

    Ok(SchemaInfo {
        tables,
        enums,
        composite_types,
        domains,
    })
}

async fn fetch_tables(pool: &PgPool, schemas: &[String]) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, String, String, i32)>(
        r#"
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            COALESCE(c.udt_name, c.data_type) as udt_name,
            c.is_nullable,
            c.ordinal_position
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
            AND t.table_type = 'BASE TABLE'
        WHERE c.table_schema = ANY($1)
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        "#,
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;

    let mut tables: Vec<TableInfo> = Vec::new();
    let mut current_key: Option<(String, String)> = None;

    for (schema, table, col_name, data_type, udt_name, nullable, ordinal) in rows {
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
            ordinal_position: ordinal,
            schema_name: schema,
        });
    }

    Ok(tables)
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
            ordinal_position: ordinal as i32,
            schema_name: schema,
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
