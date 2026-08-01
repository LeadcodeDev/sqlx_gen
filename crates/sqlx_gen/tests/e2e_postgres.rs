//! Postgres end-to-end tests.
//!
//! These need a live server, so they are `#[ignore]`d by default. Set `PG_URL`
//! and run them with `--include-ignored` — which is exactly what the
//! `test-postgres` CI job does:
//!
//! ```text
//! PG_URL=postgres://postgres:postgres@localhost:5432/sqlx_gen_test \
//!     cargo test --all-features --test e2e_postgres -- --include-ignored
//! ```

use std::collections::HashMap;
use std::env;

use sqlx::PgPool;
use sqlx_gen::cli::{DatabaseKind, TimeCrate};
use sqlx_gen::codegen::{self, GeneratedFile};
use sqlx_gen::introspect::postgres::introspect;
use sqlx_gen::introspect::SchemaInfo;

/// Connects and hands the test a private schema, so tests running in parallel
/// can't see each other's tables and types.
async fn setup_schema(name: &str) -> PgPool {
    let url = env::var("PG_URL")
        .expect("PG_URL must be set to run the Postgres e2e tests (see the module docs)");
    let pool = PgPool::connect(&url).await.unwrap();
    exec(&pool, &format!("DROP SCHEMA IF EXISTS {name} CASCADE")).await;
    exec(&pool, &format!("CREATE SCHEMA {name}")).await;
    pool
}

async fn exec(pool: &PgPool, sql: &str) {
    sqlx::query(sql).execute(pool).await.unwrap();
}

fn generate(schema: &SchemaInfo) -> Vec<GeneratedFile> {
    codegen::generate(
        schema,
        DatabaseKind::Postgres,
        &[],
        &HashMap::new(),
        false,
        TimeCrate::Chrono,
    )
    .unwrap()
}

// --- tables ---

#[tokio::test]
#[ignore = "needs a live Postgres (PG_URL)"]
async fn test_simple_table_generates_struct() {
    let pool = setup_schema("e2e_table").await;
    exec(
        &pool,
        "CREATE TABLE e2e_table.users (id INT PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;

    let schema = introspect(&pool, &["e2e_table".to_string()], false)
        .await
        .unwrap();

    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("table `users` should be introspected");
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[0].ordinal_position, 1);
    assert_eq!(table.columns[1].ordinal_position, 2);

    assert!(generate(&schema)[0].code.contains("pub struct User"));
}

// --- enums ---

#[tokio::test]
#[ignore = "needs a live Postgres (PG_URL)"]
async fn test_enum_is_introspected() {
    let pool = setup_schema("e2e_enum").await;
    exec(&pool, "CREATE TYPE e2e_enum.mood AS ENUM ('sad', 'happy')").await;

    let schema = introspect(&pool, &["e2e_enum".to_string()], false)
        .await
        .unwrap();

    let enum_info = schema
        .enums
        .iter()
        .find(|e| e.name == "mood")
        .expect("enum `mood` should be introspected");
    assert_eq!(enum_info.variants, vec!["sad", "happy"]);
}

// --- composite types ---

/// Regression test for issue #4.
///
/// `pg_attribute.attnum` is a `smallint`, but the composite-type query decoded
/// the field ordinal as `i32`. sqlx does no implicit widening, so introspection
/// died with `ColumnDecode { index: "5", .. INT4 is not compatible with INT2 }`
/// on any schema declaring a standalone composite type. Schemas without one
/// returned zero rows and never decoded anything, which is why this went
/// unnoticed for so long.
#[tokio::test]
#[ignore = "needs a live Postgres (PG_URL)"]
async fn test_composite_type_is_introspected() {
    let pool = setup_schema("e2e_composite").await;
    exec(
        &pool,
        "CREATE TYPE e2e_composite.address AS (street TEXT, city TEXT, zip TEXT)",
    )
    .await;

    let schema = introspect(&pool, &["e2e_composite".to_string()], false)
        .await
        .unwrap();

    let composite = schema
        .composite_types
        .iter()
        .find(|c| c.name == "address")
        .expect("composite type `address` should be introspected");
    let ordinals: Vec<i32> = composite
        .fields
        .iter()
        .map(|f| f.ordinal_position)
        .collect();
    assert_eq!(ordinals, vec![1, 2, 3]);
    let names: Vec<&str> = composite.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["street", "city", "zip"]);
}

/// A table's implicit row type is also a composite in `pg_type`; only
/// standalone `CREATE TYPE ... AS (..)` declarations should surface.
#[tokio::test]
#[ignore = "needs a live Postgres (PG_URL)"]
async fn test_table_row_type_is_not_a_composite() {
    let pool = setup_schema("e2e_rowtype").await;
    exec(
        &pool,
        "CREATE TABLE e2e_rowtype.orders (id INT PRIMARY KEY)",
    )
    .await;

    let schema = introspect(&pool, &["e2e_rowtype".to_string()], false)
        .await
        .unwrap();

    assert!(schema.composite_types.is_empty());
}
