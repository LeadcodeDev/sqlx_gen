use std::collections::HashMap;

use sqlx::SqlitePool;
use sqlx_gen::cli::DatabaseKind;
use sqlx_gen::codegen;
use sqlx_gen::introspect::sqlite::introspect;

async fn setup_pool() -> SqlitePool {
    SqlitePool::connect("sqlite::memory:").await.unwrap()
}

async fn exec(pool: &SqlitePool, sql: &str) {
    sqlx::query(sql).execute(pool).await.unwrap();
}

#[tokio::test]
async fn test_simple_table_generates_struct() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert!(files[0].code.contains("pub struct"));
}

#[tokio::test]
async fn test_struct_name_pascal_case() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE user_profiles (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert!(files[0].code.contains("pub struct UserProfiles"));
}

#[tokio::test]
async fn test_integer_mapped_to_i64() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE t (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert!(files[0].code.contains("i64"));
}

#[tokio::test]
async fn test_nullable_column_option() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE t (name TEXT)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert!(files[0].code.contains("Option<"));
}

#[tokio::test]
async fn test_multiple_tables_multiple_files() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE posts (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert_eq!(files.len(), 2);
}

#[tokio::test]
async fn test_filenames_correct() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert_eq!(files[0].filename, "users.rs");
}

#[tokio::test]
async fn test_generated_code_parseable() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    for f in &files {
        assert!(syn::parse_file(&f.code).is_ok(), "Failed to parse {}", f.filename);
    }
}

#[tokio::test]
async fn test_extra_derives_propagated() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let derives = vec!["Serialize".to_string()];
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &derives, &HashMap::new(), false);
    assert!(files[0].code.contains("Serialize"));
}

// --- views ---

#[tokio::test]
async fn test_view_generates_struct() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    exec(&pool, "CREATE VIEW active_users AS SELECT id, name FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    let view_file = files.iter().find(|f| f.filename == "active_users.rs").unwrap();
    assert!(view_file.code.contains("pub struct ActiveUsers"));
}

#[tokio::test]
async fn test_view_origin_contains_view() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE VIEW v AS SELECT id FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    let view_file = files.iter().find(|f| f.filename == "v.rs").unwrap();
    assert_eq!(view_file.origin, None);
}

#[tokio::test]
async fn test_view_code_parseable() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    exec(&pool, "CREATE VIEW user_view AS SELECT id, name FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    for f in &files {
        assert!(syn::parse_file(&f.code).is_ok(), "Failed to parse {}", f.filename);
    }
}

#[tokio::test]
async fn test_view_pascal_case_name() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE VIEW all_active_users AS SELECT id FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    let view_file = files.iter().find(|f| f.filename == "all_active_users.rs").unwrap();
    assert!(view_file.code.contains("pub struct AllActiveUsers"));
}

// --- exclude tables ---

#[tokio::test]
async fn test_exclude_table() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE _migrations (id INTEGER NOT NULL)").await;
    let mut schema = introspect(&pool, false).await.unwrap();
    let exclude = ["_migrations".to_string()];
    schema.tables.retain(|t| !exclude.contains(&t.name));
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].filename, "users.rs");
}

#[tokio::test]
async fn test_exclude_nonexistent_table() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE posts (id INTEGER NOT NULL)").await;
    let mut schema = introspect(&pool, false).await.unwrap();
    let exclude = ["nonexistent".to_string()];
    schema.tables.retain(|t| !exclude.contains(&t.name));
    assert_eq!(schema.tables.len(), 2);
}

#[tokio::test]
async fn test_tables_include_then_exclude() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE posts (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE comments (id INTEGER NOT NULL)").await;
    let mut schema = introspect(&pool, false).await.unwrap();
    // Simulate --tables users,posts
    let include = ["users".to_string(), "posts".to_string()];
    schema.tables.retain(|t| include.contains(&t.name));
    // Simulate --exclude-tables posts
    let exclude = ["posts".to_string()];
    schema.tables.retain(|t| !exclude.contains(&t.name));
    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.tables[0].name, "users");
}

#[tokio::test]
async fn test_exclude_view() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE VIEW v1 AS SELECT id FROM users").await;
    exec(&pool, "CREATE VIEW v2 AS SELECT id FROM users").await;
    let mut schema = introspect(&pool, true).await.unwrap();
    let exclude = ["v1".to_string()];
    schema.views.retain(|v| !exclude.contains(&v.name));
    let files = codegen::generate(&schema, DatabaseKind::Sqlite, &[], &HashMap::new(), false);
    let view_files: Vec<_> = files.iter().filter(|f| f.code.contains("kind = \"view\"")).collect();
    assert_eq!(view_files.len(), 1);
    assert_eq!(view_files[0].filename, "v2.rs");
}
