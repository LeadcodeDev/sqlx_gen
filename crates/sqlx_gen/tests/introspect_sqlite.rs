use sqlx::SqlitePool;
use sqlx_gen::introspect::sqlite::introspect;

async fn setup_pool() -> SqlitePool {
    SqlitePool::connect("sqlite::memory:").await.unwrap()
}

async fn exec(pool: &SqlitePool, sql: &str) {
    sqlx::query(sql).execute(pool).await.unwrap();
}

// --- empty database ---

#[tokio::test]
async fn test_empty_db_no_tables() {
    let pool = setup_pool().await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(schema.tables.is_empty());
}

#[tokio::test]
async fn test_empty_db_no_enums() {
    let pool = setup_pool().await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(schema.enums.is_empty());
}

#[tokio::test]
async fn test_empty_db_no_composites() {
    let pool = setup_pool().await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(schema.composite_types.is_empty());
}

#[tokio::test]
async fn test_empty_db_no_domains() {
    let pool = setup_pool().await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(schema.domains.is_empty());
}

// --- simple tables ---

#[tokio::test]
async fn test_one_table_two_columns() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.tables[0].columns.len(), 2);
}

#[tokio::test]
async fn test_table_name_correct() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert_eq!(schema.tables[0].name, "users");
}

#[tokio::test]
async fn test_schema_name_main() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert_eq!(schema.tables[0].schema_name, "main");
}

#[tokio::test]
async fn test_column_names_and_order() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL, email TEXT)").await;
    let schema = introspect(&pool, false).await.unwrap();
    let cols: Vec<&str> = schema.tables[0].columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "name", "email"]);
}

// --- column types ---

#[tokio::test]
async fn test_integer_type() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE t (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert_eq!(schema.tables[0].columns[0].data_type, "INTEGER");
}

#[tokio::test]
async fn test_not_null_column() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE t (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(!schema.tables[0].columns[0].is_nullable);
}

#[tokio::test]
async fn test_nullable_column() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE t (name TEXT)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(schema.tables[0].columns[0].is_nullable);
}

// --- multiple tables ---

#[tokio::test]
async fn test_multiple_tables_sorted() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE zebra (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE alpha (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE TABLE mid (id INTEGER NOT NULL)").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert_eq!(schema.tables.len(), 3);
    let names: Vec<&str> = schema.tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "mid", "zebra"]);
}

// --- views ---

#[tokio::test]
async fn test_view_introspected_with_flag() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    exec(&pool, "CREATE VIEW active_users AS SELECT id, name FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    assert_eq!(schema.views.len(), 1);
    assert_eq!(schema.views[0].name, "active_users");
}

#[tokio::test]
async fn test_view_columns_correct() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL, name TEXT NOT NULL)").await;
    exec(&pool, "CREATE VIEW user_names AS SELECT id, name FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    let cols: Vec<&str> = schema.views[0].columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "name"]);
}

#[tokio::test]
async fn test_view_not_in_tables() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE VIEW v AS SELECT id FROM users").await;
    let schema = introspect(&pool, true).await.unwrap();
    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.views.len(), 1);
}

#[tokio::test]
async fn test_views_empty_when_flag_false() {
    let pool = setup_pool().await;
    exec(&pool, "CREATE TABLE users (id INTEGER NOT NULL)").await;
    exec(&pool, "CREATE VIEW v AS SELECT id FROM users").await;
    let schema = introspect(&pool, false).await.unwrap();
    assert!(schema.views.is_empty());
}
