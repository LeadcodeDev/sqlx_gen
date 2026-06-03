//! Validate that codegen output is syntactically and semantically loadable.
//!
//! The fast path runs on every CI build: each generated file is parsed with
//! `syn::parse_file` (proves the output is valid Rust at the AST level).
//!
//! The deep path runs only when `SQLX_GEN_COMPILE_CHECK=1` is set in the env:
//! it scaffolds a temporary downstream crate, writes the generated files into
//! `src/lib.rs`, and runs `cargo check` against the upstream `sqlx-gen` crate
//! plus `sqlx`. That confirms the emitted attributes, derives, and imports are
//! genuinely accepted by `sqlx::FromRow`, `sqlx::Type`, and friends — not just
//! shaped like valid Rust.

use std::collections::HashMap;
use std::path::Path;

use sqlx_gen::cli::{DatabaseKind, DomainStyle, TimeCrate};
use sqlx_gen::codegen::{generate_with_domain_style, GeneratedFile};
use sqlx_gen::introspect::{
    ColumnInfo, CompositeTypeInfo, DomainInfo, EnumInfo, SchemaInfo, TableInfo,
};

fn rich_schema() -> SchemaInfo {
    SchemaInfo {
        tables: vec![TableInfo {
            schema_name: "public".to_string(),
            name: "users".to_string(),
            columns: vec![
                column("id", "int4", false, true, None),
                column("email", "text", false, false, None),
                column("name", "text", true, false, None),
                column("status", "status", false, false, None),
                column("metadata", "jsonb", true, false, None),
            ],
        }],
        views: vec![TableInfo {
            schema_name: "public".to_string(),
            name: "active_users".to_string(),
            columns: vec![
                column("id", "int4", false, false, None),
                column("email", "text", false, false, None),
            ],
        }],
        enums: vec![EnumInfo {
            schema_name: "public".to_string(),
            name: "status".to_string(),
            variants: vec!["active".to_string(), "inactive".to_string()],
            default_variant: Some("active".to_string()),
        }],
        composite_types: vec![CompositeTypeInfo {
            schema_name: "public".to_string(),
            name: "address".to_string(),
            fields: vec![
                column("street", "text", false, false, None),
                column("city", "text", false, false, None),
            ],
        }],
        domains: vec![DomainInfo {
            schema_name: "public".to_string(),
            name: "email".to_string(),
            base_type: "text".to_string(),
        }],
    }
}

fn column(name: &str, udt: &str, nullable: bool, pk: bool, default: Option<&str>) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type: udt.to_string(),
        udt_name: udt.to_string(),
        udt_schema: None,
        is_nullable: nullable,
        is_primary_key: pk,
        ordinal_position: 0,
        schema_name: "public".to_string(),
        column_default: default.map(|s| s.to_string()),
    }
}

fn parse_each_file(files: &[GeneratedFile]) {
    for f in files {
        syn::parse_file(&f.code).unwrap_or_else(|e| {
            panic!(
                "generated file '{}' is not syntactically valid Rust: {}\n--- BEGIN ---\n{}\n--- END ---",
                f.filename, e, f.code
            )
        });
    }
}

#[test]
fn generated_postgres_files_parse() {
    let files = generate_with_domain_style(
        &rich_schema(),
        DatabaseKind::Postgres,
        &[],
        &HashMap::new(),
        false,
        TimeCrate::Chrono,
        DomainStyle::Alias,
    )
    .expect("codegen");
    parse_each_file(&files);
}

#[test]
fn generated_mysql_files_parse() {
    let schema = SchemaInfo {
        // MySQL has no enums/composites/domains in our model, just tables.
        tables: rich_schema()
            .tables
            .into_iter()
            .map(|mut t| {
                t.columns
                    .retain(|c| c.udt_name != "status" && c.udt_name != "jsonb");
                t
            })
            .collect(),
        views: vec![],
        enums: vec![],
        composite_types: vec![],
        domains: vec![],
    };
    let files = generate_with_domain_style(
        &schema,
        DatabaseKind::Mysql,
        &[],
        &HashMap::new(),
        false,
        TimeCrate::Chrono,
        DomainStyle::Alias,
    )
    .expect("codegen");
    parse_each_file(&files);
}

#[test]
fn generated_sqlite_files_parse() {
    let schema = SchemaInfo {
        tables: vec![TableInfo {
            schema_name: "main".to_string(),
            name: "users".to_string(),
            columns: vec![
                column("id", "INTEGER", false, true, None),
                column("name", "TEXT", false, false, None),
            ],
        }],
        views: vec![],
        enums: vec![],
        composite_types: vec![],
        domains: vec![],
    };
    let files = generate_with_domain_style(
        &schema,
        DatabaseKind::Sqlite,
        &[],
        &HashMap::new(),
        false,
        TimeCrate::Chrono,
        DomainStyle::Alias,
    )
    .expect("codegen");
    parse_each_file(&files);
}

#[test]
fn generated_postgres_files_parse_with_newtype_domain() {
    let files = generate_with_domain_style(
        &rich_schema(),
        DatabaseKind::Postgres,
        &[],
        &HashMap::new(),
        false,
        TimeCrate::Chrono,
        DomainStyle::Newtype,
    )
    .expect("codegen");
    parse_each_file(&files);
    // Ensure the newtype actually emitted a tuple struct (not a type alias).
    let types_rs = files
        .iter()
        .find(|f| f.filename == "types.rs")
        .expect("types.rs file should be emitted");
    assert!(
        types_rs.code.contains("pub struct Email(pub String)"),
        "newtype domain not found in:\n{}",
        types_rs.code
    );
}

/// Deep check: actually run `cargo check` against a real downstream crate.
/// Skipped unless `SQLX_GEN_COMPILE_CHECK=1` is set, because it pulls in
/// the full sqlx dependency tree (~1 min on a cold cargo cache).
#[test]
fn generated_files_pass_cargo_check_in_downstream_crate() {
    if std::env::var("SQLX_GEN_COMPILE_CHECK").as_deref() != Ok("1") {
        eprintln!("skipped (set SQLX_GEN_COMPILE_CHECK=1 to enable)");
        return;
    }

    let files = generate_with_domain_style(
        &rich_schema(),
        DatabaseKind::Postgres,
        &[],
        &HashMap::new(),
        true, // single_file = true so we emit one models.rs
        TimeCrate::Chrono,
        DomainStyle::Alias,
    )
    .expect("codegen");

    let dir = tempfile::tempdir().expect("temp dir");
    let project_root = workspace_root();
    let sqlx_gen_path = project_root.join("crates/sqlx_gen");

    std::fs::write(
        dir.path().join("Cargo.toml"),
        format!(
            r#"
[package]
name = "sqlx_gen_compile_check"
version = "0.0.0"
edition = "2021"

[lib]
path = "src/lib.rs"

[dependencies]
sqlx = {{ version = "0.8", default-features = false, features = [
    "runtime-tokio", "tls-rustls-ring", "postgres", "chrono", "uuid", "json",
] }}
sqlx_gen = {{ path = "{}", default-features = false }}
serde = {{ version = "1", features = ["derive"] }}
chrono = "0.4"
uuid = "1"
serde_json = "1"
rust_decimal = "1"
ipnetwork = "0.20"
"#,
            sqlx_gen_path.display(),
        ),
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    // Concatenate all generated files into a single lib.rs.
    let mut lib = String::new();
    lib.push_str("#![allow(unused_imports, dead_code, unused_attributes)]\n\n");
    for f in &files {
        lib.push_str(&f.code);
        lib.push_str("\n\n");
    }
    std::fs::write(dir.path().join("src/lib.rs"), &lib).unwrap();

    let status = std::process::Command::new("cargo")
        .arg("check")
        .arg("--offline")
        .current_dir(dir.path())
        .status()
        .or_else(|_| {
            std::process::Command::new("cargo")
                .arg("check")
                .current_dir(dir.path())
                .status()
        })
        .expect("invoke cargo check");

    assert!(
        status.success(),
        "generated code did not pass `cargo check` in a downstream crate"
    );
}

fn workspace_root() -> std::path::PathBuf {
    let mut p = Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    // CARGO_MANIFEST_DIR is the sqlx_gen crate; walk up until we hit the workspace Cargo.toml.
    while !p.join("Cargo.toml").exists()
        || !std::fs::read_to_string(p.join("Cargo.toml"))
            .map(|c| c.contains("[workspace]"))
            .unwrap_or(false)
    {
        if !p.pop() {
            panic!(
                "could not locate workspace root from {}",
                env!("CARGO_MANIFEST_DIR")
            );
        }
    }
    p
}
