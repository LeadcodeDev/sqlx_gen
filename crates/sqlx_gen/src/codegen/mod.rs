pub mod composite_gen;
pub mod crud_gen;
pub mod domain_gen;
pub mod entity_parser;
pub mod enum_gen;
pub mod struct_gen;

use std::collections::{BTreeSet, HashMap};

use proc_macro2::TokenStream;

use crate::cli::{DatabaseKind, TimeCrate};
use crate::introspect::SchemaInfo;

/// Rust reserved keywords that cannot be used as identifiers.
const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum",
    "extern", "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move",
    "mut", "pub", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true",
    "type", "unsafe", "use", "where", "while", "yield", "abstract", "become", "box", "do",
    "final", "macro", "override", "priv", "try", "typeof", "unsized", "virtual",
];

/// Returns true if the given name is a Rust reserved keyword.
pub fn is_rust_keyword(name: &str) -> bool {
    RUST_KEYWORDS.contains(&name)
}

/// Returns the imports needed for well-known extra derives.
pub fn imports_for_derives(extra_derives: &[String]) -> Vec<String> {
    let mut imports = Vec::new();
    let has = |name: &str| extra_derives.iter().any(|d| d == name);
    if has("Serialize") || has("Deserialize") {
        let mut parts = Vec::new();
        if has("Serialize") {
            parts.push("Serialize");
        }
        if has("Deserialize") {
            parts.push("Deserialize");
        }
        imports.push(format!("use serde::{{{}}};", parts.join(", ")));
    }
    imports
}

/// Normalize a table name for use as a Rust module/filename:
/// replace multiple consecutive underscores with a single one.
pub fn normalize_module_name(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut prev_underscore = false;
    for c in name.chars() {
        if c == '_' {
            if !prev_underscore {
                result.push(c);
            }
            prev_underscore = true;
        } else {
            prev_underscore = false;
            result.push(c);
        }
    }
    result
}

/// Well-known default schemas that don't need a prefix in filenames.
const DEFAULT_SCHEMAS: &[&str] = &["public", "main", "dbo"];

/// Returns true if the schema is a well-known default (public, main, dbo).
pub fn is_default_schema(schema: &str) -> bool {
    DEFAULT_SCHEMAS.contains(&schema)
}

/// Build a module name, prefixing with schema only when the name collides
/// (same table name exists in multiple schemas).
pub fn build_module_name(schema_name: &str, table_name: &str, name_collides: bool) -> String {
    if name_collides && !is_default_schema(schema_name) {
        normalize_module_name(&format!("{}_{}", schema_name, table_name))
    } else {
        normalize_module_name(table_name)
    }
}

/// Find table/view names that appear in more than one schema.
fn find_colliding_names(schema_info: &SchemaInfo) -> BTreeSet<&str> {
    let mut seen: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    for t in &schema_info.tables {
        seen.entry(t.name.as_str()).or_default().insert(t.schema_name.as_str());
    }
    for v in &schema_info.views {
        seen.entry(v.name.as_str()).or_default().insert(v.schema_name.as_str());
    }
    seen.into_iter()
        .filter(|(_, schemas)| schemas.len() > 1)
        .map(|(name, _)| name)
        .collect()
}

/// A generated code file with its content and required imports.
#[derive(Debug, Clone)]
pub struct GeneratedFile {
    pub filename: String,
    /// Optional origin comment (e.g. "Table: schema.name")
    pub origin: Option<String>,
    pub code: String,
}

/// Generate all code for a given schema.
pub fn generate(
    schema_info: &SchemaInfo,
    db_kind: DatabaseKind,
    extra_derives: &[String],
    type_overrides: &HashMap<String, String>,
    single_file: bool,
    time_crate: TimeCrate,
) -> Vec<GeneratedFile> {
    let mut files = Vec::new();

    // Detect table/view names that appear in multiple schemas (collisions)
    let colliding_names = find_colliding_names(schema_info);

    // Generate struct files for each table
    for table in &schema_info.tables {
        let (tokens, imports) =
            struct_gen::generate_struct(table, db_kind, schema_info, extra_derives, type_overrides, false, time_crate);
        let imports = filter_imports(&imports, single_file);
        let code = format_tokens_with_imports(&tokens, &imports);
        let module_name = build_module_name(&table.schema_name, &table.name, colliding_names.contains(table.name.as_str()));
        files.push(GeneratedFile {
            filename: format!("{}.rs", module_name),
            origin: None,
            code,
        });
    }

    // Generate struct files for each view
    for view in &schema_info.views {
        let (tokens, imports) =
            struct_gen::generate_struct(view, db_kind, schema_info, extra_derives, type_overrides, true, time_crate);
        let imports = filter_imports(&imports, single_file);
        let code = format_tokens_with_imports(&tokens, &imports);
        let module_name = build_module_name(&view.schema_name, &view.name, colliding_names.contains(view.name.as_str()));
        files.push(GeneratedFile {
            filename: format!("{}.rs", module_name),
            origin: None,
            code,
        });
    }

    // Generate types file (enums, composites, domains)
    // Each item is formatted individually so we can insert blank lines between them.
    let mut types_blocks: Vec<String> = Vec::new();
    let mut types_imports = BTreeSet::new();

    // Enrich enums with default variants extracted from column defaults
    let enum_defaults = extract_enum_defaults(schema_info);
    for enum_info in &schema_info.enums {
        let mut enriched = enum_info.clone();
        if enriched.default_variant.is_none() {
            if let Some(default) = enum_defaults.get(&enum_info.name) {
                enriched.default_variant = Some(default.clone());
            }
        }
        let (tokens, imports) = enum_gen::generate_enum(&enriched, db_kind, extra_derives);
        types_blocks.push(format_tokens(&tokens));
        types_imports.extend(imports);
    }

    for composite in &schema_info.composite_types {
        let (tokens, imports) = composite_gen::generate_composite(
            composite,
            db_kind,
            schema_info,
            extra_derives,
            type_overrides,
            time_crate,
        );
        types_blocks.push(format_tokens(&tokens));
        types_imports.extend(imports);
    }

    for domain in &schema_info.domains {
        let (tokens, imports) =
            domain_gen::generate_domain(domain, db_kind, schema_info, type_overrides, time_crate);
        types_blocks.push(format_tokens(&tokens));
        types_imports.extend(imports);
    }

    if !types_blocks.is_empty() {
        let import_lines: String = types_imports
            .iter()
            .map(|i| format!("{}\n", i))
            .collect();
        let body = types_blocks.join("\n");
        let code = if import_lines.is_empty() {
            body
        } else {
            format!("{}\n\n{}", import_lines.trim_end(), body)
        };
        files.push(GeneratedFile {
            filename: "types.rs".to_string(),
            origin: None,
            code,
        });
    }

    files
}

/// Extract default variant values for enums by scanning column defaults across all tables and views.
/// PostgreSQL column defaults look like `'idle'::task_status` or `'active'::public.task_status`.
fn extract_enum_defaults(schema_info: &SchemaInfo) -> HashMap<String, String> {
    let mut defaults: HashMap<String, String> = HashMap::new();

    let all_columns = schema_info
        .tables
        .iter()
        .chain(schema_info.views.iter())
        .flat_map(|t| t.columns.iter());

    for col in all_columns {
        let default_expr = match &col.column_default {
            Some(d) => d,
            None => continue,
        };

        // Strip leading underscore for array types to get the base enum name
        let base_udt = col.udt_name.strip_prefix('_').unwrap_or(&col.udt_name);

        // Check if this column references a known enum
        let enum_match = schema_info.enums.iter().find(|e| e.name == base_udt);
        if enum_match.is_none() {
            continue;
        }

        // Parse PG default: 'variant'::type_name
        if let Some(variant) = parse_pg_enum_default(default_expr) {
            defaults.entry(base_udt.to_string()).or_insert(variant);
        }
    }

    defaults
}

/// Parse a PostgreSQL column default expression to extract the enum variant.
/// Handles formats like `'idle'::task_status`, `'idle'::public.task_status`.
fn parse_pg_enum_default(default_expr: &str) -> Option<String> {
    // Pattern: 'value'::some_type
    let stripped = default_expr.trim();
    if stripped.starts_with('\'') {
        if let Some(end_quote) = stripped[1..].find('\'') {
            let value = &stripped[1..1 + end_quote];
            // Verify there's a :: cast after the closing quote
            let rest = &stripped[2 + end_quote..];
            if rest.starts_with("::") {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// In single-file mode, strip `use super::types::` imports since everything is in the same file.
fn filter_imports(imports: &BTreeSet<String>, single_file: bool) -> BTreeSet<String> {
    if single_file {
        imports
            .iter()
            .filter(|i| !i.contains("super::types::"))
            .cloned()
            .collect()
    } else {
        imports.clone()
    }
}

/// Parse and format a TokenStream via prettyplease, then post-process spacing.
pub(crate) fn parse_and_format(tokens: &TokenStream) -> String {
    let file = syn::parse2::<syn::File>(tokens.clone()).unwrap_or_else(|e| {
        log::error!("Failed to parse generated code: {}", e);
        log::error!("This is a bug in sqlx-gen. Raw tokens:\n  {}", tokens);
        std::process::exit(1);
    });
    let raw = prettyplease::unparse(&file);
    add_blank_lines_between_items(&raw)
}

/// Format a single TokenStream block (no imports).
pub(crate) fn format_tokens(tokens: &TokenStream) -> String {
    parse_and_format(tokens)
}

pub fn format_tokens_with_imports(tokens: &TokenStream, imports: &BTreeSet<String>) -> String {
    let formatted = parse_and_format(tokens);

    let used_imports: Vec<&String> = imports
        .iter()
        .filter(|imp| is_import_used(imp, &formatted))
        .collect();

    if used_imports.is_empty() {
        formatted
    } else {
        let import_lines: String = used_imports
            .iter()
            .map(|i| format!("{}\n", i))
            .collect();
        format!("{}\n\n{}", import_lines.trim_end(), formatted)
    }
}

/// Check if an import is actually used in the generated code.
/// Extracts the imported type names and checks if they appear in the code.
fn is_import_used(import: &str, code: &str) -> bool {
    // "use foo::bar::Baz;" → check for "Baz"
    // "use foo::{A, B};" → check for "A" or "B"
    // "use foo::bar::*;" → always keep
    let trimmed = import.trim().trim_end_matches(';');
    let path = trimmed.strip_prefix("use ").unwrap_or(trimmed);

    if path.ends_with("::*") {
        return true;
    }

    // Handle grouped imports: use foo::{A, B, C};
    if let Some(start) = path.find('{') {
        if let Some(end) = path.find('}') {
            let names = &path[start + 1..end];
            return names
                .split(',')
                .map(|n| n.trim())
                .filter(|n| !n.is_empty())
                .any(|name| code.contains(name));
        }
    }

    // Simple import: use foo::Bar;
    if let Some(name) = path.rsplit("::").next() {
        return code.contains(name);
    }

    true
}

/// Post-process formatted code to:
/// - Add blank lines between enum variants with `#[sqlx(rename`
/// - Add blank lines between top-level items (structs, impls)
/// - Add blank lines between logical blocks inside async methods
fn add_blank_lines_between_items(code: &str) -> String {
    let lines: Vec<&str> = code.lines().collect();
    let mut result = Vec::with_capacity(lines.len());

    for (i, line) in lines.iter().enumerate() {
        // Insert a blank line before `#[sqlx(rename` that follows a variant line (ending with `,`)
        // but not for the first variant in the enum.
        if i > 0 && line.trim().starts_with("#[sqlx(rename") {
            let prev = lines[i - 1].trim();
            if prev.ends_with(',') {
                result.push("");
            }
        }

        // Insert a blank line before top-level items (pub struct, impl, #[derive)
        // and before methods inside impl blocks, when preceded by a closing brace `}`
        if i > 0 {
            let trimmed = line.trim();
            let prev = lines[i - 1].trim();
            if prev == "}"
                && (trimmed.starts_with("pub struct")
                    || trimmed.starts_with("impl ")
                    || trimmed.starts_with("#[derive")
                    || trimmed.starts_with("pub async fn")
                    || trimmed.starts_with("pub fn"))
            {
                result.push("");
            }
        }

        // Insert a blank line before a new logical block inside methods:
        // - before `let` or `Ok(` when preceded by `.await?;` or `.unwrap_or(…);`
        // - before `let … = sqlx::` when preceded by a simple `let … = …;` (not sqlx)
        if i > 0 {
            let trimmed = line.trim();
            let prev = lines[i - 1].trim();
            let prev_is_await_end = prev.ends_with(".await?;")
                || prev.ends_with(".await?")
                || (prev.ends_with(';') && prev.contains(".unwrap_or("));
            if prev_is_await_end
                && (trimmed.starts_with("let ") || trimmed.starts_with("Ok("))
            {
                result.push("");
            }
            // Separate a sqlx query `let` from preceding simple `let` assignments
            if trimmed.starts_with("let ") && trimmed.contains("sqlx::")
                && prev.starts_with("let ") && !prev.contains("sqlx::")
            {
                result.push("");
            }
        }

        result.push(line);
    }

    result.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::introspect::{
        ColumnInfo, CompositeTypeInfo, DomainInfo, EnumInfo, SchemaInfo, TableInfo,
    };
    use std::collections::HashMap;

    // ========== is_rust_keyword ==========

    #[test]
    fn test_keyword_type() {
        assert!(is_rust_keyword("type"));
    }

    #[test]
    fn test_keyword_fn() {
        assert!(is_rust_keyword("fn"));
    }

    #[test]
    fn test_keyword_let() {
        assert!(is_rust_keyword("let"));
    }

    #[test]
    fn test_keyword_match() {
        assert!(is_rust_keyword("match"));
    }

    #[test]
    fn test_keyword_async() {
        assert!(is_rust_keyword("async"));
    }

    #[test]
    fn test_keyword_await() {
        assert!(is_rust_keyword("await"));
    }

    #[test]
    fn test_keyword_yield() {
        assert!(is_rust_keyword("yield"));
    }

    #[test]
    fn test_keyword_abstract() {
        assert!(is_rust_keyword("abstract"));
    }

    #[test]
    fn test_keyword_try() {
        assert!(is_rust_keyword("try"));
    }

    #[test]
    fn test_not_keyword_name() {
        assert!(!is_rust_keyword("name"));
    }

    #[test]
    fn test_not_keyword_id() {
        assert!(!is_rust_keyword("id"));
    }

    #[test]
    fn test_not_keyword_uppercase_type() {
        assert!(!is_rust_keyword("Type"));
    }

    // ========== normalize_module_name ==========

    #[test]
    fn test_normalize_no_underscores() {
        assert_eq!(normalize_module_name("users"), "users");
    }

    #[test]
    fn test_normalize_single_underscore() {
        assert_eq!(normalize_module_name("user_roles"), "user_roles");
    }

    #[test]
    fn test_normalize_double_underscore() {
        assert_eq!(normalize_module_name("user__roles"), "user_roles");
    }

    #[test]
    fn test_normalize_triple_underscore() {
        assert_eq!(normalize_module_name("a___b"), "a_b");
    }

    #[test]
    fn test_normalize_leading_underscore() {
        assert_eq!(normalize_module_name("_private"), "_private");
    }

    #[test]
    fn test_normalize_trailing_underscore() {
        assert_eq!(normalize_module_name("name_"), "name_");
    }

    #[test]
    fn test_normalize_double_leading() {
        assert_eq!(normalize_module_name("__double_leading"), "_double_leading");
    }

    #[test]
    fn test_normalize_multiple_groups() {
        assert_eq!(normalize_module_name("a__b__c"), "a_b_c");
    }

    // ========== build_module_name ==========

    #[test]
    fn test_build_no_collision_no_prefix() {
        assert_eq!(build_module_name("public", "users", false), "users");
    }

    #[test]
    fn test_build_no_collision_non_default_no_prefix() {
        assert_eq!(build_module_name("billing", "invoices", false), "invoices");
    }

    #[test]
    fn test_build_collision_prefixed() {
        assert_eq!(build_module_name("billing", "users", true), "billing_users");
    }

    #[test]
    fn test_build_collision_default_schema_no_prefix() {
        assert_eq!(build_module_name("public", "users", true), "users");
    }

    #[test]
    fn test_build_collision_normalizes_double_underscore() {
        assert_eq!(build_module_name("billing", "agent__connector", true), "billing_agent_connector");
    }

    // ========== is_default_schema ==========

    #[test]
    fn test_default_schema_public() {
        assert!(is_default_schema("public"));
    }

    #[test]
    fn test_default_schema_main() {
        assert!(is_default_schema("main"));
    }

    #[test]
    fn test_non_default_schema() {
        assert!(!is_default_schema("billing"));
    }

    // ========== imports_for_derives ==========

    #[test]
    fn test_imports_empty() {
        let result = imports_for_derives(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_imports_serialize_only() {
        let derives = vec!["Serialize".to_string()];
        let result = imports_for_derives(&derives);
        assert_eq!(result, vec!["use serde::{Serialize};"]);
    }

    #[test]
    fn test_imports_deserialize_only() {
        let derives = vec!["Deserialize".to_string()];
        let result = imports_for_derives(&derives);
        assert_eq!(result, vec!["use serde::{Deserialize};"]);
    }

    #[test]
    fn test_imports_both_serde() {
        let derives = vec!["Serialize".to_string(), "Deserialize".to_string()];
        let result = imports_for_derives(&derives);
        assert_eq!(result, vec!["use serde::{Serialize, Deserialize};"]);
    }

    #[test]
    fn test_imports_non_serde() {
        let derives = vec!["Hash".to_string()];
        let result = imports_for_derives(&derives);
        assert!(result.is_empty());
    }

    #[test]
    fn test_imports_non_serde_multiple() {
        let derives = vec!["PartialEq".to_string(), "Eq".to_string()];
        let result = imports_for_derives(&derives);
        assert!(result.is_empty());
    }

    #[test]
    fn test_imports_mixed_serde_and_others() {
        let derives = vec![
            "Serialize".to_string(),
            "Hash".to_string(),
            "Deserialize".to_string(),
        ];
        let result = imports_for_derives(&derives);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("Serialize"));
        assert!(result[0].contains("Deserialize"));
    }

    // ========== add_blank_lines_between_items ==========

    #[test]
    fn test_blank_lines_between_renamed_variants() {
        let input = "pub enum Foo {\n    #[sqlx(rename = \"a\")]\n    A,\n    #[sqlx(rename = \"b\")]\n    B,\n}";
        let result = add_blank_lines_between_items(input);
        assert!(result.contains("A,\n\n    #[sqlx(rename = \"b\")]"));
    }

    #[test]
    fn test_no_blank_line_for_first_variant() {
        let input = "pub enum Foo {\n    #[sqlx(rename = \"a\")]\n    A,\n}";
        let result = add_blank_lines_between_items(input);
        // No blank line before first #[sqlx(rename because previous line is `{`
        assert!(!result.contains("{\n\n"));
    }

    #[test]
    fn test_no_change_without_rename() {
        let input = "pub enum Foo {\n    A,\n    B,\n}";
        let result = add_blank_lines_between_items(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_no_change_for_struct() {
        let input = "pub struct Foo {\n    pub a: i32,\n    pub b: String,\n}";
        let result = add_blank_lines_between_items(input);
        assert_eq!(result, input);
    }

    // ========== filter_imports ==========

    #[test]
    fn test_filter_single_file_strips_super_types() {
        let mut imports = BTreeSet::new();
        imports.insert("use super::types::Foo;".to_string());
        imports.insert("use chrono::NaiveDateTime;".to_string());
        let result = filter_imports(&imports, true);
        assert!(!result.contains("use super::types::Foo;"));
        assert!(result.contains("use chrono::NaiveDateTime;"));
    }

    #[test]
    fn test_filter_single_file_keeps_other_imports() {
        let mut imports = BTreeSet::new();
        imports.insert("use chrono::NaiveDateTime;".to_string());
        let result = filter_imports(&imports, true);
        assert!(result.contains("use chrono::NaiveDateTime;"));
    }

    #[test]
    fn test_filter_multi_file_keeps_all() {
        let mut imports = BTreeSet::new();
        imports.insert("use super::types::Foo;".to_string());
        imports.insert("use chrono::NaiveDateTime;".to_string());
        let result = filter_imports(&imports, false);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_filter_empty_set() {
        let imports = BTreeSet::new();
        let result = filter_imports(&imports, true);
        assert!(result.is_empty());
    }

    // ========== generate() orchestrator ==========

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema_name: "public".to_string(),
            name: name.to_string(),
            columns,
        }
    }

    fn make_col(name: &str, udt_name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: udt_name.to_string(),
            udt_name: udt_name.to_string(),
            is_nullable: false,
            is_primary_key: false,
            ordinal_position: 0,
            schema_name: "public".to_string(),
            column_default: None,
        }
    }

    #[test]
    fn test_generate_empty_schema() {
        let schema = SchemaInfo::default();
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert!(files.is_empty());
    }

    #[test]
    fn test_generate_one_table() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "users.rs");
    }

    #[test]
    fn test_generate_two_tables() {
        let schema = SchemaInfo {
            tables: vec![
                make_table("users", vec![make_col("id", "int4")]),
                make_table("posts", vec![make_col("id", "int4")]),
            ],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_generate_enum_creates_types_file() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string(), "inactive".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "types.rs");
    }

    #[test]
    fn test_generate_enums_composites_domains_single_types_file() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
                default_variant: None,
            }],
            composite_types: vec![CompositeTypeInfo {
                schema_name: "public".to_string(),
                name: "address".to_string(),
                fields: vec![make_col("street", "text")],
            }],
            domains: vec![DomainInfo {
                schema_name: "public".to_string(),
                name: "email".to_string(),
                base_type: "text".to_string(),
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        // Should produce exactly 1 types.rs
        let types_files: Vec<_> = files.iter().filter(|f| f.filename == "types.rs").collect();
        assert_eq!(types_files.len(), 1);
    }

    #[test]
    fn test_generate_tables_and_enums() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files.len(), 2); // users.rs + types.rs
    }

    #[test]
    fn test_generate_filename_normalized() {
        let schema = SchemaInfo {
            tables: vec![make_table("user__data", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files[0].filename, "user_data.rs");
    }

    #[test]
    fn test_generate_no_origin_for_tables() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files[0].origin, None);
    }

    #[test]
    fn test_generate_types_no_origin() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files[0].origin, None);
    }

    #[test]
    fn test_generate_single_file_filters_super_types_imports() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), true, TimeCrate::Chrono);
        // struct file should not have super::types:: imports
        let struct_file = files.iter().find(|f| f.filename == "users.rs").unwrap();
        assert!(!struct_file.code.contains("super::types::"));
    }

    #[test]
    fn test_generate_multi_file_keeps_super_types_imports() {
        // Table with a column referencing an enum
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("status", "status")])],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        let struct_file = files.iter().find(|f| f.filename == "users.rs").unwrap();
        assert!(struct_file.code.contains("super::types::"));
    }

    #[test]
    fn test_generate_extra_derives_in_struct() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let derives = vec!["Serialize".to_string()];
        let files = generate(&schema, DatabaseKind::Postgres, &derives, &HashMap::new(), false, TimeCrate::Chrono);
        assert!(files[0].code.contains("Serialize"));
    }

    #[test]
    fn test_generate_extra_derives_in_enum() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let derives = vec!["Serialize".to_string()];
        let files = generate(&schema, DatabaseKind::Postgres, &derives, &HashMap::new(), false, TimeCrate::Chrono);
        assert!(files[0].code.contains("Serialize"));
    }

    #[test]
    fn test_generate_type_overrides_in_struct() {
        let mut overrides = HashMap::new();
        overrides.insert("jsonb".to_string(), "MyJson".to_string());
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("data", "jsonb")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &overrides, false, TimeCrate::Chrono);
        assert!(files[0].code.contains("MyJson"));
    }

    #[test]
    fn test_generate_valid_rust_syntax() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![
                make_col("id", "int4"),
                make_col("name", "text"),
            ])],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string(), "inactive".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        for f in &files {
            // Should be parseable as valid Rust
            let parse_result = syn::parse_file(&f.code);
            assert!(parse_result.is_ok(), "Failed to parse {}: {:?}", f.filename, parse_result.err());
        }
    }

    // ========== generate() — views ==========

    fn make_view(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema_name: "public".to_string(),
            name: name.to_string(),
            columns,
        }
    }

    #[test]
    fn test_generate_one_view() {
        let schema = SchemaInfo {
            views: vec![make_view("active_users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "active_users.rs");
    }

    #[test]
    fn test_generate_no_origin_for_views() {
        let schema = SchemaInfo {
            views: vec![make_view("active_users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files[0].origin, None);
    }

    #[test]
    fn test_generate_tables_and_views() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            views: vec![make_view("active_users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_generate_view_valid_rust() {
        let schema = SchemaInfo {
            views: vec![make_view("active_users", vec![
                make_col("id", "int4"),
                make_col("name", "text"),
            ])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        let parse_result = syn::parse_file(&files[0].code);
        assert!(parse_result.is_ok(), "Failed to parse: {:?}", parse_result.err());
    }

    #[test]
    fn test_generate_view_nullable_column() {
        let schema = SchemaInfo {
            views: vec![make_view("v", vec![ColumnInfo {
                name: "email".to_string(),
                data_type: "text".to_string(),
                udt_name: "text".to_string(),
                is_nullable: true,
                is_primary_key: false,
                ordinal_position: 0,
                schema_name: "public".to_string(),
                column_default: None,
            }])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert!(files[0].code.contains("Option<String>"));
    }

    #[test]
    fn test_generate_collision_both_prefixed() {
        let schema = SchemaInfo {
            tables: vec![
                make_table("users", vec![make_col("id", "int4")]),
                TableInfo {
                    schema_name: "billing".to_string(),
                    name: "users".to_string(),
                    columns: vec![make_col("id", "int4")],
                },
            ],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        let filenames: Vec<_> = files.iter().map(|f| f.filename.as_str()).collect();
        assert!(filenames.contains(&"users.rs"));
        assert!(filenames.contains(&"billing_users.rs"));
    }

    #[test]
    fn test_generate_no_collision_no_prefix() {
        let schema = SchemaInfo {
            tables: vec![
                make_table("users", vec![make_col("id", "int4")]),
                TableInfo {
                    schema_name: "billing".to_string(),
                    name: "invoices".to_string(),
                    columns: vec![make_col("id", "int4")],
                },
            ],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        let filenames: Vec<_> = files.iter().map(|f| f.filename.as_str()).collect();
        assert!(filenames.contains(&"users.rs"));
        assert!(filenames.contains(&"invoices.rs"));
    }

    #[test]
    fn test_generate_single_schema_no_prefix() {
        let schema = SchemaInfo {
            tables: vec![
                make_table("users", vec![make_col("id", "int4")]),
                make_table("posts", vec![make_col("id", "int4")]),
            ],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        assert_eq!(files[0].filename, "users.rs");
        assert_eq!(files[1].filename, "posts.rs");
    }

    #[test]
    fn test_generate_view_single_file_mode() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            views: vec![make_view("active_users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), true, TimeCrate::Chrono);
        assert_eq!(files.len(), 2);
    }

    // ========== parse_pg_enum_default ==========

    #[test]
    fn test_parse_pg_enum_default_simple() {
        assert_eq!(
            parse_pg_enum_default("'idle'::task_status"),
            Some("idle".to_string())
        );
    }

    #[test]
    fn test_parse_pg_enum_default_schema_qualified() {
        assert_eq!(
            parse_pg_enum_default("'active'::public.task_status"),
            Some("active".to_string())
        );
    }

    #[test]
    fn test_parse_pg_enum_default_not_enum() {
        // No single-quote pattern
        assert_eq!(parse_pg_enum_default("nextval('users_id_seq')"), None);
    }

    #[test]
    fn test_parse_pg_enum_default_no_cast() {
        assert_eq!(parse_pg_enum_default("'hello'"), None);
    }

    #[test]
    fn test_parse_pg_enum_default_empty() {
        assert_eq!(parse_pg_enum_default(""), None);
    }

    // ========== extract_enum_defaults ==========

    #[test]
    fn test_extract_enum_defaults_from_column() {
        let schema = SchemaInfo {
            tables: vec![TableInfo {
                schema_name: "public".to_string(),
                name: "tasks".to_string(),
                columns: vec![ColumnInfo {
                    name: "status".to_string(),
                    data_type: "USER-DEFINED".to_string(),
                    udt_name: "task_status".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    ordinal_position: 0,
                    schema_name: "public".to_string(),
                    column_default: Some("'idle'::task_status".to_string()),
                }],
            }],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "task_status".to_string(),
                variants: vec!["idle".to_string(), "running".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let defaults = extract_enum_defaults(&schema);
        assert_eq!(defaults.get("task_status"), Some(&"idle".to_string()));
    }

    #[test]
    fn test_extract_enum_defaults_no_default() {
        let schema = SchemaInfo {
            tables: vec![TableInfo {
                schema_name: "public".to_string(),
                name: "tasks".to_string(),
                columns: vec![ColumnInfo {
                    name: "status".to_string(),
                    data_type: "USER-DEFINED".to_string(),
                    udt_name: "task_status".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    ordinal_position: 0,
                    schema_name: "public".to_string(),
                    column_default: None,
                }],
            }],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "task_status".to_string(),
                variants: vec!["idle".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let defaults = extract_enum_defaults(&schema);
        assert!(defaults.is_empty());
    }

    #[test]
    fn test_extract_enum_defaults_non_enum_column_ignored() {
        let schema = SchemaInfo {
            tables: vec![TableInfo {
                schema_name: "public".to_string(),
                name: "users".to_string(),
                columns: vec![ColumnInfo {
                    name: "name".to_string(),
                    data_type: "character varying".to_string(),
                    udt_name: "varchar".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    ordinal_position: 0,
                    schema_name: "public".to_string(),
                    column_default: Some("'hello'::character varying".to_string()),
                }],
            }],
            enums: vec![],
            ..Default::default()
        };
        let defaults = extract_enum_defaults(&schema);
        assert!(defaults.is_empty());
    }

    #[test]
    fn test_generate_enum_with_default() {
        let schema = SchemaInfo {
            tables: vec![TableInfo {
                schema_name: "public".to_string(),
                name: "tasks".to_string(),
                columns: vec![ColumnInfo {
                    name: "status".to_string(),
                    data_type: "USER-DEFINED".to_string(),
                    udt_name: "task_status".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    ordinal_position: 0,
                    schema_name: "public".to_string(),
                    column_default: Some("'idle'::task_status".to_string()),
                }],
            }],
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "task_status".to_string(),
                variants: vec!["idle".to_string(), "running".to_string()],
                default_variant: None,
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false, TimeCrate::Chrono);
        let types_file = files.iter().find(|f| f.filename == "types.rs").unwrap();
        assert!(types_file.code.contains("impl Default for TaskStatus"));
        assert!(types_file.code.contains("Self::Idle"));
    }
}
