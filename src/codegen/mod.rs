pub mod composite_gen;
pub mod domain_gen;
pub mod enum_gen;
pub mod struct_gen;

use std::collections::{BTreeSet, HashMap};

use proc_macro2::TokenStream;

use crate::cli::DatabaseKind;
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
) -> Vec<GeneratedFile> {
    let mut files = Vec::new();

    // Generate struct files for each table
    for table in &schema_info.tables {
        let (tokens, imports) =
            struct_gen::generate_struct(table, db_kind, schema_info, extra_derives, type_overrides);
        let imports = filter_imports(&imports, single_file);
        let code = format_tokens_with_imports(&tokens, &imports);
        let module_name = normalize_module_name(&table.name);
        let origin = format!("Table: {}.{}", table.schema_name, table.name);
        files.push(GeneratedFile {
            filename: format!("{}.rs", module_name),
            origin: Some(origin),
            code,
        });
    }

    // Generate types file (enums, composites, domains)
    // Each item is formatted individually so we can insert blank lines between them.
    let mut types_blocks: Vec<String> = Vec::new();
    let mut types_imports = BTreeSet::new();

    for enum_info in &schema_info.enums {
        let (tokens, imports) = enum_gen::generate_enum(enum_info, db_kind, extra_derives);
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
        );
        types_blocks.push(format_tokens(&tokens));
        types_imports.extend(imports);
    }

    for domain in &schema_info.domains {
        let (tokens, imports) =
            domain_gen::generate_domain(domain, db_kind, schema_info, type_overrides);
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
        eprintln!("ERROR: failed to parse generated code: {}", e);
        eprintln!("  This is a bug in sqlx-gen. Raw tokens:\n  {}", tokens);
        std::process::exit(1);
    });
    let raw = prettyplease::unparse(&file);
    add_blank_lines_between_variants(&raw)
}

/// Format a single TokenStream block (no imports).
pub(crate) fn format_tokens(tokens: &TokenStream) -> String {
    parse_and_format(tokens)
}

pub(crate) fn format_tokens_with_imports(tokens: &TokenStream, imports: &BTreeSet<String>) -> String {
    let import_lines: String = imports
        .iter()
        .map(|i| format!("{}\n", i))
        .collect();

    let formatted = parse_and_format(tokens);

    if import_lines.is_empty() {
        formatted
    } else {
        format!("{}\n\n{}", import_lines.trim_end(), formatted)
    }
}

/// Post-process formatted code to add blank lines between enum variants
/// and between struct fields. prettyplease doesn't insert them.
fn add_blank_lines_between_variants(code: &str) -> String {
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

    // ========== add_blank_lines_between_variants ==========

    #[test]
    fn test_blank_lines_between_renamed_variants() {
        let input = "pub enum Foo {\n    #[sqlx(rename = \"a\")]\n    A,\n    #[sqlx(rename = \"b\")]\n    B,\n}";
        let result = add_blank_lines_between_variants(input);
        assert!(result.contains("A,\n\n    #[sqlx(rename = \"b\")]"));
    }

    #[test]
    fn test_no_blank_line_for_first_variant() {
        let input = "pub enum Foo {\n    #[sqlx(rename = \"a\")]\n    A,\n}";
        let result = add_blank_lines_between_variants(input);
        // No blank line before first #[sqlx(rename because previous line is `{`
        assert!(!result.contains("{\n\n"));
    }

    #[test]
    fn test_no_change_without_rename() {
        let input = "pub enum Foo {\n    A,\n    B,\n}";
        let result = add_blank_lines_between_variants(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_no_change_for_struct() {
        let input = "pub struct Foo {\n    pub a: i32,\n    pub b: String,\n}";
        let result = add_blank_lines_between_variants(input);
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
            ordinal_position: 0,
            schema_name: "public".to_string(),
        }
    }

    #[test]
    fn test_generate_empty_schema() {
        let schema = SchemaInfo::default();
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
        assert!(files.is_empty());
    }

    #[test]
    fn test_generate_one_table() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
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
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_generate_enum_creates_types_file() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string(), "inactive".to_string()],
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
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
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
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
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
        assert_eq!(files.len(), 2); // users.rs + types.rs
    }

    #[test]
    fn test_generate_filename_normalized() {
        let schema = SchemaInfo {
            tables: vec![make_table("user__data", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
        assert_eq!(files[0].filename, "user_data.rs");
    }

    #[test]
    fn test_generate_origin_correct() {
        let schema = SchemaInfo {
            tables: vec![make_table("users", vec![make_col("id", "int4")])],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
        assert_eq!(files[0].origin, Some("Table: public.users".to_string()));
    }

    #[test]
    fn test_generate_types_no_origin() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
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
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), true);
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
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
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
        let files = generate(&schema, DatabaseKind::Postgres, &derives, &HashMap::new(), false);
        assert!(files[0].code.contains("Serialize"));
    }

    #[test]
    fn test_generate_extra_derives_in_enum() {
        let schema = SchemaInfo {
            enums: vec![EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["active".to_string()],
            }],
            ..Default::default()
        };
        let derives = vec!["Serialize".to_string()];
        let files = generate(&schema, DatabaseKind::Postgres, &derives, &HashMap::new(), false);
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
        let files = generate(&schema, DatabaseKind::Postgres, &[], &overrides, false);
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
            }],
            ..Default::default()
        };
        let files = generate(&schema, DatabaseKind::Postgres, &[], &HashMap::new(), false);
        for f in &files {
            // Should be parseable as valid Rust
            let parse_result = syn::parse_file(&f.code);
            assert!(parse_result.is_ok(), "Failed to parse {}: {:?}", f.filename, parse_result.err());
        }
    }
}
