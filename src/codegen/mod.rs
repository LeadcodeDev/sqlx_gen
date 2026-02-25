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
fn parse_and_format(tokens: &TokenStream) -> String {
    let file = syn::parse2::<syn::File>(tokens.clone()).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to parse generated code: {}", e);
        eprintln!("  This is a bug in sqlx-gen. Raw tokens:\n  {}", tokens);
        std::process::exit(1);
    });
    let raw = prettyplease::unparse(&file);
    add_blank_lines_between_variants(&raw)
}

/// Format a single TokenStream block (no imports).
fn format_tokens(tokens: &TokenStream) -> String {
    parse_and_format(tokens)
}

fn format_tokens_with_imports(tokens: &TokenStream, imports: &BTreeSet<String>) -> String {
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
