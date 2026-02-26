use std::path::Path;

use quote::ToTokens;

/// Represents a field parsed from a generated entity struct.
#[derive(Debug, Clone)]
pub struct ParsedField {
    /// Rust field name (e.g. "connector_type")
    pub rust_name: String,
    /// Original DB column name. From `#[sqlx(rename = "...")]` if present, otherwise same as rust_name.
    pub column_name: String,
    /// Full Rust type as a string (e.g. "Option<String>", "i32", "uuid::Uuid")
    pub rust_type: String,
    /// Whether the type is wrapped in Option<T>
    pub is_nullable: bool,
    /// The inner type if nullable, or the full type if not
    pub inner_type: String,
    /// Whether this field is a primary key (`#[sqlx_gen(primary_key)]`)
    pub is_primary_key: bool,
}

/// Represents an entity parsed from a generated Rust file.
#[derive(Debug, Clone)]
pub struct ParsedEntity {
    /// Struct name in PascalCase (e.g. "Users", "UserRoles")
    pub struct_name: String,
    /// Original table/view name from `#[sqlx_gen(table = "...")]`
    pub table_name: String,
    /// Whether this entity represents a view (`#[sqlx_gen(kind = "view")]`)
    pub is_view: bool,
    /// Parsed fields
    pub fields: Vec<ParsedField>,
    /// `use` imports from the entity source file (e.g. "use chrono::{DateTime, Utc};")
    pub imports: Vec<String>,
}

/// Parse an entity struct from a `.rs` file on disk.
pub fn parse_entity_file(path: &Path) -> crate::error::Result<ParsedEntity> {
    let source = std::fs::read_to_string(path).map_err(crate::error::Error::Io)?;
    parse_entity_source(&source).map_err(|e| {
        crate::error::Error::Config(format!("{}: {}", path.display(), e))
    })
}

/// Parse an entity struct from a Rust source string.
pub fn parse_entity_source(source: &str) -> Result<ParsedEntity, String> {
    let syntax = syn::parse_file(source).map_err(|e| format!("Failed to parse: {}", e))?;

    // Collect use imports (excluding serde and sqlx derives)
    let imports = extract_use_imports(&syntax);

    for item in &syntax.items {
        if let syn::Item::Struct(item_struct) = item {
            if has_from_row_derive(item_struct) {
                let mut entity = extract_entity(item_struct)?;
                entity.imports = imports;
                return Ok(entity);
            }
        }
    }

    Err("No struct with sqlx::FromRow derive found".to_string())
}

/// Check if a struct has `sqlx::FromRow` in its derive attributes.
fn has_from_row_derive(item: &syn::ItemStruct) -> bool {
    for attr in &item.attrs {
        if attr.path().is_ident("derive") {
            let tokens = attr.meta.to_token_stream().to_string();
            if tokens.contains("FromRow") {
                return true;
            }
        }
    }
    false
}

/// Extract `use` imports from the source file, excluding serde/sqlx imports
/// (those are already handled by the CRUD generator).
fn extract_use_imports(file: &syn::File) -> Vec<String> {
    file.items
        .iter()
        .filter_map(|item| {
            if let syn::Item::Use(use_item) = item {
                let text = use_item.to_token_stream().to_string();
                // Skip serde and sqlx imports — the CRUD generator adds those itself
                if text.contains("serde") || text.contains("sqlx") {
                    return None;
                }
                // Normalize spacing: "use chrono :: { DateTime , Utc } ;" → cleaned up
                let normalized = normalize_use_statement(&text);
                Some(normalized)
            } else {
                None
            }
        })
        .collect()
}

/// Normalize a tokenized `use` statement by removing extra spaces around `::`, `{`, `}`, and `,`.
fn normalize_use_statement(s: &str) -> String {
    s.replace(" :: ", "::")
        .replace(":: ", "::")
        .replace(" ::", "::")
        .replace("{ ", "{")
        .replace(" }", "}")
        .replace(" ,", ",")
        .replace(" ;", ";")
}

/// Extract a ParsedEntity from a struct item.
fn extract_entity(item: &syn::ItemStruct) -> Result<ParsedEntity, String> {
    let struct_name = item.ident.to_string();

    let (kind, table_name) = parse_sqlx_gen_struct_attrs(&item.attrs);
    let is_view = kind.as_deref() == Some("view");

    // Fall back to struct name if no table annotation
    let table_name = table_name.unwrap_or_else(|| struct_name.clone());

    let fields = match &item.fields {
        syn::Fields::Named(named) => {
            named
                .named
                .iter()
                .map(extract_field)
                .collect::<Result<Vec<_>, _>>()?
        }
        _ => return Err("Expected named fields".to_string()),
    };

    Ok(ParsedEntity {
        struct_name,
        table_name,
        is_view,
        fields,
        imports: Vec::new(), // filled by parse_entity_source
    })
}

/// Parse `#[sqlx_gen(kind = "...", table = "...")]` from struct attributes.
/// Returns (kind, table_name).
fn parse_sqlx_gen_struct_attrs(attrs: &[syn::Attribute]) -> (Option<String>, Option<String>) {
    let mut kind = None;
    let mut table_name = None;

    for attr in attrs {
        if attr.path().is_ident("sqlx_gen") {
            let tokens = attr.meta.to_token_stream().to_string();
            if let Some(k) = extract_attr_value(&tokens, "kind") {
                kind = Some(k);
            }
            if let Some(t) = extract_attr_value(&tokens, "table") {
                table_name = Some(t);
            }
        }
    }

    (kind, table_name)
}

/// Extract a named string value from an attribute token string.
/// e.g. extract_attr_value(`sqlx_gen(kind = "view", table = "users")`, "kind") -> Some("view")
fn extract_attr_value(tokens: &str, key: &str) -> Option<String> {
    let pattern = format!("{} = \"", key);
    let start = tokens.find(&pattern)? + pattern.len();
    let rest = &tokens[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Extract a ParsedField from a syn::Field.
fn extract_field(field: &syn::Field) -> Result<ParsedField, String> {
    let rust_name = field
        .ident
        .as_ref()
        .ok_or("Unnamed field")?
        .to_string();

    let column_name = get_sqlx_rename(&field.attrs).unwrap_or_else(|| rust_name.clone());
    let is_primary_key = has_sqlx_gen_primary_key(&field.attrs);

    let rust_type = field.ty.to_token_stream().to_string();
    let (is_nullable, inner_type) = extract_option_type(&field.ty);
    let inner_type = if is_nullable {
        inner_type
    } else {
        rust_type.clone()
    };

    Ok(ParsedField {
        rust_name,
        column_name,
        rust_type,
        is_nullable,
        inner_type,
        is_primary_key,
    })
}

/// Check for `#[sqlx_gen(primary_key)]` on a field.
fn has_sqlx_gen_primary_key(attrs: &[syn::Attribute]) -> bool {
    for attr in attrs {
        if attr.path().is_ident("sqlx_gen") {
            let tokens = attr.meta.to_token_stream().to_string();
            if tokens.contains("primary_key") {
                return true;
            }
        }
    }
    false
}

/// Extract `#[sqlx(rename = "...")]` value from field attributes.
fn get_sqlx_rename(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("sqlx") {
            let tokens = attr.meta.to_token_stream().to_string();
            return extract_attr_value(&tokens, "rename");
        }
    }
    None
}

/// Check if a type is `Option<T>` and extract the inner type.
fn extract_option_type(ty: &syn::Type) -> (bool, String) {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return (true, inner.to_token_stream().to_string());
                    }
                }
            }
        }
    }
    (false, String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- basic parsing ---

    #[test]
    fn test_parse_simple_table() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: i32,
                pub name: String,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.struct_name, "Users");
        assert_eq!(entity.table_name, "users");
        assert!(!entity.is_view);
        assert_eq!(entity.fields.len(), 2);
    }

    #[test]
    fn test_parse_view() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "view", table = "active_users")]
            pub struct ActiveUsers {
                pub id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.is_view);
        assert_eq!(entity.table_name, "active_users");
    }

    #[test]
    fn test_parse_table_not_view() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(!entity.is_view);
    }

    // --- primary key ---

    #[test]
    fn test_parse_primary_key() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                #[sqlx_gen(primary_key)]
                pub id: i32,
                pub name: String,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.fields[0].is_primary_key);
        assert!(!entity.fields[1].is_primary_key);
    }

    #[test]
    fn test_composite_primary_key() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "user_roles")]
            pub struct UserRoles {
                #[sqlx_gen(primary_key)]
                pub user_id: i32,
                #[sqlx_gen(primary_key)]
                pub role_id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.fields[0].is_primary_key);
        assert!(entity.fields[1].is_primary_key);
    }

    #[test]
    fn test_no_primary_key() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "logs")]
            pub struct Logs {
                pub message: String,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(!entity.fields[0].is_primary_key);
    }

    // --- sqlx rename ---

    #[test]
    fn test_sqlx_rename() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "connector")]
            pub struct Connector {
                #[sqlx(rename = "type")]
                pub connector_type: String,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.fields[0].rust_name, "connector_type");
        assert_eq!(entity.fields[0].column_name, "type");
    }

    #[test]
    fn test_no_rename_uses_field_name() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub name: String,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.fields[0].rust_name, "name");
        assert_eq!(entity.fields[0].column_name, "name");
    }

    // --- nullable types ---

    #[test]
    fn test_option_field_nullable() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub email: Option<String>,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.fields[0].is_nullable);
        assert_eq!(entity.fields[0].inner_type, "String");
    }

    #[test]
    fn test_non_option_not_nullable() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(!entity.fields[0].is_nullable);
        assert_eq!(entity.fields[0].inner_type, "i32");
    }

    #[test]
    fn test_option_complex_type() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub created_at: Option<chrono::NaiveDateTime>,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.fields[0].is_nullable);
        assert!(entity.fields[0].inner_type.contains("NaiveDateTime"));
    }

    // --- type preservation ---

    #[test]
    fn test_rust_type_preserved() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: uuid::Uuid,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.fields[0].rust_type.contains("Uuid"));
    }

    // --- error cases ---

    #[test]
    fn test_no_from_row_struct() {
        let source = r#"
            pub struct NotAnEntity {
                pub id: i32,
            }
        "#;
        let result = parse_entity_source(source);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_source() {
        let result = parse_entity_source("");
        assert!(result.is_err());
    }

    // --- fallback table name ---

    #[test]
    fn test_fallback_table_name_to_struct_name() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            pub struct Users {
                pub id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.table_name, "Users");
    }

    // --- combined attributes ---

    #[test]
    fn test_pk_with_rename() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "items")]
            pub struct Items {
                #[sqlx_gen(primary_key)]
                #[sqlx(rename = "itemID")]
                pub item_id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        let f = &entity.fields[0];
        assert!(f.is_primary_key);
        assert_eq!(f.column_name, "itemID");
        assert_eq!(f.rust_name, "item_id");
    }

    #[test]
    fn test_full_entity() {
        let source = r#"
            #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                #[sqlx_gen(primary_key)]
                pub id: i32,
                pub name: String,
                pub email: Option<String>,
                #[sqlx(rename = "createdAt")]
                pub created_at: chrono::NaiveDateTime,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.struct_name, "Users");
        assert_eq!(entity.table_name, "users");
        assert!(!entity.is_view);
        assert_eq!(entity.fields.len(), 4);

        assert!(entity.fields[0].is_primary_key);
        assert_eq!(entity.fields[0].rust_name, "id");

        assert!(!entity.fields[1].is_primary_key);
        assert_eq!(entity.fields[1].rust_type, "String");

        assert!(entity.fields[2].is_nullable);
        assert_eq!(entity.fields[2].inner_type, "String");

        assert_eq!(entity.fields[3].column_name, "createdAt");
        assert_eq!(entity.fields[3].rust_name, "created_at");
    }

    // --- imports extraction ---

    #[test]
    fn test_imports_extracted() {
        let source = r#"
            use chrono::{DateTime, Utc};
            use uuid::Uuid;
            use serde::{Serialize, Deserialize};

            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: Uuid,
                pub created_at: DateTime<Utc>,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.imports.len(), 2);
        assert!(entity.imports.iter().any(|i| i.contains("chrono")));
        assert!(entity.imports.iter().any(|i| i.contains("uuid")));
        // serde should be excluded
        assert!(!entity.imports.iter().any(|i| i.contains("serde")));
    }

    #[test]
    fn test_imports_empty_when_none() {
        let source = r#"
            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert!(entity.imports.is_empty());
    }

    #[test]
    fn test_imports_exclude_sqlx() {
        let source = r#"
            use sqlx::types::Uuid;
            use chrono::NaiveDateTime;

            #[derive(Debug, Clone, sqlx::FromRow)]
            #[sqlx_gen(kind = "table", table = "users")]
            pub struct Users {
                pub id: i32,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        assert_eq!(entity.imports.len(), 1);
        assert!(entity.imports[0].contains("chrono"));
    }
}
