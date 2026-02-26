use std::collections::{BTreeSet, HashMap};

use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::DatabaseKind;
use crate::codegen::{imports_for_derives, is_rust_keyword};
use crate::introspect::{SchemaInfo, TableInfo};
use crate::typemap;

pub fn generate_struct(
    table: &TableInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    extra_derives: &[String],
    type_overrides: &HashMap<String, String>,
    is_view: bool,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    for imp in imports_for_derives(extra_derives) {
        imports.insert(imp);
    }
    let struct_name = format_ident!("{}", table.name.to_upper_camel_case());

    // Build derive list
    imports.insert("use serde::{Serialize, Deserialize};".to_string());
    imports.insert("use sqlx_gen::SqlxGen;".to_string());
    let mut derive_tokens = vec![
        quote! { Debug },
        quote! { Clone },
        quote! { PartialEq },
        quote! { Eq },
        quote! { Serialize },
        quote! { Deserialize },
        quote! { sqlx::FromRow },
        quote! { SqlxGen },
    ];
    for d in extra_derives {
        let ident = format_ident!("{}", d);
        derive_tokens.push(quote! { #ident });
    }

    // Build fields
    let fields: Vec<TokenStream> = table
        .columns
        .iter()
        .map(|col| {
            let rust_type = resolve_column_type(col, db_kind, table, schema_info, type_overrides);
            if let Some(imp) = &rust_type.needs_import {
                imports.insert(imp.clone());
            }

            let field_name_snake = col.name.to_snake_case();
            // If the field name is a Rust keyword, prefix with table name
            // e.g. column "type" on table "connector" → "connector_type"
            let (effective_name, needs_rename) = if is_rust_keyword(&field_name_snake) {
                let prefixed = format!(
                    "{}_{}",
                    table.name.to_snake_case(),
                    field_name_snake
                );
                (prefixed, true)
            } else {
                let changed = field_name_snake != col.name;
                (field_name_snake, changed)
            };

            let field_ident = format_ident!("{}", effective_name);
            let type_tokens: TokenStream = rust_type.path.parse().unwrap_or_else(|_| {
                let fallback = format_ident!("String");
                quote! { #fallback }
            });

            let rename = if needs_rename {
                let original = &col.name;
                quote! { #[sqlx(rename = #original)] }
            } else {
                quote! {}
            };

            let pk_attr = if col.is_primary_key {
                quote! { #[sqlx_gen(primary_key)] }
            } else {
                quote! {}
            };

            quote! {
                #pk_attr
                #rename
                pub #field_ident: #type_tokens,
            }
        })
        .collect();

    let table_name_str = &table.name;
    let kind_str = if is_view { "view" } else { "table" };

    let tokens = quote! {
        #[derive(#(#derive_tokens),*)]
        #[sqlx_gen(kind = #kind_str, table = #table_name_str)]
        pub struct #struct_name {
            #(#fields)*
        }
    };

    (tokens, imports)
}

fn resolve_column_type(
    col: &crate::introspect::ColumnInfo,
    db_kind: DatabaseKind,
    table: &TableInfo,
    schema_info: &SchemaInfo,
    type_overrides: &HashMap<String, String>,
) -> typemap::RustType {
    // For MySQL ENUM columns, resolve to the generated enum type
    if db_kind == DatabaseKind::Mysql && col.udt_name.starts_with("enum(") {
        let enum_type_name = typemap::mysql::resolve_enum_type(&table.name, &col.name);
        let rt = typemap::RustType::with_import(
            &enum_type_name,
            &format!("use super::types::{};", enum_type_name),
        );
        return if col.is_nullable {
            rt.wrap_option()
        } else {
            rt
        };
    }

    typemap::map_column(col, db_kind, schema_info, type_overrides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codegen::parse_and_format;
    use crate::introspect::ColumnInfo;

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema_name: "public".to_string(),
            name: name.to_string(),
            columns,
        }
    }

    fn make_col(name: &str, udt_name: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: udt_name.to_string(),
            udt_name: udt_name.to_string(),
            is_nullable: nullable,
            is_primary_key: false,
            ordinal_position: 0,
            schema_name: "public".to_string(),
        }
    }

    fn gen(table: &TableInfo) -> String {
        let schema = SchemaInfo::default();
        let (tokens, _) = generate_struct(table, DatabaseKind::Postgres, &schema, &[], &HashMap::new(), false);
        parse_and_format(&tokens)
    }

    fn gen_with(
        table: &TableInfo,
        schema: &SchemaInfo,
        db: DatabaseKind,
        derives: &[String],
        overrides: &HashMap<String, String>,
    ) -> (String, BTreeSet<String>) {
        let (tokens, imports) = generate_struct(table, db, schema, derives, overrides, false);
        (parse_and_format(&tokens), imports)
    }

    // --- basic structure ---

    #[test]
    fn test_simple_table() {
        let table = make_table("users", vec![
            make_col("id", "int4", false),
            make_col("name", "text", false),
        ]);
        let code = gen(&table);
        assert!(code.contains("pub id: i32"));
        assert!(code.contains("pub name: String"));
    }

    #[test]
    fn test_struct_name_pascal_case() {
        let table = make_table("user_roles", vec![make_col("id", "int4", false)]);
        let code = gen(&table);
        assert!(code.contains("pub struct UserRoles"));
    }

    #[test]
    fn test_struct_name_simple() {
        let table = make_table("users", vec![make_col("id", "int4", false)]);
        let code = gen(&table);
        assert!(code.contains("pub struct Users"));
    }

    // --- nullable ---

    #[test]
    fn test_nullable_column() {
        let table = make_table("users", vec![make_col("email", "text", true)]);
        let code = gen(&table);
        assert!(code.contains("pub email: Option<String>"));
    }

    #[test]
    fn test_non_nullable_column() {
        let table = make_table("users", vec![make_col("name", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub name: String"));
        assert!(!code.contains("Option"));
    }

    #[test]
    fn test_mix_nullable() {
        let table = make_table("users", vec![
            make_col("id", "int4", false),
            make_col("bio", "text", true),
        ]);
        let code = gen(&table);
        assert!(code.contains("pub id: i32"));
        assert!(code.contains("pub bio: Option<String>"));
    }

    // --- keyword renaming ---

    #[test]
    fn test_keyword_type_renamed() {
        let table = make_table("connector", vec![make_col("type", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub connector_type: String"));
        assert!(code.contains("sqlx(rename = \"type\")"));
    }

    #[test]
    fn test_keyword_fn_renamed() {
        let table = make_table("item", vec![make_col("fn", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub item_fn: String"));
        assert!(code.contains("sqlx(rename = \"fn\")"));
    }

    #[test]
    fn test_keyword_match_renamed() {
        let table = make_table("game", vec![make_col("match", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub game_match: String"));
    }

    #[test]
    fn test_non_keyword_no_rename() {
        let table = make_table("users", vec![make_col("name", "text", false)]);
        let code = gen(&table);
        assert!(!code.contains("sqlx(rename"));
    }

    // --- snake_case renaming ---

    #[test]
    fn test_camel_case_column_renamed() {
        let table = make_table("users", vec![make_col("CreatedAt", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub created_at: String"));
        assert!(code.contains("sqlx(rename = \"CreatedAt\")"));
    }

    #[test]
    fn test_mixed_case_column_renamed() {
        let table = make_table("users", vec![make_col("firstName", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub first_name: String"));
        assert!(code.contains("sqlx(rename = \"firstName\")"));
    }

    #[test]
    fn test_already_snake_case_no_rename() {
        let table = make_table("users", vec![make_col("created_at", "text", false)]);
        let code = gen(&table);
        assert!(code.contains("pub created_at: String"));
        assert!(!code.contains("sqlx(rename"));
    }

    // --- derives ---

    #[test]
    fn test_default_derives() {
        let table = make_table("users", vec![make_col("id", "int4", false)]);
        let code = gen(&table);
        assert!(code.contains("Debug"));
        assert!(code.contains("Clone"));
        assert!(code.contains("sqlx::FromRow") || code.contains("sqlx :: FromRow"));
    }

    #[test]
    fn test_extra_derive_serialize() {
        let table = make_table("users", vec![make_col("id", "int4", false)]);
        let schema = SchemaInfo::default();
        let derives = vec!["Serialize".to_string()];
        let (code, _) = gen_with(&table, &schema, DatabaseKind::Postgres, &derives, &HashMap::new());
        assert!(code.contains("Serialize"));
    }

    #[test]
    fn test_extra_derives_both_serde() {
        let table = make_table("users", vec![make_col("id", "int4", false)]);
        let schema = SchemaInfo::default();
        let derives = vec!["Serialize".to_string(), "Deserialize".to_string()];
        let (_, imports) = gen_with(&table, &schema, DatabaseKind::Postgres, &derives, &HashMap::new());
        assert!(imports.iter().any(|i| i.contains("serde")));
    }

    // --- imports ---

    #[test]
    fn test_uuid_import() {
        let table = make_table("users", vec![make_col("id", "uuid", false)]);
        let schema = SchemaInfo::default();
        let (_, imports) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &HashMap::new());
        assert!(imports.iter().any(|i| i.contains("uuid::Uuid")));
    }

    #[test]
    fn test_timestamptz_import() {
        let table = make_table("users", vec![make_col("created_at", "timestamptz", false)]);
        let schema = SchemaInfo::default();
        let (_, imports) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &HashMap::new());
        assert!(imports.iter().any(|i| i.contains("chrono")));
    }

    #[test]
    fn test_int4_only_serde_import() {
        let table = make_table("users", vec![make_col("id", "int4", false)]);
        let schema = SchemaInfo::default();
        let (_, imports) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &HashMap::new());
        assert_eq!(imports.len(), 2);
        assert!(imports.iter().any(|i| i.contains("serde")));
        assert!(imports.iter().any(|i| i.contains("sqlx_gen::SqlxGen")));
    }

    #[test]
    fn test_multiple_imports_collected() {
        let table = make_table("users", vec![
            make_col("id", "uuid", false),
            make_col("created_at", "timestamptz", false),
        ]);
        let schema = SchemaInfo::default();
        let (_, imports) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &HashMap::new());
        assert!(imports.iter().any(|i| i.contains("uuid")));
        assert!(imports.iter().any(|i| i.contains("chrono")));
    }

    // --- MySQL enum ---

    #[test]
    fn test_mysql_enum_column() {
        let table = make_table("users", vec![ColumnInfo {
            name: "status".to_string(),
            data_type: "enum".to_string(),
            udt_name: "enum('active','inactive')".to_string(),
            is_nullable: false,
            is_primary_key: false,
            ordinal_position: 0,
            schema_name: "test_db".to_string(),
        }]);
        let schema = SchemaInfo::default();
        let (code, imports) = gen_with(&table, &schema, DatabaseKind::Mysql, &[], &HashMap::new());
        assert!(code.contains("UsersStatus"));
        assert!(imports.iter().any(|i| i.contains("super::types::")));
    }

    #[test]
    fn test_mysql_enum_nullable() {
        let table = make_table("users", vec![ColumnInfo {
            name: "status".to_string(),
            data_type: "enum".to_string(),
            udt_name: "enum('a','b')".to_string(),
            is_nullable: true,
            is_primary_key: false,
            ordinal_position: 0,
            schema_name: "test_db".to_string(),
        }]);
        let schema = SchemaInfo::default();
        let (code, _) = gen_with(&table, &schema, DatabaseKind::Mysql, &[], &HashMap::new());
        assert!(code.contains("Option<UsersStatus>"));
    }

    // --- type overrides ---

    #[test]
    fn test_type_override() {
        let table = make_table("users", vec![make_col("data", "jsonb", false)]);
        let schema = SchemaInfo::default();
        let mut overrides = HashMap::new();
        overrides.insert("jsonb".to_string(), "MyJson".to_string());
        let (code, _) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &overrides);
        assert!(code.contains("pub data: MyJson"));
    }

    #[test]
    fn test_type_override_absent() {
        let table = make_table("users", vec![make_col("data", "jsonb", false)]);
        let schema = SchemaInfo::default();
        let (code, _) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &HashMap::new());
        assert!(code.contains("Value"));
    }

    #[test]
    fn test_type_override_nullable() {
        let table = make_table("users", vec![make_col("data", "jsonb", true)]);
        let schema = SchemaInfo::default();
        let mut overrides = HashMap::new();
        overrides.insert("jsonb".to_string(), "MyJson".to_string());
        let (code, _) = gen_with(&table, &schema, DatabaseKind::Postgres, &[], &overrides);
        assert!(code.contains("Option<MyJson>"));
    }
}
