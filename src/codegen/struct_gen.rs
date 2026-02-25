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
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    for imp in imports_for_derives(extra_derives) {
        imports.insert(imp);
    }
    let struct_name = format_ident!("{}", table.name.to_upper_camel_case());

    // Build derive list
    let mut derive_tokens = vec![
        quote! { Debug },
        quote! { Clone },
        quote! { sqlx::FromRow },
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

            quote! {
                #rename
                pub #field_ident: #type_tokens,
            }
        })
        .collect();

    let tokens = quote! {
        #[derive(#(#derive_tokens),*)]
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
