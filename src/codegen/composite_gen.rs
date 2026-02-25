use std::collections::{BTreeSet, HashMap};

use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::DatabaseKind;
use crate::codegen::{imports_for_derives, is_rust_keyword};
use crate::introspect::{CompositeTypeInfo, SchemaInfo};
use crate::typemap;

pub fn generate_composite(
    composite: &CompositeTypeInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    extra_derives: &[String],
    type_overrides: &HashMap<String, String>,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    for imp in imports_for_derives(extra_derives) {
        imports.insert(imp);
    }
    let struct_name = format_ident!("{}", composite.name.to_upper_camel_case());

    let doc = format!(
        "Composite type: {}.{}",
        composite.schema_name, composite.name
    );

    let mut derive_tokens = vec![
        quote! { Debug },
        quote! { Clone },
        quote! { sqlx::Type },
    ];
    for d in extra_derives {
        let ident = format_ident!("{}", d);
        derive_tokens.push(quote! { #ident });
    }

    let pg_name = &composite.name;
    let type_attr = quote! { #[sqlx(type_name = #pg_name)] };

    let fields: Vec<TokenStream> = composite
        .fields
        .iter()
        .map(|col| {
            let rust_type = typemap::map_column(col, db_kind, schema_info, type_overrides);
            if let Some(imp) = &rust_type.needs_import {
                imports.insert(imp.clone());
            }

            let field_name_snake = col.name.to_snake_case();
            let (effective_name, needs_rename) = if is_rust_keyword(&field_name_snake) {
                let prefixed = format!(
                    "{}_{}",
                    composite.name.to_snake_case(),
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
        #[doc = #doc]
        #[derive(#(#derive_tokens),*)]
        #type_attr
        pub struct #struct_name {
            #(#fields)*
        }
    };

    (tokens, imports)
}
