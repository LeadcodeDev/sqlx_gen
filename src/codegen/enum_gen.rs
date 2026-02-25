use std::collections::BTreeSet;

use heck::ToUpperCamelCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::DatabaseKind;
use crate::codegen::imports_for_derives;
use crate::introspect::EnumInfo;

pub fn generate_enum(
    enum_info: &EnumInfo,
    db_kind: DatabaseKind,
    extra_derives: &[String],
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    for imp in imports_for_derives(extra_derives) {
        imports.insert(imp);
    }
    let enum_name = format_ident!("{}", enum_info.name.to_upper_camel_case());

    let doc = format!("Enum: {}.{}", enum_info.schema_name, enum_info.name);

    let mut derive_tokens = vec![
        quote! { Debug },
        quote! { Clone },
        quote! { PartialEq },
        quote! { sqlx::Type },
    ];
    for d in extra_derives {
        let ident = format_ident!("{}", d);
        derive_tokens.push(quote! { #ident });
    }

    // For PG, add #[sqlx(type_name = "...")]
    let type_attr = if db_kind == DatabaseKind::Postgres {
        let pg_name = &enum_info.name;
        quote! { #[sqlx(type_name = #pg_name)] }
    } else {
        quote! {}
    };

    let variants: Vec<TokenStream> = enum_info
        .variants
        .iter()
        .map(|v| {
            let variant_pascal = v.to_upper_camel_case();
            let variant_ident = format_ident!("{}", variant_pascal);

            let rename = if variant_pascal != *v {
                quote! { #[sqlx(rename = #v)] }
            } else {
                quote! {}
            };

            quote! {
                #rename
                #variant_ident,
            }
        })
        .collect();

    let tokens = quote! {
        #[doc = #doc]
        #[derive(#(#derive_tokens),*)]
        #type_attr
        pub enum #enum_name {
            #(#variants)*
        }
    };

    (tokens, imports)
}
