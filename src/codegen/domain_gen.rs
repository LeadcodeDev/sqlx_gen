use std::collections::{BTreeSet, HashMap};

use heck::ToUpperCamelCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::DatabaseKind;
use crate::introspect::{DomainInfo, SchemaInfo};
use crate::typemap;

pub fn generate_domain(
    domain: &DomainInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    type_overrides: &HashMap<String, String>,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    let alias_name = format_ident!("{}", domain.name.to_upper_camel_case());

    let doc = format!(
        "Domain: {}.{} (base: {})",
        domain.schema_name, domain.name, domain.base_type
    );

    // Create a fake ColumnInfo to reuse the type mapper for the base type
    let fake_col = crate::introspect::ColumnInfo {
        name: String::new(),
        data_type: domain.base_type.clone(),
        udt_name: domain.base_type.clone(),
        is_nullable: false,
        ordinal_position: 0,
        schema_name: domain.schema_name.clone(),
    };

    let rust_type = typemap::map_column(&fake_col, db_kind, schema_info, type_overrides);
    if let Some(imp) = &rust_type.needs_import {
        imports.insert(imp.clone());
    }

    let type_tokens: TokenStream = rust_type.path.parse().unwrap_or_else(|_| {
        let fallback = format_ident!("String");
        quote! { #fallback }
    });

    let tokens = quote! {
        #[doc = #doc]
        pub type #alias_name = #type_tokens;
    };

    (tokens, imports)
}
