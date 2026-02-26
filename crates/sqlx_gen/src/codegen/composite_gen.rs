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

    imports.insert("use serde::{Serialize, Deserialize};".to_string());
    imports.insert("use sqlx_gen::SqlxGen;".to_string());
    let mut derive_tokens = vec![
        quote! { Debug },
        quote! { Clone },
        quote! { PartialEq },
        quote! { Eq },
        quote! { Serialize },
        quote! { Deserialize },
        quote! { sqlx::Type },
        quote! { SqlxGen },
    ];
    for d in extra_derives {
        let ident = format_ident!("{}", d);
        derive_tokens.push(quote! { #ident });
    }

    // Schema-qualify the type name for non-public schemas so sqlx can find the type
    let pg_name = if composite.schema_name != "public" {
        format!("{}.{}", composite.schema_name, composite.name)
    } else {
        composite.name.clone()
    };
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
        #[sqlx_gen(kind = "composite")]
        #type_attr
        pub struct #struct_name {
            #(#fields)*
        }
    };

    (tokens, imports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codegen::parse_and_format;
    use crate::introspect::ColumnInfo;

    fn make_composite(name: &str, fields: Vec<ColumnInfo>) -> CompositeTypeInfo {
        CompositeTypeInfo {
            schema_name: "public".to_string(),
            name: name.to_string(),
            fields,
        }
    }

    fn make_field(name: &str, udt_name: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: udt_name.to_string(),
            udt_name: udt_name.to_string(),
            is_nullable: nullable,
            is_primary_key: false,
            ordinal_position: 0,
            schema_name: "public".to_string(),
            column_default: None,
        }
    }

    fn gen(composite: &CompositeTypeInfo) -> String {
        let schema = SchemaInfo::default();
        let (tokens, _) = generate_composite(composite, DatabaseKind::Postgres, &schema, &[], &HashMap::new());
        parse_and_format(&tokens)
    }

    fn gen_with(
        composite: &CompositeTypeInfo,
        derives: &[String],
        overrides: &HashMap<String, String>,
    ) -> (String, BTreeSet<String>) {
        let schema = SchemaInfo::default();
        let (tokens, imports) = generate_composite(composite, DatabaseKind::Postgres, &schema, derives, overrides);
        (parse_and_format(&tokens), imports)
    }

    // --- basic structure ---

    #[test]
    fn test_simple_composite() {
        let c = make_composite("address", vec![
            make_field("street", "text", false),
            make_field("city", "text", false),
        ]);
        let code = gen(&c);
        assert!(code.contains("pub street: String"));
        assert!(code.contains("pub city: String"));
    }

    #[test]
    fn test_name_pascal_case() {
        let c = make_composite("geo_point", vec![make_field("x", "float8", false)]);
        let code = gen(&c);
        assert!(code.contains("pub struct GeoPoint"));
    }

    #[test]
    fn test_doc_comment() {
        let c = make_composite("address", vec![make_field("x", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("Composite type: public.address"));
    }

    #[test]
    fn test_sqlx_type_name() {
        let c = make_composite("geo_point", vec![make_field("x", "float8", false)]);
        let code = gen(&c);
        assert!(code.contains("sqlx(type_name = \"geo_point\")"));
    }

    #[test]
    fn test_non_public_schema_qualified_type_name() {
        let c = CompositeTypeInfo {
            schema_name: "geo".to_string(),
            name: "point".to_string(),
            fields: vec![make_field("x", "float8", false)],
        };
        let schema = SchemaInfo::default();
        let (tokens, _) = generate_composite(&c, DatabaseKind::Postgres, &schema, &[], &HashMap::new());
        let code = parse_and_format(&tokens);
        assert!(code.contains("sqlx(type_name = \"geo.point\")"));
    }

    #[test]
    fn test_public_schema_not_qualified() {
        let c = make_composite("address", vec![make_field("x", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("sqlx(type_name = \"address\")"));
        // type_name should NOT be schema-qualified for public schema
        assert!(!code.contains("type_name = \"public.address\""));
    }

    // --- fields ---

    #[test]
    fn test_nullable_field() {
        let c = make_composite("address", vec![make_field("zip", "text", true)]);
        let code = gen(&c);
        assert!(code.contains("Option<String>"));
    }

    #[test]
    fn test_non_nullable_field() {
        let c = make_composite("address", vec![make_field("city", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("pub city: String"));
        assert!(!code.contains("Option"));
    }

    #[test]
    fn test_keyword_field_prefixed() {
        let c = make_composite("item", vec![make_field("type", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("pub item_type: String"));
        assert!(code.contains("sqlx(rename = \"type\")"));
    }

    // --- rename ---

    #[test]
    fn test_camel_case_field_renamed() {
        let c = make_composite("address", vec![make_field("StreetName", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("pub street_name: String"));
        assert!(code.contains("sqlx(rename = \"StreetName\")"));
    }

    #[test]
    fn test_snake_case_field_no_rename() {
        let c = make_composite("address", vec![make_field("street_name", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("pub street_name: String"));
        assert!(!code.contains("sqlx(rename"));
    }

    // --- types ---

    #[test]
    fn test_int4_field() {
        let c = make_composite("data", vec![make_field("count", "int4", false)]);
        let code = gen(&c);
        assert!(code.contains("pub count: i32"));
    }

    #[test]
    fn test_uuid_field_import() {
        let c = make_composite("data", vec![make_field("id", "uuid", false)]);
        let (_, imports) = gen_with(&c, &[], &HashMap::new());
        assert!(imports.iter().any(|i| i.contains("uuid::Uuid")));
    }

    #[test]
    fn test_text_field() {
        let c = make_composite("data", vec![make_field("label", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("pub label: String"));
    }

    // --- derives ---

    #[test]
    fn test_default_derives() {
        let c = make_composite("data", vec![make_field("x", "text", false)]);
        let code = gen(&c);
        assert!(code.contains("Debug"));
        assert!(code.contains("Clone"));
        assert!(code.contains("sqlx::Type") || code.contains("sqlx :: Type"));
    }

    #[test]
    fn test_extra_derive() {
        let c = make_composite("data", vec![make_field("x", "text", false)]);
        let derives = vec!["Serialize".to_string()];
        let (code, _) = gen_with(&c, &derives, &HashMap::new());
        assert!(code.contains("Serialize"));
    }

    // --- overrides ---

    #[test]
    fn test_type_override() {
        let c = make_composite("data", vec![make_field("payload", "jsonb", false)]);
        let mut overrides = HashMap::new();
        overrides.insert("jsonb".to_string(), "MyJson".to_string());
        let (code, _) = gen_with(&c, &[], &overrides);
        assert!(code.contains("pub payload: MyJson"));
    }
}
