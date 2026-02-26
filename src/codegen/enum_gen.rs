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

    imports.insert("use serde::{Serialize, Deserialize};".to_string());
    let mut derive_tokens = vec![
        quote! { Debug },
        quote! { Clone },
        quote! { PartialEq },
        quote! { Eq },
        quote! { Serialize },
        quote! { Deserialize },
        quote! { sqlx::Type },
    ];
    for d in extra_derives {
        let ident = format_ident!("{}", d);
        derive_tokens.push(quote! { #ident });
    }

    // For PG, add #[sqlx(type_name = "...")]
    // Schema-qualify the type name for non-public schemas so sqlx can find the type
    let type_attr = if db_kind == DatabaseKind::Postgres {
        let pg_name = if enum_info.schema_name != "public" {
            format!("{}.{}", enum_info.schema_name, enum_info.name)
        } else {
            enum_info.name.clone()
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codegen::parse_and_format;

    fn make_enum(name: &str, variants: Vec<&str>) -> EnumInfo {
        EnumInfo {
            schema_name: "public".to_string(),
            name: name.to_string(),
            variants: variants.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    fn gen(info: &EnumInfo, db: DatabaseKind) -> String {
        let (tokens, _) = generate_enum(info, db, &[]);
        parse_and_format(&tokens)
    }

    fn gen_with_derives(
        info: &EnumInfo,
        db: DatabaseKind,
        derives: &[String],
    ) -> (String, BTreeSet<String>) {
        let (tokens, imports) = generate_enum(info, db, derives);
        (parse_and_format(&tokens), imports)
    }

    // --- basic structure ---

    #[test]
    fn test_enum_variants() {
        let e = make_enum("status", vec!["active", "inactive"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("Active"));
        assert!(code.contains("Inactive"));
    }

    #[test]
    fn test_enum_name_pascal_case() {
        let e = make_enum("user_status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("pub enum UserStatus"));
    }

    #[test]
    fn test_doc_comment() {
        let e = make_enum("status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("Enum: public.status"));
    }

    // --- sqlx attributes ---

    #[test]
    fn test_postgres_has_type_name() {
        let e = make_enum("user_status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("sqlx(type_name = \"user_status\")"));
    }

    #[test]
    fn test_postgres_non_public_schema_qualified_type_name() {
        let e = EnumInfo {
            schema_name: "auth".to_string(),
            name: "role".to_string(),
            variants: vec!["admin".to_string(), "user".to_string()],
        };
        let (tokens, _) = generate_enum(&e, DatabaseKind::Postgres, &[]);
        let code = parse_and_format(&tokens);
        assert!(code.contains("sqlx(type_name = \"auth.role\")"));
    }

    #[test]
    fn test_postgres_public_schema_not_qualified() {
        let e = make_enum("status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("sqlx(type_name = \"status\")"));
        // type_name should NOT be schema-qualified for public schema
        assert!(!code.contains("type_name = \"public.status\""));
    }

    #[test]
    fn test_mysql_no_type_name() {
        let e = make_enum("status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Mysql);
        assert!(!code.contains("type_name"));
    }

    #[test]
    fn test_sqlite_no_type_name() {
        let e = make_enum("status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Sqlite);
        assert!(!code.contains("type_name"));
    }

    // --- rename variants ---

    #[test]
    fn test_snake_case_variant_renamed() {
        let e = make_enum("status", vec!["in_progress"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("InProgress"));
        assert!(code.contains("sqlx(rename = \"in_progress\")"));
    }

    #[test]
    fn test_lowercase_variant_renamed() {
        let e = make_enum("status", vec!["active"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("Active"));
        assert!(code.contains("sqlx(rename = \"active\")"));
    }

    #[test]
    fn test_already_pascal_no_rename() {
        let e = make_enum("status", vec!["Active"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("Active"));
        assert!(!code.contains("sqlx(rename"));
    }

    #[test]
    fn test_upper_case_variant_renamed() {
        let e = make_enum("status", vec!["UPPER_CASE"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("UpperCase"));
        assert!(code.contains("sqlx(rename = \"UPPER_CASE\")"));
    }

    // --- derives ---

    #[test]
    fn test_default_derives() {
        let e = make_enum("status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("Debug"));
        assert!(code.contains("Clone"));
        assert!(code.contains("PartialEq"));
        assert!(code.contains("sqlx::Type") || code.contains("sqlx :: Type"));
    }

    #[test]
    fn test_extra_derive_serialize() {
        let e = make_enum("status", vec!["a"]);
        let derives = vec!["Serialize".to_string()];
        let (code, _) = gen_with_derives(&e, DatabaseKind::Postgres, &derives);
        assert!(code.contains("Serialize"));
    }

    #[test]
    fn test_extra_derives_serde_imports() {
        let e = make_enum("status", vec!["a"]);
        let derives = vec!["Serialize".to_string(), "Deserialize".to_string()];
        let (_, imports) = gen_with_derives(&e, DatabaseKind::Postgres, &derives);
        assert!(imports.iter().any(|i| i.contains("serde")));
    }

    // --- imports ---

    #[test]
    fn test_no_extra_derives_has_serde_import() {
        let e = make_enum("status", vec!["a"]);
        let (_, imports) = gen_with_derives(&e, DatabaseKind::Postgres, &[]);
        assert!(imports.iter().any(|i| i.contains("serde")));
    }

    #[test]
    fn test_serde_import_present() {
        let e = make_enum("status", vec!["a"]);
        let derives = vec!["Serialize".to_string()];
        let (_, imports) = gen_with_derives(&e, DatabaseKind::Postgres, &derives);
        assert!(!imports.is_empty());
    }

    // --- edge cases ---

    #[test]
    fn test_single_variant() {
        let e = make_enum("status", vec!["only"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("Only"));
    }

    #[test]
    fn test_many_variants() {
        let variants: Vec<&str> = vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
        let e = make_enum("status", variants);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("A,"));
        assert!(code.contains("J,"));
    }

    #[test]
    fn test_variant_with_digits() {
        let e = make_enum("version", vec!["v2"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("V2"));
    }

    #[test]
    fn test_enum_name_with_double_underscores() {
        let e = make_enum("my__enum", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("pub enum MyEnum"));
    }
}
