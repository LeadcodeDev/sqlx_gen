use std::collections::{BTreeSet, HashMap};

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::{DatabaseKind, DomainStyle, TimeCrate};
use crate::codegen::rust_type_name_for;
use crate::introspect::{DomainInfo, SchemaInfo};
use crate::typemap;

pub fn generate_domain(
    domain: &DomainInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    type_overrides: &HashMap<String, String>,
    time_crate: TimeCrate,
) -> (TokenStream, BTreeSet<String>) {
    generate_domain_with_style(
        domain,
        db_kind,
        schema_info,
        type_overrides,
        time_crate,
        DomainStyle::Alias,
    )
}

pub fn generate_domain_with_style(
    domain: &DomainInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    type_overrides: &HashMap<String, String>,
    time_crate: TimeCrate,
    style: DomainStyle,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    let rust_name = rust_type_name_for(schema_info, &domain.schema_name, &domain.name);
    let alias_name = format_ident!("{}", rust_name);

    let doc = format!(
        "Domain: {}.{} (base: {})",
        domain.schema_name, domain.name, domain.base_type
    );

    // Create a fake ColumnInfo to reuse the type mapper for the base type
    let fake_col = crate::introspect::ColumnInfo {
        name: String::new(),
        data_type: domain.base_type.clone(),
        udt_name: domain.base_type.clone(),
        udt_schema: None,
        is_nullable: false,
        is_primary_key: false,
        ordinal_position: 0,
        schema_name: domain.schema_name.clone(),
        column_default: None,
    };

    let rust_type =
        typemap::map_column(&fake_col, db_kind, schema_info, type_overrides, time_crate);
    if let Some(imp) = &rust_type.needs_import {
        imports.insert(imp.clone());
    }

    let type_tokens: TokenStream = rust_type.path.parse().unwrap_or_else(|_| {
        let fallback = format_ident!("String");
        quote! { #fallback }
    });

    let domain_doc = "sqlx_gen:kind=domain";
    let tokens = match style {
        DomainStyle::Alias => quote! {
            #[doc = #doc]
            #[doc = #domain_doc]
            pub type #alias_name = #type_tokens;
        },
        DomainStyle::Newtype => {
            imports.insert("use serde::{Serialize, Deserialize};".to_string());
            quote! {
                #[doc = #doc]
                #[doc = #domain_doc]
                #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
                #[sqlx(transparent)]
                pub struct #alias_name(pub #type_tokens);
            }
        }
    };

    (tokens, imports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codegen::parse_and_format;

    fn make_domain(name: &str, base: &str) -> DomainInfo {
        DomainInfo {
            schema_name: "public".to_string(),
            name: name.to_string(),
            base_type: base.to_string(),
        }
    }

    fn gen(domain: &DomainInfo) -> (String, BTreeSet<String>) {
        let schema = SchemaInfo::default();
        let (tokens, imports) = generate_domain(
            domain,
            DatabaseKind::Postgres,
            &schema,
            &HashMap::new(),
            TimeCrate::Chrono,
        );
        (parse_and_format(&tokens).unwrap(), imports)
    }

    fn gen_with_overrides(
        domain: &DomainInfo,
        overrides: &HashMap<String, String>,
    ) -> (String, BTreeSet<String>) {
        let schema = SchemaInfo::default();
        let (tokens, imports) = generate_domain(
            domain,
            DatabaseKind::Postgres,
            &schema,
            overrides,
            TimeCrate::Chrono,
        );
        (parse_and_format(&tokens).unwrap(), imports)
    }

    #[test]
    fn test_domain_text() {
        let d = make_domain("email", "text");
        let (code, _) = gen(&d);
        assert!(code.contains("pub type Email = String"));
    }

    #[test]
    fn test_domain_int4() {
        let d = make_domain("positive_int", "int4");
        let (code, _) = gen(&d);
        assert!(code.contains("pub type PositiveInt = i32"));
    }

    #[test]
    fn test_domain_uuid() {
        let d = make_domain("my_uuid", "uuid");
        let (code, imports) = gen(&d);
        assert!(code.contains("pub type MyUuid = Uuid"));
        assert!(imports.iter().any(|i| i.contains("uuid::Uuid")));
    }

    #[test]
    fn test_doc_comment() {
        let d = make_domain("email", "text");
        let (code, _) = gen(&d);
        assert!(code.contains("Domain: public.email (base: text)"));
    }

    #[test]
    fn test_import_when_needed() {
        let d = make_domain("my_uuid", "uuid");
        let (_, imports) = gen(&d);
        assert!(!imports.is_empty());
    }

    #[test]
    fn test_no_import_simple_type() {
        let d = make_domain("email", "text");
        let (_, imports) = gen(&d);
        assert!(imports.is_empty());
    }

    #[test]
    fn test_pascal_case_name() {
        let d = make_domain("email_address", "text");
        let (code, _) = gen(&d);
        assert!(code.contains("pub type EmailAddress"));
    }

    #[test]
    fn test_type_override() {
        let d = make_domain("json_data", "jsonb");
        let mut overrides = HashMap::new();
        overrides.insert("jsonb".to_string(), "MyJson".to_string());
        let (code, _) = gen_with_overrides(&d, &overrides);
        assert!(code.contains("pub type JsonData = MyJson"));
    }

    #[test]
    fn test_domain_jsonb() {
        let d = make_domain("data", "jsonb");
        let (code, imports) = gen(&d);
        assert!(code.contains("Value"));
        assert!(imports.iter().any(|i| i.contains("serde_json")));
    }

    #[test]
    fn test_domain_timestamptz() {
        let d = make_domain("created", "timestamptz");
        let (_, imports) = gen(&d);
        assert!(imports.iter().any(|i| i.contains("chrono")));
    }

    // ========== DomainStyle::Newtype ==========

    fn gen_newtype(domain: &DomainInfo) -> (String, BTreeSet<String>) {
        let schema = SchemaInfo::default();
        let (tokens, imports) = generate_domain_with_style(
            domain,
            DatabaseKind::Postgres,
            &schema,
            &HashMap::new(),
            TimeCrate::Chrono,
            DomainStyle::Newtype,
        );
        (parse_and_format(&tokens).unwrap(), imports)
    }

    #[test]
    fn test_newtype_emits_tuple_struct() {
        let d = make_domain("email", "text");
        let (code, _) = gen_newtype(&d);
        assert!(
            code.contains("pub struct Email(pub String)"),
            "newtype must wrap the base type in a tuple struct, got:\n{}",
            code
        );
    }

    #[test]
    fn test_newtype_uses_transparent_derive() {
        let d = make_domain("email", "text");
        let (code, _) = gen_newtype(&d);
        assert!(code.contains("#[sqlx(transparent)]"));
        assert!(code.contains("sqlx::Type"));
    }

    #[test]
    fn test_newtype_keeps_doc_comments() {
        let d = make_domain("email", "text");
        let (code, _) = gen_newtype(&d);
        assert!(code.contains("Domain: public.email (base: text)"));
        assert!(code.contains("sqlx_gen:kind=domain"));
    }

    #[test]
    fn test_newtype_wraps_uuid_with_import() {
        let d = make_domain("my_uuid", "uuid");
        let (code, imports) = gen_newtype(&d);
        assert!(code.contains("pub struct MyUuid(pub Uuid)"));
        assert!(imports.iter().any(|i| i.contains("uuid::Uuid")));
    }

    #[test]
    fn test_newtype_does_not_emit_type_alias() {
        let d = make_domain("email", "text");
        let (code, _) = gen_newtype(&d);
        assert!(!code.contains("pub type Email"));
    }
}
