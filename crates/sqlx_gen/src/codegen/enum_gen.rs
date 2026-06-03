use std::collections::BTreeSet;

use heck::ToUpperCamelCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::DatabaseKind;
use crate::codegen::{imports_for_derives, rust_type_name_for};
use crate::introspect::{EnumInfo, SchemaInfo};

/// Detect two SQL enum variants that collapse to the same Rust identifier after
/// `to_upper_camel_case` (e.g. `"foo bar"` and `"foo_bar"` both become `FooBar`).
/// Returns an error pointing at the offending pair so the user can rename one
/// side in the database before regenerating.
pub fn check_variant_collisions(enum_info: &EnumInfo) -> crate::error::Result<()> {
    use std::collections::BTreeMap;
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for v in &enum_info.variants {
        let pascal = v.to_upper_camel_case();
        if let Some(prev) = seen.get(pascal.as_str()).copied() {
            return Err(crate::error::Error::Config(format!(
                "Enum '{}.{}': SQL variants '{}' and '{}' both map to Rust identifier '{}'. \
                 Rename one of them in the database or use a custom mapping.",
                enum_info.schema_name, enum_info.name, prev, v, pascal
            )));
        }
        seen.insert(pascal, v.as_str());
    }
    Ok(())
}

pub fn generate_enum(
    enum_info: &EnumInfo,
    db_kind: DatabaseKind,
    extra_derives: &[String],
) -> (TokenStream, BTreeSet<String>) {
    // Backwards-compatible entry point — uses an empty SchemaInfo so the
    // enum keeps its bare PascalCase name (no schema prefix).
    generate_enum_with_schema(enum_info, db_kind, extra_derives, &SchemaInfo::default())
}

pub fn generate_enum_with_schema(
    enum_info: &EnumInfo,
    db_kind: DatabaseKind,
    extra_derives: &[String],
    schema_info: &SchemaInfo,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();
    for imp in imports_for_derives(extra_derives) {
        imports.insert(imp);
    }

    let rust_name = rust_type_name_for(schema_info, &enum_info.schema_name, &enum_info.name);
    let enum_name = format_ident!("{}", rust_name);
    let doc = format!("Enum: {}.{}", enum_info.schema_name, enum_info.name);

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

    // For PG, add #[sqlx(type_name = "...")] — always unqualified.
    // sqlx 0.8's PgTypeInfo::with_name does NOT accept schema-qualified names; emitting
    // "schema.type" causes runtime decode failures. The user is expected to set
    // `search_path` on the connection so that PG resolves the unqualified type name.
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

    let default_impl = if let Some(ref default_variant) = enum_info.default_variant {
        let variant_pascal = default_variant.to_upper_camel_case();
        let variant_ident = format_ident!("{}", variant_pascal);
        quote! {
            impl Default for #enum_name {
                fn default() -> Self {
                    Self::#variant_ident
                }
            }
        }
    } else {
        quote! {}
    };

    // Postgres arrays of an enum (`my_enum[]`) require an explicit
    // PgHasArrayType impl on the Rust side; otherwise sqlx fails at decode
    // with "unsupported type _my_enum". The array type name in PG is the
    // base type prefixed with '_'.
    let array_type_impl = if db_kind == DatabaseKind::Postgres {
        let array_type_name = format!("_{}", enum_info.name);
        quote! {
            impl sqlx::postgres::PgHasArrayType for #enum_name {
                fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                    sqlx::postgres::PgTypeInfo::with_name(#array_type_name)
                }
            }
        }
    } else {
        quote! {}
    };

    let schema_name_str = &enum_info.schema_name;
    let enum_name_str = &enum_info.name;

    let tokens = quote! {
        #[doc = #doc]
        #[derive(#(#derive_tokens),*)]
        #[sqlx_gen(kind = "enum", schema = #schema_name_str, name = #enum_name_str)]
        #type_attr
        pub enum #enum_name {
            #(#variants)*
        }

        #default_impl

        #array_type_impl
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
            default_variant: None,
        }
    }

    fn gen(info: &EnumInfo, db: DatabaseKind) -> String {
        let (tokens, _) = generate_enum(info, db, &[]);
        parse_and_format(&tokens).unwrap()
    }

    fn gen_with_derives(
        info: &EnumInfo,
        db: DatabaseKind,
        derives: &[String],
    ) -> (String, BTreeSet<String>) {
        let (tokens, imports) = generate_enum(info, db, derives);
        (parse_and_format(&tokens).unwrap(), imports)
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

    #[test]
    fn test_sqlx_gen_attr_has_schema_and_name() {
        let e = make_enum("status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("sqlx_gen(kind = \"enum\", schema = \"public\", name = \"status\")"));
    }

    #[test]
    fn test_sqlx_gen_attr_non_public_schema() {
        let e = EnumInfo {
            schema_name: "auth".to_string(),
            name: "role".to_string(),
            variants: vec!["admin".to_string(), "user".to_string()],
            default_variant: None,
        };
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("sqlx_gen(kind = \"enum\", schema = \"auth\", name = \"role\")"));
    }

    // --- sqlx attributes ---

    #[test]
    fn test_postgres_has_type_name() {
        let e = make_enum("user_status", vec!["a"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("sqlx(type_name = \"user_status\")"));
    }

    #[test]
    fn test_check_variant_collisions_detects_after_camel_case() {
        let e = EnumInfo {
            schema_name: "public".into(),
            name: "weird".into(),
            variants: vec!["foo bar".into(), "foo_bar".into()],
            default_variant: None,
        };
        let result = check_variant_collisions(&e);
        assert!(result.is_err(), "must detect collision");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("FooBar"),
            "error must mention conflicting Rust ident, got: {}",
            msg
        );
        assert!(msg.contains("foo bar") || msg.contains("foo_bar"));
    }

    #[test]
    fn test_check_variant_collisions_accepts_distinct_variants() {
        let e = make_enum("status", vec!["active", "inactive"]);
        assert!(check_variant_collisions(&e).is_ok());
    }

    #[test]
    fn test_check_variant_collisions_accepts_single_variant() {
        let e = make_enum("status", vec!["only"]);
        assert!(check_variant_collisions(&e).is_ok());
    }

    #[test]
    fn test_postgres_emits_pg_has_array_type_impl() {
        let e = make_enum("status", vec!["a", "b"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(
            code.contains("impl sqlx::postgres::PgHasArrayType for Status"),
            "must impl PgHasArrayType so Vec<Status> works, got:\n{}",
            code
        );
        assert!(
            code.contains("\"_status\""),
            "array type name must be '_<enum_name>', got:\n{}",
            code
        );
    }

    #[test]
    fn test_mysql_does_not_emit_pg_has_array_type_impl() {
        let e = make_enum("status", vec!["a", "b"]);
        let code = gen(&e, DatabaseKind::Mysql);
        assert!(!code.contains("PgHasArrayType"));
    }

    #[test]
    fn test_sqlite_does_not_emit_pg_has_array_type_impl() {
        let e = make_enum("status", vec!["a", "b"]);
        let code = gen(&e, DatabaseKind::Sqlite);
        assert!(!code.contains("PgHasArrayType"));
    }

    #[test]
    fn test_postgres_non_public_schema_type_name_is_unqualified() {
        // Regression: previously emitted "auth.role" which crashes sqlx 0.8 at runtime
        // (PgTypeInfo::with_name does not accept schema-qualified names).
        let e = EnumInfo {
            schema_name: "auth".to_string(),
            name: "role".to_string(),
            variants: vec!["admin".to_string(), "user".to_string()],
            default_variant: None,
        };
        let (tokens, _) = generate_enum(&e, DatabaseKind::Postgres, &[]);
        let code = parse_and_format(&tokens).unwrap();
        assert!(
            code.contains("sqlx(type_name = \"role\")"),
            "type_name must be unqualified for sqlx 0.8 compatibility, got:\n{}",
            code
        );
        assert!(
            !code.contains("\"auth.role\""),
            "type_name must NOT include schema; got:\n{}",
            code
        );
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
    fn test_mysql_inline_enum_emits_rename_for_lowercase_variants() {
        // Inline MySQL ENUM('active', 'inactive') → Rust variants are PascalCase
        // and need #[sqlx(rename)] so encode/decode hits the SQL text values.
        let e = make_enum("status", vec!["active", "inactive"]);
        let code = gen(&e, DatabaseKind::Mysql);
        assert!(
            code.contains("sqlx(rename = \"active\")"),
            "MySQL inline ENUM variant must carry rename for round-trip:\n{}",
            code
        );
        assert!(code.contains("sqlx(rename = \"inactive\")"));
        // type_name does NOT exist for MySQL — only PG-native enums need it.
        assert!(!code.contains("type_name"));
    }

    #[test]
    fn test_mysql_inline_enum_preserves_case_sensitive_variants() {
        let e = make_enum("priority", vec!["LOW", "HIGH"]);
        let code = gen(&e, DatabaseKind::Mysql);
        // PascalCase("LOW") = "Low" → rename required so SQL sees "LOW"
        assert!(code.contains("sqlx(rename = \"LOW\")"));
        assert!(code.contains("sqlx(rename = \"HIGH\")"));
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

    // --- impl Default ---

    #[test]
    fn test_default_impl_generated() {
        let e = EnumInfo {
            schema_name: "public".to_string(),
            name: "task_status".to_string(),
            variants: vec![
                "idle".to_string(),
                "running".to_string(),
                "done".to_string(),
            ],
            default_variant: Some("idle".to_string()),
        };
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("impl Default for TaskStatus"));
        assert!(code.contains("Self::Idle"));
    }

    #[test]
    fn test_no_default_impl_when_none() {
        let e = make_enum("status", vec!["active", "inactive"]);
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(!code.contains("impl Default"));
    }

    #[test]
    fn test_default_impl_snake_case_variant() {
        let e = EnumInfo {
            schema_name: "public".to_string(),
            name: "status".to_string(),
            variants: vec!["in_progress".to_string(), "done".to_string()],
            default_variant: Some("in_progress".to_string()),
        };
        let code = gen(&e, DatabaseKind::Postgres);
        assert!(code.contains("impl Default for Status"));
        assert!(code.contains("Self::InProgress"));
    }

    // --- public vs named schema integration ---

    fn make_enum_in_schema(schema: &str, name: &str, variants: Vec<&str>) -> EnumInfo {
        EnumInfo {
            schema_name: schema.to_string(),
            name: name.to_string(),
            variants: variants.into_iter().map(|s| s.to_string()).collect(),
            default_variant: None,
        }
    }

    #[test]
    fn test_public_schema_full_output() {
        let e = make_enum_in_schema(
            "public",
            "order_status",
            vec!["pending", "shipped", "delivered"],
        );
        let code = gen(&e, DatabaseKind::Postgres);

        assert!(code.contains("Enum: public.order_status"));
        assert!(code.contains("pub enum OrderStatus"));
        assert!(code.contains("sqlx(type_name = \"order_status\")"));
        assert!(!code.contains("sqlx(type_name = \"public.order_status\")"));
        assert!(code
            .contains("sqlx_gen(kind = \"enum\", schema = \"public\", name = \"order_status\")"));
        assert!(code.contains("Pending"));
        assert!(code.contains("Shipped"));
        assert!(code.contains("Delivered"));
    }

    #[test]
    fn test_named_schema_full_output() {
        let e = make_enum_in_schema(
            "analysis",
            "toolcall_status",
            vec!["PENDING", "RUNNING", "DONE"],
        );
        let code = gen(&e, DatabaseKind::Postgres);

        assert!(code.contains("Enum: analysis.toolcall_status"));
        assert!(code.contains("pub enum ToolcallStatus"));
        assert!(code.contains("sqlx(type_name = \"toolcall_status\")"));
        assert!(!code.contains("\"analysis.toolcall_status\""));
        assert!(code.contains(
            "sqlx_gen(kind = \"enum\", schema = \"analysis\", name = \"toolcall_status\")"
        ));
        assert!(code.contains("Pending"));
        assert!(code.contains("Running"));
        assert!(code.contains("Done"));
    }

    #[test]
    fn test_named_schema_with_default_variant() {
        let e = EnumInfo {
            schema_name: "billing".to_string(),
            name: "payment_status".to_string(),
            variants: vec![
                "pending".to_string(),
                "paid".to_string(),
                "refunded".to_string(),
            ],
            default_variant: Some("pending".to_string()),
        };
        let code = gen(&e, DatabaseKind::Postgres);

        assert!(code.contains("sqlx(type_name = \"payment_status\")"));
        assert!(!code.contains("\"billing.payment_status\""));
        assert!(code.contains("impl Default for PaymentStatus"));
        assert!(code.contains("Self::Pending"));
    }

    #[test]
    fn test_named_schema_variant_rename() {
        let e = make_enum_in_schema("audit", "log_level", vec!["info", "warn_high", "CRITICAL"]);
        let code = gen(&e, DatabaseKind::Postgres);

        assert!(code.contains("sqlx(type_name = \"log_level\")"));
        assert!(!code.contains("\"audit.log_level\""));
        assert!(code.contains("sqlx(rename = \"info\")"));
        assert!(code.contains("sqlx(rename = \"warn_high\")"));
        assert!(code.contains("WarnHigh"));
        assert!(code.contains("sqlx(rename = \"CRITICAL\")"));
        assert!(code.contains("Critical"));
    }

    #[test]
    fn test_named_schema_mysql_no_type_name() {
        let e = make_enum_in_schema("analytics", "event_type", vec!["click", "view"]);
        let code = gen(&e, DatabaseKind::Mysql);

        assert!(!code.contains("type_name"));
    }

    #[test]
    fn test_named_schema_sqlite_no_type_name() {
        let e = make_enum_in_schema("analytics", "event_type", vec!["click", "view"]);
        let code = gen(&e, DatabaseKind::Sqlite);

        assert!(!code.contains("type_name"));
    }
}
