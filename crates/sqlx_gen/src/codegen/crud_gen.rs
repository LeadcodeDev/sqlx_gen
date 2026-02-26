use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::{DatabaseKind, Methods};
use crate::codegen::entity_parser::{ParsedEntity, ParsedField};

pub fn generate_crud_from_parsed(
    entity: &ParsedEntity,
    db_kind: DatabaseKind,
    entity_module_path: &str,
    methods: &Methods,
    query_macro: bool,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();

    let entity_ident = format_ident!("{}", entity.struct_name);
    let repo_name = format!("{}Repository", entity.struct_name);
    let repo_ident = format_ident!("{}", repo_name);

    let table_name = &entity.table_name;

    // Pool type (used via full path sqlx::PgPool etc., no import needed)
    let pool_type = pool_type_tokens(db_kind);

    // Entity import
    imports.insert(format!("use {}::{};", entity_module_path, entity.struct_name));

    // Forward type imports from the entity file (chrono, uuid, etc.)
    // Rewrite `use super::X` imports to absolute paths based on entity_module_path,
    // since the repository lives in a different module where `super` has a different meaning.
    let entity_parent = entity_module_path
        .rsplit_once("::")
        .map(|(parent, _)| parent)
        .unwrap_or(entity_module_path);
    for imp in &entity.imports {
        if let Some(rest) = imp.strip_prefix("use super::") {
            imports.insert(format!("use {}::{}", entity_parent, rest));
        } else {
            imports.insert(imp.clone());
        }
    }

    // Primary key fields
    let pk_fields: Vec<&ParsedField> = entity.fields.iter().filter(|f| f.is_primary_key).collect();

    // Non-PK fields (for insert)
    let non_pk_fields: Vec<&ParsedField> = entity.fields.iter().filter(|f| !f.is_primary_key).collect();

    let is_view = entity.is_view;

    // Build method tokens
    let mut method_tokens = Vec::new();
    let mut param_structs = Vec::new();

    // --- get_all ---
    if methods.get_all {
        let sql = format!("SELECT * FROM {}", table_name);
        let method = if query_macro {
            quote! {
                pub async fn get_all(&self) -> Result<Vec<#entity_ident>, sqlx::Error> {
                    sqlx::query_as!(#entity_ident, #sql)
                        .fetch_all(&self.pool)
                        .await
                }
            }
        } else {
            quote! {
                pub async fn get_all(&self) -> Result<Vec<#entity_ident>, sqlx::Error> {
                    sqlx::query_as::<_, #entity_ident>(#sql)
                        .fetch_all(&self.pool)
                        .await
                }
            }
        };
        method_tokens.push(method);
    }

    // --- paginate ---
    if methods.paginate {
        let paginate_params_ident = format_ident!("Paginate{}Params", entity.struct_name);
        let paginated_ident = format_ident!("Paginated{}", entity.struct_name);
        let pagination_meta_ident = format_ident!("Pagination{}Meta", entity.struct_name);
        let count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
        let sql = match db_kind {
            DatabaseKind::Postgres => format!("SELECT * FROM {} LIMIT $1 OFFSET $2", table_name),
            DatabaseKind::Mysql | DatabaseKind::Sqlite => format!("SELECT * FROM {} LIMIT ? OFFSET ?", table_name),
        };
        let method = if query_macro {
            quote! {
                pub async fn paginate(&self, params: &#paginate_params_ident) -> Result<#paginated_ident, sqlx::Error> {
                    let total: i64 = sqlx::query_scalar!(#count_sql)
                        .fetch_one(&self.pool)
                        .await?
                        .unwrap_or(0);
                    let per_page = params.per_page;
                    let current_page = params.page;
                    let last_page = (total + per_page - 1) / per_page;
                    let offset = (current_page - 1) * per_page;
                    let data = sqlx::query_as!(#entity_ident, #sql, per_page, offset)
                        .fetch_all(&self.pool)
                        .await?;
                    Ok(#paginated_ident {
                        meta: #pagination_meta_ident {
                            total,
                            per_page,
                            current_page,
                            last_page,
                            first_page: 1,
                        },
                        data,
                    })
                }
            }
        } else {
            quote! {
                pub async fn paginate(&self, params: &#paginate_params_ident) -> Result<#paginated_ident, sqlx::Error> {
                    let total: i64 = sqlx::query_scalar(#count_sql)
                        .fetch_one(&self.pool)
                        .await?;
                    let per_page = params.per_page;
                    let current_page = params.page;
                    let last_page = (total + per_page - 1) / per_page;
                    let offset = (current_page - 1) * per_page;
                    let data = sqlx::query_as::<_, #entity_ident>(#sql)
                        .bind(per_page)
                        .bind(offset)
                        .fetch_all(&self.pool)
                        .await?;
                    Ok(#paginated_ident {
                        meta: #pagination_meta_ident {
                            total,
                            per_page,
                            current_page,
                            last_page,
                            first_page: 1,
                        },
                        data,
                    })
                }
            }
        };
        method_tokens.push(method);
        param_structs.push(quote! {
            #[derive(Debug, Clone, Default)]
            pub struct #paginate_params_ident {
                pub page: i64,
                pub per_page: i64,
            }
        });
        param_structs.push(quote! {
            #[derive(Debug, Clone)]
            pub struct #pagination_meta_ident {
                pub total: i64,
                pub per_page: i64,
                pub current_page: i64,
                pub last_page: i64,
                pub first_page: i64,
            }
        });
        param_structs.push(quote! {
            #[derive(Debug, Clone)]
            pub struct #paginated_ident {
                pub meta: #pagination_meta_ident,
                pub data: Vec<#entity_ident>,
            }
        });
    }

    // --- get (by PK) ---
    if methods.get && !pk_fields.is_empty() {
        let pk_params: Vec<TokenStream> = pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                let ty: TokenStream = f.inner_type.parse().unwrap();
                quote! { #name: &#ty }
            })
            .collect();

        let where_clause = build_where_clause_parsed(&pk_fields, db_kind, 1);
        let where_clause_cast = build_where_clause_cast(&pk_fields, db_kind, 1);
        let sql = format!("SELECT * FROM {} WHERE {}", table_name, where_clause);
        let sql_macro = format!("SELECT * FROM {} WHERE {}", table_name, where_clause_cast);

        let binds: Vec<TokenStream> = pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { .bind(#name) }
            })
            .collect();

        let method = if query_macro {
            let pk_arg_names: Vec<TokenStream> = pk_fields
                .iter()
                .map(|f| {
                    let name = format_ident!("{}", f.rust_name);
                    quote! { #name }
                })
                .collect();
            quote! {
                pub async fn get(&self, #(#pk_params),*) -> Result<Option<#entity_ident>, sqlx::Error> {
                    sqlx::query_as!(#entity_ident, #sql_macro, #(#pk_arg_names),*)
                        .fetch_optional(&self.pool)
                        .await
                }
            }
        } else {
            quote! {
                pub async fn get(&self, #(#pk_params),*) -> Result<Option<#entity_ident>, sqlx::Error> {
                    sqlx::query_as::<_, #entity_ident>(#sql)
                        #(#binds)*
                        .fetch_optional(&self.pool)
                        .await
                }
            }
        };
        method_tokens.push(method);
    }

    // --- insert (skip for views) ---
    if !is_view && methods.insert && !non_pk_fields.is_empty() {
        let insert_params_ident = format_ident!("Insert{}Params", entity.struct_name);

        let insert_fields: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                let ty: TokenStream = f.rust_type.parse().unwrap();
                quote! { pub #name: #ty, }
            })
            .collect();

        let col_names: Vec<&str> = non_pk_fields.iter().map(|f| f.column_name.as_str()).collect();
        let col_list = col_names.join(", ");
        // Use casted placeholders for macro mode, plain for runtime
        let placeholders = build_placeholders(non_pk_fields.len(), db_kind, 1);
        let placeholders_cast = build_placeholders_with_cast(&non_pk_fields, db_kind, 1, true);

        let build_insert_sql = |ph: &str| match db_kind {
            DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                format!(
                    "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
                    table_name, col_list, ph
                )
            }
            DatabaseKind::Mysql => {
                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table_name, col_list, ph
                )
            }
        };
        let sql = build_insert_sql(&placeholders);
        let sql_macro = build_insert_sql(&placeholders_cast);

        let binds: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { .bind(&params.#name) }
            })
            .collect();

        let insert_method = build_insert_method_parsed(
            &entity_ident,
            &insert_params_ident,
            &sql,
            &sql_macro,
            &binds,
            db_kind,
            table_name,
            &pk_fields,
            &non_pk_fields,
            query_macro,
        );
        method_tokens.push(insert_method);

        param_structs.push(quote! {
            #[derive(Debug, Clone, Default)]
            pub struct #insert_params_ident {
                #(#insert_fields)*
            }
        });
    }

    // --- update (skip for views) ---
    if !is_view && methods.update && !pk_fields.is_empty() {
        let update_params_ident = format_ident!("Update{}Params", entity.struct_name);

        let update_fields: Vec<TokenStream> = entity
            .fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                let ty: TokenStream = f.rust_type.parse().unwrap();
                quote! { pub #name: #ty, }
            })
            .collect();

        let set_cols: Vec<String> = non_pk_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let p = placeholder(db_kind, i + 1);
                format!("{} = {}", f.column_name, p)
            })
            .collect();
        let set_clause = set_cols.join(", ");

        // SET clause with casts for macro mode
        let set_cols_cast: Vec<String> = non_pk_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let p = placeholder_with_cast(db_kind, i + 1, f);
                format!("{} = {}", f.column_name, p)
            })
            .collect();
        let set_clause_cast = set_cols_cast.join(", ");

        let pk_start = non_pk_fields.len() + 1;
        let where_clause = build_where_clause_parsed(&pk_fields, db_kind, pk_start);
        let where_clause_cast = build_where_clause_cast(&pk_fields, db_kind, pk_start);

        let build_update_sql = |sc: &str, wc: &str| match db_kind {
            DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                format!(
                    "UPDATE {} SET {} WHERE {} RETURNING *",
                    table_name, sc, wc
                )
            }
            DatabaseKind::Mysql => {
                format!(
                    "UPDATE {} SET {} WHERE {}",
                    table_name, sc, wc
                )
            }
        };
        let sql = build_update_sql(&set_clause, &where_clause);
        let sql_macro = build_update_sql(&set_clause_cast, &where_clause_cast);

        // Bind non-PK first, then PK
        let mut all_binds: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { .bind(&params.#name) }
            })
            .collect();
        for f in &pk_fields {
            let name = format_ident!("{}", f.rust_name);
            all_binds.push(quote! { .bind(&params.#name) });
        }

        // Macro args: non-PK fields first, then PK fields
        let update_macro_args: Vec<TokenStream> = non_pk_fields
            .iter()
            .chain(pk_fields.iter())
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { params.#name }
            })
            .collect();

        let update_method = if query_macro {
            match db_kind {
                DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                    quote! {
                        pub async fn update(&self, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
                            sqlx::query_as!(#entity_ident, #sql_macro, #(#update_macro_args),*)
                                .fetch_one(&self.pool)
                                .await
                        }
                    }
                }
                DatabaseKind::Mysql => {
                    let pk_where_select = build_where_clause_parsed(&pk_fields, db_kind, 1);
                    let select_sql = format!("SELECT * FROM {} WHERE {}", table_name, pk_where_select);
                    let pk_macro_args: Vec<TokenStream> = pk_fields
                        .iter()
                        .map(|f| {
                            let name = format_ident!("{}", f.rust_name);
                            quote! { params.#name }
                        })
                        .collect();
                    quote! {
                        pub async fn update(&self, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
                            sqlx::query!(#sql_macro, #(#update_macro_args),*)
                                .execute(&self.pool)
                                .await?;
                            sqlx::query_as!(#entity_ident, #select_sql, #(#pk_macro_args),*)
                                .fetch_one(&self.pool)
                                .await
                        }
                    }
                }
            }
        } else {
            match db_kind {
                DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                    quote! {
                        pub async fn update(&self, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
                            sqlx::query_as::<_, #entity_ident>(#sql)
                                #(#all_binds)*
                                .fetch_one(&self.pool)
                                .await
                        }
                    }
                }
                DatabaseKind::Mysql => {
                    let pk_where_select = build_where_clause_parsed(&pk_fields, db_kind, 1);
                    let select_sql = format!("SELECT * FROM {} WHERE {}", table_name, pk_where_select);
                    let pk_binds: Vec<TokenStream> = pk_fields
                        .iter()
                        .map(|f| {
                            let name = format_ident!("{}", f.rust_name);
                            quote! { .bind(&params.#name) }
                        })
                        .collect();
                    quote! {
                        pub async fn update(&self, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
                            sqlx::query(#sql)
                                #(#all_binds)*
                                .execute(&self.pool)
                                .await?;
                            sqlx::query_as::<_, #entity_ident>(#select_sql)
                                #(#pk_binds)*
                                .fetch_one(&self.pool)
                                .await
                        }
                    }
                }
            }
        };
        method_tokens.push(update_method);

        param_structs.push(quote! {
            #[derive(Debug, Clone, Default)]
            pub struct #update_params_ident {
                #(#update_fields)*
            }
        });
    }

    // --- delete (skip for views) ---
    if !is_view && methods.delete && !pk_fields.is_empty() {
        let pk_params: Vec<TokenStream> = pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                let ty: TokenStream = f.inner_type.parse().unwrap();
                quote! { #name: &#ty }
            })
            .collect();

        let where_clause = build_where_clause_parsed(&pk_fields, db_kind, 1);
        let where_clause_cast = build_where_clause_cast(&pk_fields, db_kind, 1);
        let sql = format!("DELETE FROM {} WHERE {}", table_name, where_clause);
        let sql_macro = format!("DELETE FROM {} WHERE {}", table_name, where_clause_cast);

        let binds: Vec<TokenStream> = pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { .bind(#name) }
            })
            .collect();

        let method = if query_macro {
            let pk_arg_names: Vec<TokenStream> = pk_fields
                .iter()
                .map(|f| {
                    let name = format_ident!("{}", f.rust_name);
                    quote! { #name }
                })
                .collect();
            quote! {
                pub async fn delete(&self, #(#pk_params),*) -> Result<(), sqlx::Error> {
                    sqlx::query!(#sql_macro, #(#pk_arg_names),*)
                        .execute(&self.pool)
                        .await?;
                    Ok(())
                }
            }
        } else {
            quote! {
                pub async fn delete(&self, #(#pk_params),*) -> Result<(), sqlx::Error> {
                    sqlx::query(#sql)
                        #(#binds)*
                        .execute(&self.pool)
                        .await?;
                    Ok(())
                }
            }
        };
        method_tokens.push(method);
    }

    let tokens = quote! {
        #(#param_structs)*

        pub struct #repo_ident {
            pool: #pool_type,
        }

        impl #repo_ident {
            pub fn new(pool: #pool_type) -> Self {
                Self { pool }
            }

            #(#method_tokens)*
        }
    };

    (tokens, imports)
}

fn pool_type_tokens(db_kind: DatabaseKind) -> TokenStream {
    match db_kind {
        DatabaseKind::Postgres => quote! { sqlx::PgPool },
        DatabaseKind::Mysql => quote! { sqlx::MySqlPool },
        DatabaseKind::Sqlite => quote! { sqlx::SqlitePool },
    }
}

fn placeholder(db_kind: DatabaseKind, index: usize) -> String {
    match db_kind {
        DatabaseKind::Postgres => format!("${}", index),
        DatabaseKind::Mysql | DatabaseKind::Sqlite => "?".to_string(),
    }
}

fn placeholder_with_cast(db_kind: DatabaseKind, index: usize, field: &ParsedField) -> String {
    let base = placeholder(db_kind, index);
    match (&field.sql_type, field.is_sql_array) {
        (Some(t), true) => format!("{} as {}[]", base, t),
        (Some(t), false) => format!("{} as {}", base, t),
        (None, _) => base,
    }
}

fn build_placeholders(count: usize, db_kind: DatabaseKind, start: usize) -> String {
    (0..count)
        .map(|i| placeholder(db_kind, start + i))
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_placeholders_with_cast(fields: &[&ParsedField], db_kind: DatabaseKind, start: usize, use_cast: bool) -> String {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if use_cast {
                placeholder_with_cast(db_kind, start + i, f)
            } else {
                placeholder(db_kind, start + i)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_where_clause_parsed(
    pk_fields: &[&ParsedField],
    db_kind: DatabaseKind,
    start_index: usize,
) -> String {
    pk_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let p = placeholder(db_kind, start_index + i);
            format!("{} = {}", f.column_name, p)
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn build_where_clause_cast(
    pk_fields: &[&ParsedField],
    db_kind: DatabaseKind,
    start_index: usize,
) -> String {
    pk_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let p = placeholder_with_cast(db_kind, start_index + i, f);
            format!("{} = {}", f.column_name, p)
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[allow(clippy::too_many_arguments)]
fn build_insert_method_parsed(
    entity_ident: &proc_macro2::Ident,
    insert_params_ident: &proc_macro2::Ident,
    sql: &str,
    sql_macro: &str,
    binds: &[TokenStream],
    db_kind: DatabaseKind,
    table_name: &str,
    pk_fields: &[&ParsedField],
    non_pk_fields: &[&ParsedField],
    query_macro: bool,
) -> TokenStream {
    if query_macro {
        let macro_args: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { params.#name }
            })
            .collect();

        match db_kind {
            DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                quote! {
                    pub async fn insert(&self, params: &#insert_params_ident) -> Result<#entity_ident, sqlx::Error> {
                        sqlx::query_as!(#entity_ident, #sql_macro, #(#macro_args),*)
                            .fetch_one(&self.pool)
                            .await
                    }
                }
            }
            DatabaseKind::Mysql => {
                let pk_where = build_where_clause_parsed(pk_fields, db_kind, 1);
                let select_sql = format!("SELECT * FROM {} WHERE {}", table_name, pk_where);
                quote! {
                    pub async fn insert(&self, params: &#insert_params_ident) -> Result<#entity_ident, sqlx::Error> {
                        sqlx::query!(#sql_macro, #(#macro_args),*)
                            .execute(&self.pool)
                            .await?;
                        let id = sqlx::query_scalar!("SELECT LAST_INSERT_ID() as id")
                            .fetch_one(&self.pool)
                            .await?;
                        sqlx::query_as!(#entity_ident, #select_sql, id)
                            .fetch_one(&self.pool)
                            .await
                    }
                }
            }
        }
    } else {
        match db_kind {
            DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                quote! {
                    pub async fn insert(&self, params: &#insert_params_ident) -> Result<#entity_ident, sqlx::Error> {
                        sqlx::query_as::<_, #entity_ident>(#sql)
                            #(#binds)*
                            .fetch_one(&self.pool)
                            .await
                    }
                }
            }
            DatabaseKind::Mysql => {
                let pk_where = build_where_clause_parsed(pk_fields, db_kind, 1);
                let select_sql = format!("SELECT * FROM {} WHERE {}", table_name, pk_where);
                quote! {
                    pub async fn insert(&self, params: &#insert_params_ident) -> Result<#entity_ident, sqlx::Error> {
                        sqlx::query(#sql)
                            #(#binds)*
                            .execute(&self.pool)
                            .await?;
                        let id = sqlx::query_scalar::<_, i64>("SELECT LAST_INSERT_ID()")
                            .fetch_one(&self.pool)
                            .await?;
                        sqlx::query_as::<_, #entity_ident>(#select_sql)
                            .bind(id)
                            .fetch_one(&self.pool)
                            .await
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codegen::parse_and_format;
    use crate::cli::Methods;

    fn make_field(rust_name: &str, column_name: &str, rust_type: &str, nullable: bool, is_pk: bool) -> ParsedField {
        let inner_type = if nullable {
            // Strip "Option<" prefix and ">" suffix
            rust_type
                .strip_prefix("Option<")
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or(rust_type)
                .to_string()
        } else {
            rust_type.to_string()
        };
        ParsedField {
            rust_name: rust_name.to_string(),
            column_name: column_name.to_string(),
            rust_type: rust_type.to_string(),
            is_nullable: nullable,
            inner_type,
            is_primary_key: is_pk,
            sql_type: None,
            is_sql_array: false,
        }
    }

    fn standard_entity() -> ParsedEntity {
        ParsedEntity {
            struct_name: "Users".to_string(),
            table_name: "users".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("id", "id", "i32", false, true),
                make_field("name", "name", "String", false, false),
                make_field("email", "email", "Option<String>", true, false),
            ],
            imports: vec![],
        }
    }

    fn gen(entity: &ParsedEntity, db: DatabaseKind) -> String {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::users", &skip, false);
        parse_and_format(&tokens)
    }

    fn gen_macro(entity: &ParsedEntity, db: DatabaseKind) -> String {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::users", &skip, true);
        parse_and_format(&tokens)
    }

    fn gen_with_methods(entity: &ParsedEntity, db: DatabaseKind, methods: &Methods) -> String {
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::users", methods, false);
        parse_and_format(&tokens)
    }

    // --- basic structure ---

    #[test]
    fn test_repo_struct_name() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct UsersRepository"));
    }

    #[test]
    fn test_repo_new_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub fn new("));
    }

    #[test]
    fn test_repo_pool_field_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pool: sqlx::PgPool") || code.contains("pool: sqlx :: PgPool"));
    }

    #[test]
    fn test_repo_pool_field_mysql() {
        let code = gen(&standard_entity(), DatabaseKind::Mysql);
        assert!(code.contains("MySqlPool") || code.contains("MySql"));
    }

    #[test]
    fn test_repo_pool_field_sqlite() {
        let code = gen(&standard_entity(), DatabaseKind::Sqlite);
        assert!(code.contains("SqlitePool") || code.contains("Sqlite"));
    }

    // --- get_all ---

    #[test]
    fn test_get_all_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn get_all"));
    }

    #[test]
    fn test_get_all_returns_vec() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("Vec<Users>"));
    }

    #[test]
    fn test_get_all_sql() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("SELECT * FROM users"));
    }

    // --- paginate ---

    #[test]
    fn test_paginate_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn paginate"));
    }

    #[test]
    fn test_paginate_params_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct PaginateUsersParams"));
    }

    #[test]
    fn test_paginate_params_fields() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub page: i64"));
        assert!(code.contains("pub per_page: i64"));
    }

    #[test]
    fn test_paginate_returns_paginated() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("PaginatedUsers"));
        assert!(code.contains("PaginationUsersMeta"));
    }

    #[test]
    fn test_paginate_meta_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct PaginationUsersMeta"));
        assert!(code.contains("pub total: i64"));
        assert!(code.contains("pub last_page: i64"));
        assert!(code.contains("pub first_page: i64"));
        assert!(code.contains("pub current_page: i64"));
    }

    #[test]
    fn test_paginate_data_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct PaginatedUsers"));
        assert!(code.contains("pub meta: PaginationUsersMeta"));
        assert!(code.contains("pub data: Vec<Users>"));
    }

    #[test]
    fn test_paginate_count_sql() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("SELECT COUNT(*) FROM users"));
    }

    #[test]
    fn test_paginate_sql_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("LIMIT $1 OFFSET $2"));
    }

    #[test]
    fn test_paginate_sql_mysql() {
        let code = gen(&standard_entity(), DatabaseKind::Mysql);
        assert!(code.contains("LIMIT ? OFFSET ?"));
    }

    // --- get ---

    #[test]
    fn test_get_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn get"));
    }

    #[test]
    fn test_get_returns_option() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("Option<Users>"));
    }

    #[test]
    fn test_get_where_pk_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("WHERE id = $1"));
    }

    #[test]
    fn test_get_where_pk_mysql() {
        let code = gen(&standard_entity(), DatabaseKind::Mysql);
        assert!(code.contains("WHERE id = ?"));
    }

    // --- insert ---

    #[test]
    fn test_insert_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn insert"));
    }

    #[test]
    fn test_insert_params_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct InsertUsersParams"));
    }

    #[test]
    fn test_insert_params_no_pk() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub name: String"));
        assert!(code.contains("pub email: Option<String>") || code.contains("pub email: Option < String >"));
    }

    #[test]
    fn test_insert_returning_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("RETURNING *"));
    }

    #[test]
    fn test_insert_returning_sqlite() {
        let code = gen(&standard_entity(), DatabaseKind::Sqlite);
        assert!(code.contains("RETURNING *"));
    }

    #[test]
    fn test_insert_mysql_last_insert_id() {
        let code = gen(&standard_entity(), DatabaseKind::Mysql);
        assert!(code.contains("LAST_INSERT_ID"));
    }

    // --- update ---

    #[test]
    fn test_update_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn update"));
    }

    #[test]
    fn test_update_params_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct UpdateUsersParams"));
    }

    #[test]
    fn test_update_params_all_cols() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub id: i32"));
        assert!(code.contains("pub name: String"));
    }

    #[test]
    fn test_update_set_clause_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("SET name = $1"));
        assert!(code.contains("WHERE id = $3"));
    }

    #[test]
    fn test_update_returning_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("UPDATE users SET"));
        assert!(code.contains("RETURNING *"));
    }

    // --- delete ---

    #[test]
    fn test_delete_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn delete"));
    }

    #[test]
    fn test_delete_where_pk() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("DELETE FROM users WHERE id = $1"));
    }

    #[test]
    fn test_delete_returns_unit() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("Result<(), sqlx::Error>") || code.contains("Result<(), sqlx :: Error>"));
    }

    // --- views (read-only) ---

    #[test]
    fn test_view_no_insert() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(!code.contains("pub async fn insert"));
    }

    #[test]
    fn test_view_no_update() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(!code.contains("pub async fn update"));
    }

    #[test]
    fn test_view_no_delete() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(!code.contains("pub async fn delete"));
    }

    #[test]
    fn test_view_has_get_all() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(code.contains("pub async fn get_all"));
    }

    #[test]
    fn test_view_has_paginate() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(code.contains("pub async fn paginate"));
    }

    #[test]
    fn test_view_has_get() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(code.contains("pub async fn get"));
    }

    // --- selective methods ---

    #[test]
    fn test_only_get_all() {
        let m = Methods { get_all: true, ..Default::default() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(code.contains("pub async fn get_all"));
        assert!(!code.contains("pub async fn paginate"));
        assert!(!code.contains("pub async fn insert"));
    }

    #[test]
    fn test_without_get_all() {
        let m = Methods { get_all: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn get_all"));
    }

    #[test]
    fn test_without_paginate() {
        let m = Methods { paginate: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn paginate"));
        assert!(!code.contains("PaginateUsersParams"));
    }

    #[test]
    fn test_without_get() {
        let m = Methods { get: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(code.contains("pub async fn get_all"));
        let without_get_all = code.replace("get_all", "XXX");
        assert!(!without_get_all.contains("fn get("));
    }

    #[test]
    fn test_without_insert() {
        let m = Methods { insert: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn insert"));
        assert!(!code.contains("InsertUsersParams"));
    }

    #[test]
    fn test_without_update() {
        let m = Methods { update: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn update"));
        assert!(!code.contains("UpdateUsersParams"));
    }

    #[test]
    fn test_without_delete() {
        let m = Methods { delete: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn delete"));
    }

    #[test]
    fn test_empty_methods_no_methods() {
        let m = Methods::default();
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn get_all"));
        assert!(!code.contains("pub async fn paginate"));
        assert!(!code.contains("pub async fn insert"));
        assert!(!code.contains("pub async fn update"));
        assert!(!code.contains("pub async fn delete"));
    }

    // --- imports ---

    #[test]
    fn test_no_pool_import() {
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false);
        assert!(!imports.iter().any(|i| i.contains("PgPool")));
    }

    #[test]
    fn test_imports_contain_entity() {
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false);
        assert!(imports.iter().any(|i| i.contains("crate::models::users::Users")));
    }

    // --- renamed columns ---

    #[test]
    fn test_renamed_column_in_sql() {
        let entity = ParsedEntity {
            struct_name: "Connector".to_string(),
            table_name: "connector".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("id", "id", "i32", false, true),
                make_field("connector_type", "type", "String", false, false),
            ],
            imports: vec![],
        };
        let code = gen(&entity, DatabaseKind::Postgres);
        // INSERT should use the DB column name "type", not "connector_type"
        assert!(code.contains("type"));
        // The Rust param field should be connector_type
        assert!(code.contains("pub connector_type: String"));
    }

    // --- no PK edge cases ---

    #[test]
    fn test_no_pk_no_get() {
        let entity = ParsedEntity {
            struct_name: "Logs".to_string(),
            table_name: "logs".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("message", "message", "String", false, false),
                make_field("ts", "ts", "String", false, false),
            ],
            imports: vec![],
        };
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(code.contains("pub async fn get_all"));
        let without_get_all = code.replace("get_all", "XXX");
        assert!(!without_get_all.contains("fn get("));
    }

    #[test]
    fn test_no_pk_no_delete() {
        let entity = ParsedEntity {
            struct_name: "Logs".to_string(),
            table_name: "logs".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("message", "message", "String", false, false),
            ],
            imports: vec![],
        };
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(!code.contains("pub async fn delete"));
    }

    // --- Default derive on param structs ---

    #[test]
    fn test_param_structs_have_default() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("Default"));
    }

    // --- entity imports forwarded ---

    #[test]
    fn test_entity_imports_forwarded() {
        let entity = ParsedEntity {
            struct_name: "Users".to_string(),
            table_name: "users".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("id", "id", "Uuid", false, true),
                make_field("created_at", "created_at", "DateTime<Utc>", false, false),
            ],
            imports: vec![
                "use chrono::{DateTime, Utc};".to_string(),
                "use uuid::Uuid;".to_string(),
            ],
        };
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&entity, DatabaseKind::Postgres, "crate::models::users", &skip, false);
        assert!(imports.iter().any(|i| i.contains("chrono")));
        assert!(imports.iter().any(|i| i.contains("uuid")));
    }

    #[test]
    fn test_entity_imports_empty_when_no_imports() {
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false);
        // Should only have pool + entity imports, no chrono/uuid
        assert!(!imports.iter().any(|i| i.contains("chrono")));
        assert!(!imports.iter().any(|i| i.contains("uuid")));
    }

    // --- query_macro mode ---

    #[test]
    fn test_macro_get_all() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("query_as!"));
        assert!(!code.contains("query_as::<"));
    }

    #[test]
    fn test_macro_paginate() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("query_as!"));
        assert!(code.contains("per_page, offset"));
    }

    #[test]
    fn test_macro_get() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        // The get method should use query_as! with the PK as arg
        assert!(code.contains("query_as!(Users"));
    }

    #[test]
    fn test_macro_insert_pg() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("query_as!(Users"));
        assert!(code.contains("params.name"));
        assert!(code.contains("params.email"));
    }

    #[test]
    fn test_macro_insert_mysql() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Mysql);
        // MySQL insert uses query! (not query_as!) for the INSERT
        assert!(code.contains("query!"));
        assert!(code.contains("query_scalar!"));
    }

    #[test]
    fn test_macro_update() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("query_as!(Users"));
        // Should contain params.name, params.email, params.id as args
        assert!(code.contains("params.name"));
        assert!(code.contains("params.id"));
    }

    #[test]
    fn test_macro_delete() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        // delete uses query! (no return type)
        assert!(code.contains("query!"));
    }

    #[test]
    fn test_macro_no_bind_calls() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        assert!(!code.contains(".bind("));
    }

    #[test]
    fn test_function_style_uses_bind() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains(".bind("));
        assert!(!code.contains("query_as!("));
        assert!(!code.contains("query!("));
    }
}
