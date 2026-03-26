use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::cli::{DatabaseKind, Methods, PoolVisibility};
use crate::codegen::entity_parser::{ParsedEntity, ParsedField};

pub fn generate_crud_from_parsed(
    entity: &ParsedEntity,
    db_kind: DatabaseKind,
    entity_module_path: &str,
    methods: &Methods,
    query_macro: bool,
    pool_visibility: PoolVisibility,
) -> (TokenStream, BTreeSet<String>) {
    let mut imports = BTreeSet::new();

    let entity_ident = format_ident!("{}", entity.struct_name);
    let repo_name = format!("{}Repository", entity.struct_name);
    let repo_ident = format_ident!("{}", repo_name);

    let table_name = match &entity.schema_name {
        Some(schema) => format!("{}.{}", schema, entity.table_name),
        None => entity.table_name.clone(),
    };

    // Pool type (used via full path sqlx::PgPool etc., no import needed)
    let pool_type = pool_type_tokens(db_kind);

    // When the entity has custom SQL types (enums, composites, arrays),
    // query_as! macro can't resolve the column type at compile time. Fall back to runtime query_as::<_, T>()
    // for queries that return rows. DELETE (no rows returned) can still use macro.
    let has_custom_sql_type = entity.fields.iter().any(|f| f.sql_type.is_some());
    let use_macro = query_macro && !has_custom_sql_type && !entity.is_view;

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
        let method = if use_macro {
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
        let method = if use_macro {
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
                quote! { #name: #ty }
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

        let method = if use_macro {
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
    if !is_view && methods.insert && (!non_pk_fields.is_empty() || !pk_fields.is_empty()) {
        let insert_params_ident = format_ident!("Insert{}Params", entity.struct_name);

        // When all columns are PKs (e.g. junction tables), use pk_fields for insert
        let insert_source_fields: Vec<&ParsedField> = if non_pk_fields.is_empty() {
            pk_fields.clone()
        } else {
            non_pk_fields.clone()
        };

        // Fields with column_default and not already nullable → Option<T>
        let insert_fields: Vec<TokenStream> = insert_source_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                if f.column_default.is_some() && !f.is_nullable {
                    let ty: TokenStream = format!("Option<{}>", f.rust_type).parse().unwrap();
                    quote! { pub #name: #ty, }
                } else {
                    let ty: TokenStream = f.rust_type.parse().unwrap();
                    quote! { pub #name: #ty, }
                }
            })
            .collect();

        let col_names: Vec<&str> = insert_source_fields.iter().map(|f| f.column_name.as_str()).collect();
        let col_list = col_names.join(", ");

        // Build placeholders with COALESCE for fields that have a column_default
        let placeholders: String = insert_source_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let p = placeholder(db_kind, i + 1);
                match &f.column_default {
                    Some(default_expr) => format!("COALESCE({}, {})", p, default_expr),
                    None => p,
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let placeholders_cast: String = insert_source_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let p = placeholder_with_cast(db_kind, i + 1, f);
                match &f.column_default {
                    Some(default_expr) => format!("COALESCE({}, {})", p, default_expr),
                    None => p,
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

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

        let binds: Vec<TokenStream> = insert_source_fields
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
            &table_name,
            &pk_fields,
            &insert_source_fields,
            use_macro,
        );
        method_tokens.push(insert_method);

        param_structs.push(quote! {
            #[derive(Debug, Clone, Default)]
            pub struct #insert_params_ident {
                #(#insert_fields)*
            }
        });
    }

    // --- insert_many_transactionally (skip for views) ---
    if !is_view && methods.insert_many && (!non_pk_fields.is_empty() || !pk_fields.is_empty()) {
        let insert_params_ident = format_ident!("Insert{}Params", entity.struct_name);

        let insert_source_fields: Vec<&ParsedField> = if non_pk_fields.is_empty() {
            pk_fields.clone()
        } else {
            non_pk_fields.clone()
        };

        let col_names: Vec<&str> = insert_source_fields.iter().map(|f| f.column_name.as_str()).collect();
        let col_list = col_names.join(", ");
        let num_cols = insert_source_fields.len();

        let binds_loop: Vec<TokenStream> = insert_source_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { query = query.bind(&params.#name); }
            })
            .collect();

        let insert_many_method = build_insert_many_transactionally_method(
            &entity_ident,
            &insert_params_ident,
            &col_list,
            num_cols,
            &insert_source_fields,
            &binds_loop,
            db_kind,
            &table_name,
            &pk_fields,
        );
        method_tokens.push(insert_many_method);

        // Only generate InsertParams if we haven't generated it from the insert method
        if !methods.insert {
            let insert_fields: Vec<TokenStream> = insert_source_fields
                .iter()
                .map(|f| {
                    let name = format_ident!("{}", f.rust_name);
                    if f.column_default.is_some() && !f.is_nullable {
                        let ty: TokenStream = format!("Option<{}>", f.rust_type).parse().unwrap();
                        quote! { pub #name: #ty, }
                    } else {
                        let ty: TokenStream = f.rust_type.parse().unwrap();
                        quote! { pub #name: #ty, }
                    }
                })
                .collect();

            param_structs.push(quote! {
                #[derive(Debug, Clone, Default)]
                pub struct #insert_params_ident {
                    #(#insert_fields)*
                }
            });
        }
    }

    // --- overwrite (full replacement — skip for views, skip when all columns are PKs) ---
    if !is_view && methods.overwrite && !pk_fields.is_empty() && !non_pk_fields.is_empty() {
        let overwrite_params_ident = format_ident!("Overwrite{}Params", entity.struct_name);

        // PK as function parameters (like get/delete)
        let pk_fn_params: Vec<TokenStream> = pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                let ty: TokenStream = f.inner_type.parse().unwrap();
                quote! { #name: #ty }
            })
            .collect();

        // Non-PK fields keep original types (required)
        let overwrite_fields: Vec<TokenStream> = non_pk_fields
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

        let build_overwrite_sql = |sc: &str, wc: &str| match db_kind {
            DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                format!("UPDATE {} SET {} WHERE {} RETURNING *", table_name, sc, wc)
            }
            DatabaseKind::Mysql => {
                format!("UPDATE {} SET {} WHERE {}", table_name, sc, wc)
            }
        };
        let sql = build_overwrite_sql(&set_clause, &where_clause);
        let sql_macro = build_overwrite_sql(&set_clause_cast, &where_clause_cast);

        // Bind non-PK first (from params), then PK (from function args)
        let mut all_binds: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { .bind(&params.#name) }
            })
            .collect();
        for f in &pk_fields {
            let name = format_ident!("{}", f.rust_name);
            all_binds.push(quote! { .bind(#name) });
        }

        // Macro args: non-PK from params, then PK from function args
        let overwrite_macro_args: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| macro_arg_for_field(f))
            .chain(pk_fields.iter().map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { #name }
            }))
            .collect();

        let overwrite_method = if use_macro {
            match db_kind {
                DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                    quote! {
                        pub async fn overwrite(&self, #(#pk_fn_params),*, params: &#overwrite_params_ident) -> Result<#entity_ident, sqlx::Error> {
                            sqlx::query_as!(#entity_ident, #sql_macro, #(#overwrite_macro_args),*)
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
                            quote! { #name }
                        })
                        .collect();
                    quote! {
                        pub async fn overwrite(&self, #(#pk_fn_params),*, params: &#overwrite_params_ident) -> Result<#entity_ident, sqlx::Error> {
                            sqlx::query!(#sql_macro, #(#overwrite_macro_args),*)
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
                        pub async fn overwrite(&self, #(#pk_fn_params),*, params: &#overwrite_params_ident) -> Result<#entity_ident, sqlx::Error> {
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
                            quote! { .bind(#name) }
                        })
                        .collect();
                    quote! {
                        pub async fn overwrite(&self, #(#pk_fn_params),*, params: &#overwrite_params_ident) -> Result<#entity_ident, sqlx::Error> {
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
        method_tokens.push(overwrite_method);

        param_structs.push(quote! {
            #[derive(Debug, Clone, Default)]
            pub struct #overwrite_params_ident {
                #(#overwrite_fields)*
            }
        });
    }

    // --- update / patch (COALESCE — skip for views, skip when all columns are PKs) ---
    if !is_view && methods.update && !pk_fields.is_empty() && !non_pk_fields.is_empty() {
        let update_params_ident = format_ident!("Update{}Params", entity.struct_name);

        // PK as function parameters (like get/delete)
        let pk_fn_params: Vec<TokenStream> = pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                let ty: TokenStream = f.inner_type.parse().unwrap();
                quote! { #name: #ty }
            })
            .collect();

        // Non-PK fields become Option<T> (no double Option for already nullable)
        let update_fields: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                if f.is_nullable {
                    // Already Option<T> — keep as-is to avoid Option<Option<T>>
                    let ty: TokenStream = f.rust_type.parse().unwrap();
                    quote! { pub #name: #ty, }
                } else {
                    let ty: TokenStream = format!("Option<{}>", f.rust_type).parse().unwrap();
                    quote! { pub #name: #ty, }
                }
            })
            .collect();

        // SET clause with COALESCE for runtime mode
        let set_cols: Vec<String> = non_pk_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let p = placeholder(db_kind, i + 1);
                format!("{col} = COALESCE({p}, {col})", col = f.column_name, p = p)
            })
            .collect();
        let set_clause = set_cols.join(", ");

        // SET clause with COALESCE and casts for macro mode
        let set_cols_cast: Vec<String> = non_pk_fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let p = placeholder_with_cast(db_kind, i + 1, f);
                format!("{col} = COALESCE({p}, {col})", col = f.column_name, p = p)
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

        // Bind non-PK first (from params), then PK (from function args)
        let mut all_binds: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { .bind(&params.#name) }
            })
            .collect();
        for f in &pk_fields {
            let name = format_ident!("{}", f.rust_name);
            all_binds.push(quote! { .bind(#name) });
        }

        // Macro args: non-PK from params, then PK from function args
        let update_macro_args: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| macro_arg_for_field(f))
            .chain(pk_fields.iter().map(|f| {
                let name = format_ident!("{}", f.rust_name);
                quote! { #name }
            }))
            .collect();

        let update_method = if use_macro {
            match db_kind {
                DatabaseKind::Postgres | DatabaseKind::Sqlite => {
                    quote! {
                        pub async fn update(&self, #(#pk_fn_params),*, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
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
                            quote! { #name }
                        })
                        .collect();
                    quote! {
                        pub async fn update(&self, #(#pk_fn_params),*, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
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
                        pub async fn update(&self, #(#pk_fn_params),*, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
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
                            quote! { .bind(#name) }
                        })
                        .collect();
                    quote! {
                        pub async fn update(&self, #(#pk_fn_params),*, params: &#update_params_ident) -> Result<#entity_ident, sqlx::Error> {
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
                quote! { #name: #ty }
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

    let pool_vis: TokenStream = match pool_visibility {
        PoolVisibility::Private => quote! {},
        PoolVisibility::Pub => quote! { pub },
        PoolVisibility::PubCrate => quote! { pub(crate) },
    };

    let tokens = quote! {
        #(#param_structs)*

        pub struct #repo_ident {
            #pool_vis pool: #pool_type,
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

fn macro_arg_for_field(field: &ParsedField) -> TokenStream {
    let name = format_ident!("{}", field.rust_name);
    let check_type = if field.is_nullable {
        &field.inner_type
    } else {
        &field.rust_type
    };
    let normalized = check_type.replace(' ', "");
    if normalized.starts_with("Vec<") {
        quote! { params.#name.as_slice() }
    } else {
        quote! { params.#name }
    }
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
    use_macro: bool,
) -> TokenStream {
    if use_macro {
        let macro_args: Vec<TokenStream> = non_pk_fields
            .iter()
            .map(|f| macro_arg_for_field(f))
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

#[allow(clippy::too_many_arguments)]
fn build_insert_many_transactionally_method(
    entity_ident: &proc_macro2::Ident,
    insert_params_ident: &proc_macro2::Ident,
    col_list: &str,
    num_cols: usize,
    insert_source_fields: &[&ParsedField],
    binds_loop: &[TokenStream],
    db_kind: DatabaseKind,
    table_name: &str,
    pk_fields: &[&ParsedField],
) -> TokenStream {
    let body = match db_kind {
        DatabaseKind::Postgres | DatabaseKind::Sqlite => {
            let col_list_str = col_list.to_string();
            let table_name_str = table_name.to_string();

            let row_placeholder_exprs: Vec<TokenStream> = insert_source_fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let offset = i;
                    match &f.column_default {
                        Some(default_expr) => {
                            let def = default_expr.as_str();
                            match db_kind {
                                DatabaseKind::Postgres => quote! {
                                    format!("COALESCE(${}, {})", base + #offset + 1, #def)
                                },
                                _ => quote! {
                                    format!("COALESCE(?, {})", #def)
                                },
                            }
                        }
                        None => {
                            match db_kind {
                                DatabaseKind::Postgres => quote! {
                                    format!("${}", base + #offset + 1)
                                },
                                _ => quote! {
                                    "?".to_string()
                                },
                            }
                        }
                    }
                })
                .collect();

            quote! {
                let mut tx = self.pool.begin().await?;
                let mut all_results = Vec::with_capacity(entries.len());
                let max_per_chunk = 65535 / #num_cols;
                for chunk in entries.chunks(max_per_chunk) {
                    let mut values_parts = Vec::with_capacity(chunk.len());
                    for (row_idx, _) in chunk.iter().enumerate() {
                        let base = row_idx * #num_cols;
                        let placeholders = vec![#(#row_placeholder_exprs),*];
                        values_parts.push(format!("({})", placeholders.join(", ")));
                    }
                    let sql = format!(
                        "INSERT INTO {} ({}) VALUES {} RETURNING *",
                        #table_name_str,
                        #col_list_str,
                        values_parts.join(", ")
                    );
                    let mut query = sqlx::query_as::<_, #entity_ident>(&sql);
                    for params in chunk {
                        #(#binds_loop)*
                    }
                    let rows = query.fetch_all(&mut *tx).await?;
                    all_results.extend(rows);
                }
                tx.commit().await?;
                Ok(all_results)
            }
        }
        DatabaseKind::Mysql => {
            let single_placeholders: String = insert_source_fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let p = placeholder(db_kind, i + 1);
                    match &f.column_default {
                        Some(default_expr) => format!("COALESCE({}, {})", p, default_expr),
                        None => p,
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");

            let single_insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name, col_list, single_placeholders
            );

            let single_binds: Vec<TokenStream> = insert_source_fields
                .iter()
                .map(|f| {
                    let name = format_ident!("{}", f.rust_name);
                    quote! { .bind(&params.#name) }
                })
                .collect();

            let pk_where = build_where_clause_parsed(pk_fields, db_kind, 1);
            let select_sql = format!("SELECT * FROM {} WHERE {}", table_name, pk_where);

            quote! {
                let mut tx = self.pool.begin().await?;
                let mut results = Vec::with_capacity(entries.len());
                for params in &entries {
                    sqlx::query(#single_insert_sql)
                        #(#single_binds)*
                        .execute(&mut *tx)
                        .await?;
                    let id = sqlx::query_scalar::<_, i64>("SELECT LAST_INSERT_ID()")
                        .fetch_one(&mut *tx)
                        .await?;
                    let row = sqlx::query_as::<_, #entity_ident>(#select_sql)
                        .bind(id)
                        .fetch_one(&mut *tx)
                        .await?;
                    results.push(row);
                }
                tx.commit().await?;
                Ok(results)
            }
        }
    };

    quote! {
        pub async fn insert_many_transactionally(
            &self,
            entries: Vec<#insert_params_ident>,
        ) -> Result<Vec<#entity_ident>, sqlx::Error> {
            #body
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
            column_default: None,
        }
    }

    fn make_field_with_default(rust_name: &str, column_name: &str, rust_type: &str, nullable: bool, is_pk: bool, default: &str) -> ParsedField {
        let mut f = make_field(rust_name, column_name, rust_type, nullable, is_pk);
        f.column_default = Some(default.to_string());
        f
    }

    fn entity_with_defaults() -> ParsedEntity {
        ParsedEntity {
            struct_name: "Tasks".to_string(),
            table_name: "tasks".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("id", "id", "i32", false, true),
                make_field("title", "title", "String", false, false),
                make_field_with_default("status", "status", "String", false, false, "'idle'::task_status"),
                make_field_with_default("priority", "priority", "i32", false, false, "0"),
                make_field_with_default("created_at", "created_at", "DateTime<Utc>", false, false, "now()"),
                make_field("description", "description", "Option<String>", true, false),
                make_field_with_default("deleted_at", "deleted_at", "Option<DateTime<Utc>>", true, false, "NULL"),
            ],
            imports: vec![],
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
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::users", &skip, false, PoolVisibility::Private);
        parse_and_format(&tokens)
    }

    fn gen_macro(entity: &ParsedEntity, db: DatabaseKind) -> String {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::users", &skip, true, PoolVisibility::Private);
        parse_and_format(&tokens)
    }

    fn gen_with_methods(entity: &ParsedEntity, db: DatabaseKind, methods: &Methods) -> String {
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::users", methods, false, PoolVisibility::Private);
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
    fn test_repo_pool_field_pub() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false, PoolVisibility::Pub);
        let code = parse_and_format(&tokens);
        assert!(code.contains("pub pool: sqlx::PgPool") || code.contains("pub pool: sqlx :: PgPool"));
    }

    #[test]
    fn test_repo_pool_field_pub_crate() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false, PoolVisibility::PubCrate);
        let code = parse_and_format(&tokens);
        assert!(code.contains("pub(crate) pool: sqlx::PgPool") || code.contains("pub(crate) pool: sqlx :: PgPool"));
    }

    #[test]
    fn test_repo_pool_field_private() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        // Should NOT have `pub pool` or `pub(crate) pool`
        assert!(!code.contains("pub pool"));
        assert!(!code.contains("pub(crate) pool"));
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

    // --- insert with column_default ---

    #[test]
    fn test_insert_default_col_is_optional() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        // Fields with column_default and not nullable → Option<T>
        let struct_start = code.find("pub struct InsertTasksParams").expect("InsertTasksParams not found");
        let struct_end = code[struct_start..].find('}').unwrap() + struct_start;
        let struct_body = &code[struct_start..struct_end];
        assert!(struct_body.contains("Option") && struct_body.contains("status"), "Expected status as Option in InsertTasksParams: {}", struct_body);
    }

    #[test]
    fn test_insert_non_default_col_required() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        // 'title' has no default → required type (String)
        let struct_start = code.find("pub struct InsertTasksParams").expect("InsertTasksParams not found");
        let struct_end = code[struct_start..].find('}').unwrap() + struct_start;
        let struct_body = &code[struct_start..struct_end];
        assert!(struct_body.contains("title") && struct_body.contains("String"), "Expected title as String: {}", struct_body);
    }

    #[test]
    fn test_insert_default_col_coalesce_sql() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        assert!(code.contains("COALESCE($2, 'idle'::task_status)"), "Expected COALESCE for status:\n{}", code);
        assert!(code.contains("COALESCE($3, 0)"), "Expected COALESCE for priority:\n{}", code);
        assert!(code.contains("COALESCE($4, now())"), "Expected COALESCE for created_at:\n{}", code);
    }

    #[test]
    fn test_insert_no_coalesce_for_non_default() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        // title has no default, so its placeholder should be plain $1 not COALESCE
        assert!(code.contains("VALUES ($1, COALESCE"), "Expected $1 without COALESCE for title:\n{}", code);
    }

    #[test]
    fn test_insert_nullable_with_default_no_double_option() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        assert!(!code.contains("Option < Option") && !code.contains("Option<Option"), "Should not have Option<Option>:\n{}", code);
    }

    #[test]
    fn test_insert_derive_default() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        let struct_start = code.find("pub struct InsertTasksParams").expect("InsertTasksParams not found");
        let before_struct = &code[..struct_start];
        assert!(before_struct.ends_with("Default)]\n") || before_struct.contains("Default)]"), "Expected #[derive(Default)] on InsertTasksParams");
    }

    // --- update (patch with COALESCE) ---

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
    fn test_update_pk_in_fn_signature() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        // PK 'id: i32' should appear between "fn update" and "UpdateUsersParams"
        let update_pos = code.find("fn update").expect("fn update not found");
        let params_pos = code[update_pos..].find("UpdateUsersParams").expect("UpdateUsersParams not found in update fn");
        let signature = &code[update_pos..update_pos + params_pos];
        assert!(signature.contains("id"), "Expected 'id' PK in update fn signature: {}", signature);
    }

    #[test]
    fn test_update_pk_not_in_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        // UpdateUsersParams should NOT contain id field
        // Extract the struct definition and check it doesn't have id
        let struct_start = code.find("pub struct UpdateUsersParams").expect("UpdateUsersParams not found");
        let struct_end = code[struct_start..].find('}').unwrap() + struct_start;
        let struct_body = &code[struct_start..struct_end];
        assert!(!struct_body.contains("pub id"), "PK 'id' should not be in UpdateUsersParams:\n{}", struct_body);
    }

    #[test]
    fn test_update_params_non_nullable_wrapped_in_option() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        // `name: String` becomes `name: Option<String>` in patch params
        assert!(code.contains("pub name: Option<String>") || code.contains("pub name : Option < String >"));
    }

    #[test]
    fn test_update_params_already_nullable_no_double_option() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        // `email: Option<String>` stays `Option<String>`, NOT `Option<Option<String>>`
        assert!(!code.contains("Option<Option") && !code.contains("Option < Option"));
    }

    #[test]
    fn test_update_set_clause_uses_coalesce_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("COALESCE($1, name)"), "Expected COALESCE for name:\n{}", code);
        assert!(code.contains("COALESCE($2, email)"), "Expected COALESCE for email:\n{}", code);
    }

    #[test]
    fn test_update_where_clause_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("WHERE id = $3"));
    }

    #[test]
    fn test_update_returning_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("COALESCE"));
        assert!(code.contains("RETURNING *"));
    }

    #[test]
    fn test_update_set_clause_mysql() {
        let code = gen(&standard_entity(), DatabaseKind::Mysql);
        assert!(code.contains("COALESCE(?, name)"), "Expected COALESCE for MySQL:\n{}", code);
        assert!(code.contains("COALESCE(?, email)"), "Expected COALESCE for email in MySQL:\n{}", code);
    }

    #[test]
    fn test_update_set_clause_sqlite() {
        let code = gen(&standard_entity(), DatabaseKind::Sqlite);
        assert!(code.contains("COALESCE(?, name)"), "Expected COALESCE for SQLite:\n{}", code);
    }

    // --- overwrite (full replacement, PK as fn param) ---

    #[test]
    fn test_overwrite_method() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn overwrite"));
    }

    #[test]
    fn test_overwrite_params_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct OverwriteUsersParams"));
    }

    #[test]
    fn test_overwrite_pk_in_fn_signature() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        let pos = code.find("fn overwrite").expect("fn overwrite not found");
        let params_pos = code[pos..].find("OverwriteUsersParams").expect("OverwriteUsersParams not found");
        let signature = &code[pos..pos + params_pos];
        assert!(signature.contains("id"), "Expected PK in overwrite fn signature: {}", signature);
    }

    #[test]
    fn test_overwrite_pk_not_in_struct() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        let struct_start = code.find("pub struct OverwriteUsersParams").expect("OverwriteUsersParams not found");
        let struct_end = code[struct_start..].find('}').unwrap() + struct_start;
        let struct_body = &code[struct_start..struct_end];
        assert!(!struct_body.contains("pub id"), "PK should not be in OverwriteUsersParams: {}", struct_body);
    }

    #[test]
    fn test_overwrite_no_coalesce() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        // Find the overwrite SQL — should have direct SET, no COALESCE
        let pos = code.find("fn overwrite").expect("fn overwrite not found");
        let method_body = &code[pos..pos + 500.min(code.len() - pos)];
        assert!(!method_body.contains("COALESCE"), "Overwrite should not use COALESCE: {}", method_body);
    }

    #[test]
    fn test_overwrite_set_clause_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("SET name = $1, email = $2 WHERE id = $3"));
    }

    #[test]
    fn test_overwrite_returning_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        let pos = code.find("fn overwrite").expect("fn overwrite not found");
        let method_body = &code[pos..pos + 500.min(code.len() - pos)];
        assert!(method_body.contains("RETURNING *"), "Expected RETURNING * in overwrite");
    }

    #[test]
    fn test_view_no_overwrite() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(!code.contains("pub async fn overwrite"));
    }

    #[test]
    fn test_without_overwrite() {
        let m = Methods { overwrite: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn overwrite"));
        assert!(!code.contains("OverwriteUsersParams"));
    }

    #[test]
    fn test_update_and_overwrite_coexist() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn update"), "Expected update method");
        assert!(code.contains("pub async fn overwrite"), "Expected overwrite method");
        assert!(code.contains("UpdateUsersParams"), "Expected UpdateUsersParams");
        assert!(code.contains("OverwriteUsersParams"), "Expected OverwriteUsersParams");
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
        let m = Methods { insert: false, insert_many: false, ..Methods::all() };
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
        assert!(!code.contains("pub async fn overwrite"));
        assert!(!code.contains("pub async fn delete"));
        assert!(!code.contains("pub async fn insert_many"));
    }

    // --- imports ---

    #[test]
    fn test_no_pool_import() {
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false, PoolVisibility::Private);
        assert!(!imports.iter().any(|i| i.contains("PgPool")));
    }

    #[test]
    fn test_imports_contain_entity() {
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false, PoolVisibility::Private);
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
        let (_, imports) = generate_crud_from_parsed(&entity, DatabaseKind::Postgres, "crate::models::users", &skip, false, PoolVisibility::Private);
        assert!(imports.iter().any(|i| i.contains("chrono")));
        assert!(imports.iter().any(|i| i.contains("uuid")));
    }

    #[test]
    fn test_entity_imports_empty_when_no_imports() {
        let skip = Methods::all();
        let (_, imports) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &skip, false, PoolVisibility::Private);
        // Should only have pool + entity imports, no chrono/uuid
        assert!(!imports.iter().any(|i| i.contains("chrono")));
        assert!(!imports.iter().any(|i| i.contains("uuid")));
    }

    // --- query_macro mode ---

    #[test]
    fn test_macro_get_all() {
        let m = Methods { get_all: true, ..Default::default() };
        let (tokens, _) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &m, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
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
        assert!(code.contains("COALESCE"), "Expected COALESCE in macro update:\n{}", code);
        assert!(code.contains("pub async fn update"));
        assert!(code.contains("UpdateUsersParams"));
    }

    #[test]
    fn test_macro_delete() {
        let code = gen_macro(&standard_entity(), DatabaseKind::Postgres);
        // delete uses query! (no return type)
        assert!(code.contains("query!"));
    }

    #[test]
    fn test_macro_no_bind_calls() {
        // insert_many always uses runtime mode, so exclude it for this test
        let m = Methods { insert_many: false, ..Methods::all() };
        let (tokens, _) = generate_crud_from_parsed(&standard_entity(), DatabaseKind::Postgres, "crate::models::users", &m, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        assert!(!code.contains(".bind("));
    }

    #[test]
    fn test_function_style_uses_bind() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains(".bind("));
        assert!(!code.contains("query_as!("));
        assert!(!code.contains("query!("));
    }

    // --- custom sql_type fallback: macro mode + custom type → runtime for SELECT, macro for DELETE ---

    fn entity_with_sql_array() -> ParsedEntity {
        ParsedEntity {
            struct_name: "AgentConnector".to_string(),
            table_name: "agent.agent_connector".to_string(),
            schema_name: Some("agent".to_string()),
            is_view: false,
            fields: vec![
                ParsedField {
                    rust_name: "connector_id".to_string(),
                    column_name: "connector_id".to_string(),
                    rust_type: "Uuid".to_string(),
                    inner_type: "Uuid".to_string(),
                    is_nullable: false,
                    is_primary_key: true,
                    sql_type: None,
                    is_sql_array: false,
                    column_default: None,
                },
                ParsedField {
                    rust_name: "agent_id".to_string(),
                    column_name: "agent_id".to_string(),
                    rust_type: "Uuid".to_string(),
                    inner_type: "Uuid".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    sql_type: None,
                    is_sql_array: false,
                    column_default: None,
                },
                ParsedField {
                    rust_name: "usages".to_string(),
                    column_name: "usages".to_string(),
                    rust_type: "Vec<ConnectorUsages>".to_string(),
                    inner_type: "Vec<ConnectorUsages>".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    sql_type: Some("agent.connector_usages".to_string()),
                    is_sql_array: true,
                    column_default: None,
                },
            ],
            imports: vec!["use uuid::Uuid;".to_string()],
        }
    }

    fn gen_macro_array(entity: &ParsedEntity, db: DatabaseKind) -> String {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(entity, db, "crate::models::agent_connector", &skip, true, PoolVisibility::Private);
        parse_and_format(&tokens)
    }

    #[test]
    fn test_sql_array_macro_get_all_uses_runtime() {
        let code = gen_macro_array(&entity_with_sql_array(), DatabaseKind::Postgres);
        // get_all should use runtime query_as, not macro
        assert!(code.contains("query_as::<"));
    }

    #[test]
    fn test_sql_array_macro_get_uses_runtime() {
        let code = gen_macro_array(&entity_with_sql_array(), DatabaseKind::Postgres);
        // get should use .bind( since it's runtime
        assert!(code.contains(".bind("));
    }

    #[test]
    fn test_sql_array_macro_insert_uses_runtime() {
        let code = gen_macro_array(&entity_with_sql_array(), DatabaseKind::Postgres);
        // insert RETURNING should use runtime query_as
        assert!(code.contains("query_as::<_ , AgentConnector>") || code.contains("query_as::<_, AgentConnector>"));
    }


    #[test]
    fn test_sql_array_macro_delete_still_uses_macro() {
        let code = gen_macro_array(&entity_with_sql_array(), DatabaseKind::Postgres);
        // delete uses query! macro (no rows returned, no array issue)
        assert!(code.contains("query!"));
    }

    #[test]
    fn test_sql_array_no_query_as_macro() {
        let code = gen_macro_array(&entity_with_sql_array(), DatabaseKind::Postgres);
        // Should NOT contain query_as! macro (only query_as::<_ for runtime)
        assert!(!code.contains("query_as!("));
    }

    // --- custom enum (non-array) also triggers runtime fallback ---

    fn entity_with_sql_enum() -> ParsedEntity {
        ParsedEntity {
            struct_name: "Task".to_string(),
            table_name: "tasks".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                ParsedField {
                    rust_name: "id".to_string(),
                    column_name: "id".to_string(),
                    rust_type: "i32".to_string(),
                    inner_type: "i32".to_string(),
                    is_nullable: false,
                    is_primary_key: true,
                    sql_type: None,
                    is_sql_array: false,
                    column_default: None,
                },
                ParsedField {
                    rust_name: "status".to_string(),
                    column_name: "status".to_string(),
                    rust_type: "TaskStatus".to_string(),
                    inner_type: "TaskStatus".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    sql_type: Some("task_status".to_string()),
                    is_sql_array: false,
                    column_default: None,
                },
            ],
            imports: vec![],
        }
    }

    #[test]
    fn test_sql_enum_macro_uses_runtime() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&entity_with_sql_enum(), DatabaseKind::Postgres, "crate::models::task", &skip, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        // SELECT queries should use runtime query_as, not macro
        assert!(code.contains("query_as::<"));
        assert!(!code.contains("query_as!("));
    }

    #[test]
    fn test_sql_enum_macro_delete_still_uses_macro() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&entity_with_sql_enum(), DatabaseKind::Postgres, "crate::models::task", &skip, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        // DELETE still uses query! macro
        assert!(code.contains("query!"));
    }

    // --- Vec<String> native array uses .as_slice() in macro mode ---

    fn entity_with_vec_string() -> ParsedEntity {
        ParsedEntity {
            struct_name: "PromptHistory".to_string(),
            table_name: "prompt_history".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                ParsedField {
                    rust_name: "id".to_string(),
                    column_name: "id".to_string(),
                    rust_type: "Uuid".to_string(),
                    inner_type: "Uuid".to_string(),
                    is_nullable: false,
                    is_primary_key: true,
                    sql_type: None,
                    is_sql_array: false,
                    column_default: None,
                },
                ParsedField {
                    rust_name: "content".to_string(),
                    column_name: "content".to_string(),
                    rust_type: "String".to_string(),
                    inner_type: "String".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    sql_type: None,
                    is_sql_array: false,
                    column_default: None,
                },
                ParsedField {
                    rust_name: "tags".to_string(),
                    column_name: "tags".to_string(),
                    rust_type: "Vec<String>".to_string(),
                    inner_type: "Vec<String>".to_string(),
                    is_nullable: false,
                    is_primary_key: false,
                    sql_type: None,
                    is_sql_array: false,
                    column_default: None,
                },
            ],
            imports: vec!["use uuid::Uuid;".to_string()],
        }
    }

    #[test]
    fn test_vec_string_macro_insert_uses_as_slice() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&entity_with_vec_string(), DatabaseKind::Postgres, "crate::models::prompt_history", &skip, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        assert!(code.contains("as_slice()"));
    }

    #[test]
    fn test_vec_string_macro_update_uses_as_slice() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&entity_with_vec_string(), DatabaseKind::Postgres, "crate::models::prompt_history", &skip, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        // Should have as_slice() for insert and update
        let count = code.matches("as_slice()").count();
        assert!(count >= 2, "expected at least 2 as_slice() calls (insert + update), found {}", count);
    }

    #[test]
    fn test_vec_string_non_macro_no_as_slice() {
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&entity_with_vec_string(), DatabaseKind::Postgres, "crate::models::prompt_history", &skip, false, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        // Runtime mode uses .bind() so no as_slice needed
        assert!(!code.contains("as_slice()"));
    }

    #[test]
    fn test_vec_string_parsed_from_source_uses_as_slice() {
        use crate::codegen::entity_parser::parse_entity_source;
        let source = r#"
            use uuid::Uuid;

            #[derive(Debug, Clone, sqlx::FromRow, SqlxGen)]
            #[sqlx_gen(kind = "table", schema = "agent", table = "prompt_history")]
            pub struct PromptHistory {
                #[sqlx_gen(primary_key)]
                pub id: Uuid,
                pub content: String,
                pub tags: Vec<String>,
            }
        "#;
        let entity = parse_entity_source(source).unwrap();
        let skip = Methods::all();
        let (tokens, _) = generate_crud_from_parsed(&entity, DatabaseKind::Postgres, "crate::models::prompt_history", &skip, true, PoolVisibility::Private);
        let code = parse_and_format(&tokens);
        assert!(code.contains("as_slice()"), "Expected as_slice() in generated code:\n{}", code);
    }

    // --- composite PK only (junction table) ---

    fn junction_entity() -> ParsedEntity {
        ParsedEntity {
            struct_name: "AnalysisRecord".to_string(),
            table_name: "analysis.analysis__record".to_string(),
            schema_name: None,
            is_view: false,
            fields: vec![
                make_field("record_id", "record_id", "uuid::Uuid", false, true),
                make_field("analysis_id", "analysis_id", "uuid::Uuid", false, true),
            ],
            imports: vec![],
        }
    }

    #[test]
    fn test_composite_pk_only_insert_generated() {
        let code = gen(&junction_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub struct InsertAnalysisRecordParams"), "Expected InsertAnalysisRecordParams struct:\n{}", code);
        assert!(code.contains("pub record_id"), "Expected record_id field in insert params:\n{}", code);
        assert!(code.contains("pub analysis_id"), "Expected analysis_id field in insert params:\n{}", code);
        assert!(code.contains("INSERT INTO analysis.analysis__record (record_id, analysis_id) VALUES ($1, $2) RETURNING *"), "Expected valid INSERT SQL:\n{}", code);
        assert!(code.contains("pub async fn insert"), "Expected insert method:\n{}", code);
    }

    #[test]
    fn test_composite_pk_only_no_update() {
        let code = gen(&junction_entity(), DatabaseKind::Postgres);
        assert!(!code.contains("UpdateAnalysisRecordParams"), "Expected no UpdateAnalysisRecordParams struct:\n{}", code);
        assert!(!code.contains("pub async fn update"), "Expected no update method:\n{}", code);
    }


    #[test]
    fn test_composite_pk_only_delete_generated() {
        let code = gen(&junction_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn delete"), "Expected delete method:\n{}", code);
        assert!(code.contains("DELETE FROM analysis.analysis__record WHERE record_id = $1 AND analysis_id = $2"), "Expected valid DELETE SQL:\n{}", code);
    }

    #[test]
    fn test_composite_pk_only_get_generated() {
        let code = gen(&junction_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn get"), "Expected get method:\n{}", code);
        assert!(code.contains("WHERE record_id = $1 AND analysis_id = $2"), "Expected WHERE clause with both PK columns:\n{}", code);
    }

    // --- insert_many_transactionally ---

    #[test]
    fn test_insert_many_transactionally_method_generated() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn insert_many_transactionally"), "Expected insert_many_transactionally method:\n{}", code);
    }

    #[test]
    fn test_insert_many_transactionally_signature() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(code.contains("entries: Vec<InsertUsersParams>"), "Expected Vec<InsertUsersParams> param:\n{}", code);
        assert!(code.contains("Result<Vec<Users>"), "Expected Result<Vec<Users>> return type:\n{}", code);
    }

    #[test]
    fn test_insert_many_transactionally_no_strategy_enum() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        assert!(!code.contains("TransactionStrategy"), "TransactionStrategy should not be generated:\n{}", code);
        assert!(!code.contains("InsertManyUsersResult"), "InsertManyUsersResult should not be generated:\n{}", code);
    }

    #[test]
    fn test_insert_many_transactionally_uses_transaction_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        let method_start = code.find("fn insert_many_transactionally").expect("insert_many_transactionally not found");
        let method_body = &code[method_start..];
        assert!(method_body.contains("self.pool.begin()"), "Expected begin():\n{}", method_body);
        assert!(method_body.contains("tx.commit()"), "Expected commit():\n{}", method_body);
    }

    #[test]
    fn test_insert_many_transactionally_multi_row_pg() {
        let code = gen(&standard_entity(), DatabaseKind::Postgres);
        let method_start = code.find("fn insert_many_transactionally").expect("not found");
        let method_body = &code[method_start..];
        assert!(method_body.contains("RETURNING *"), "Expected RETURNING * in multi-row SQL:\n{}", method_body);
        assert!(method_body.contains("values_parts"), "Expected multi-row VALUES building:\n{}", method_body);
        assert!(method_body.contains("65535"), "Expected chunk size limit:\n{}", method_body);
    }

    #[test]
    fn test_insert_many_transactionally_multi_row_sqlite() {
        let code = gen(&standard_entity(), DatabaseKind::Sqlite);
        let method_start = code.find("fn insert_many_transactionally").expect("not found");
        let method_body = &code[method_start..];
        assert!(method_body.contains("values_parts"), "Expected multi-row VALUES building for SQLite:\n{}", method_body);
        assert!(method_body.contains("RETURNING *"), "Expected RETURNING * for SQLite:\n{}", method_body);
    }

    #[test]
    fn test_insert_many_transactionally_mysql_individual_inserts() {
        let code = gen(&standard_entity(), DatabaseKind::Mysql);
        let method_start = code.find("fn insert_many_transactionally").expect("not found");
        let method_body = &code[method_start..];
        assert!(method_body.contains("LAST_INSERT_ID"), "Expected LAST_INSERT_ID for MySQL:\n{}", method_body);
        assert!(method_body.contains("self.pool.begin()"), "Expected begin() for MySQL:\n{}", method_body);
    }

    #[test]
    fn test_insert_many_transactionally_view_not_generated() {
        let mut entity = standard_entity();
        entity.is_view = true;
        let code = gen(&entity, DatabaseKind::Postgres);
        assert!(!code.contains("pub async fn insert_many_transactionally"), "should not be generated for views");
    }

    #[test]
    fn test_insert_many_transactionally_without_method_not_generated() {
        let m = Methods { insert_many: false, ..Methods::all() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(!code.contains("pub async fn insert_many_transactionally"), "should not be generated when disabled");
    }

    #[test]
    fn test_insert_many_transactionally_generates_params_when_insert_disabled() {
        let m = Methods { insert: false, insert_many: true, ..Default::default() };
        let code = gen_with_methods(&standard_entity(), DatabaseKind::Postgres, &m);
        assert!(code.contains("pub struct InsertUsersParams"), "Expected InsertUsersParams:\n{}", code);
        assert!(code.contains("pub async fn insert_many_transactionally"), "Expected method:\n{}", code);
        assert!(!code.contains("pub async fn insert("), "insert should not be present:\n{}", code);
    }

    #[test]
    fn test_insert_many_transactionally_with_column_defaults_coalesce() {
        let code = gen(&entity_with_defaults(), DatabaseKind::Postgres);
        let method_start = code.find("fn insert_many_transactionally").expect("not found");
        let method_body = &code[method_start..];
        assert!(method_body.contains("COALESCE"), "Expected COALESCE for fields with defaults:\n{}", method_body);
    }

    #[test]
    fn test_insert_many_transactionally_junction_table() {
        let code = gen(&junction_entity(), DatabaseKind::Postgres);
        assert!(code.contains("pub async fn insert_many_transactionally"), "Expected method for junction table:\n{}", code);
    }

    #[test]
    fn test_insert_many_transactionally_all_three_backends_compile() {
        for db in [DatabaseKind::Postgres, DatabaseKind::Mysql, DatabaseKind::Sqlite] {
            let code = gen(&standard_entity(), db);
            assert!(code.contains("pub async fn insert_many_transactionally"), "Expected method for {:?}", db);
        }
    }
}
