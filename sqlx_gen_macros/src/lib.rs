use proc_macro::TokenStream;

/// No-op derive macro that registers `sqlx_gen` as a helper attribute.
///
/// This allows `#[sqlx_gen(...)]` to be used on both structs and fields
/// without the compiler rejecting them as unknown attributes.
///
/// # Usage
///
/// Add `SqlxGen` to your derive list:
/// ```ignore
/// #[derive(sqlx::FromRow, SqlxGen)]
/// #[sqlx_gen(kind = "table", table = "users")]
/// pub struct Users {
///     #[sqlx_gen(primary_key)]
///     pub id: i32,
/// }
/// ```
#[proc_macro_derive(SqlxGen, attributes(sqlx_gen))]
pub fn derive_sqlx_gen(_input: TokenStream) -> TokenStream {
    TokenStream::new()
}
