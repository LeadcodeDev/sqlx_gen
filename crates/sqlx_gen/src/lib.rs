#[cfg(feature = "cli")]
pub mod cli;
#[cfg(feature = "cli")]
pub mod codegen;
#[cfg(feature = "cli")]
pub mod error;
#[cfg(feature = "cli")]
pub mod introspect;
#[cfg(feature = "cli")]
pub mod typemap;
#[cfg(feature = "cli")]
pub mod writer;

pub use sqlx_gen_macros::SqlxGen;
