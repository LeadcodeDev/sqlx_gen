use anyhow::Result;
use clap::Parser;
use sqlx::{MySqlPool, PgPool, SqlitePool};

use sqlx_gen::cli::{Args, DatabaseKind};
use sqlx_gen::codegen;
use sqlx_gen::introspect;
use sqlx_gen::writer;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let db_kind = args.database_kind()?;
    let type_overrides = args.parse_type_overrides();

    eprintln!("Connecting to {} database...", match db_kind {
        DatabaseKind::Postgres => "PostgreSQL",
        DatabaseKind::Mysql => "MySQL",
        DatabaseKind::Sqlite => "SQLite",
    });

    let mut schema_info = match db_kind {
        DatabaseKind::Postgres => {
            let pool = PgPool::connect(&args.database_url).await?;
            let info = introspect::postgres::introspect(&pool, &args.schemas, args.views).await?;
            pool.close().await;
            info
        }
        DatabaseKind::Mysql => {
            let pool = MySqlPool::connect(&args.database_url).await?;
            let info = introspect::mysql::introspect(&pool, &args.schemas, args.views).await?;
            pool.close().await;
            info
        }
        DatabaseKind::Sqlite => {
            let pool = SqlitePool::connect(&args.database_url).await?;
            let info = introspect::sqlite::introspect(&pool, args.views).await?;
            pool.close().await;
            info
        }
    };

    // Filter tables if requested
    if let Some(ref filter) = args.tables {
        schema_info.tables.retain(|t| filter.contains(&t.name));
    }

    let table_count = schema_info.tables.len();
    let view_count = schema_info.views.len();
    let enum_count = schema_info.enums.len();
    eprintln!(
        "Found {} tables, {} views, {} enums, {} composite types, {} domains",
        table_count,
        view_count,
        enum_count,
        schema_info.composite_types.len(),
        schema_info.domains.len(),
    );

    let files = codegen::generate(&schema_info, db_kind, &args.derives, &type_overrides, args.single_file);

    writer::write_files(&files, &args.output_dir, args.single_file, args.dry_run)?;

    if !args.dry_run {
        eprintln!("Done! Generated {} files.", files.len() + 1); // +1 for mod.rs
    }

    Ok(())
}
