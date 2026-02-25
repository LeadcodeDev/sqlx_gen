use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "sqlx-gen", about = "Generate Rust structs from database schema")]
pub struct Args {
    /// Database connection URL
    #[arg(short = 'u', long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Output directory for generated files
    #[arg(short = 'o', long, default_value = "src/models")]
    pub output_dir: PathBuf,

    /// Schemas to introspect (comma-separated, PG default: public)
    #[arg(short = 's', long, value_delimiter = ',', default_value = "public")]
    pub schemas: Vec<String>,

    /// Additional derives (e.g. Serialize,Deserialize,PartialEq)
    #[arg(long, value_delimiter = ',')]
    pub derives: Vec<String>,

    /// Type overrides (e.g. jsonb=MyJsonType,uuid=MyUuid)
    #[arg(long, value_delimiter = ',')]
    pub type_overrides: Vec<String>,

    /// Generate everything into a single file instead of one file per table
    #[arg(long)]
    pub single_file: bool,

    /// Only generate for these tables (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub tables: Option<Vec<String>>,

    /// Print to stdout without writing files
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    Postgres,
    Mysql,
    Sqlite,
}

impl Args {
    pub fn database_kind(&self) -> anyhow::Result<DatabaseKind> {
        let url = &self.database_url;
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(DatabaseKind::Postgres)
        } else if url.starts_with("mysql://") {
            Ok(DatabaseKind::Mysql)
        } else if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
            Ok(DatabaseKind::Sqlite)
        } else {
            anyhow::bail!(
                "Cannot detect database type from URL. Expected postgres://, mysql://, or sqlite:// prefix."
            )
        }
    }

    pub fn parse_type_overrides(&self) -> HashMap<String, String> {
        self.type_overrides
            .iter()
            .filter_map(|s| {
                let (k, v) = s.split_once('=')?;
                Some((k.to_string(), v.to_string()))
            })
            .collect()
    }
}
