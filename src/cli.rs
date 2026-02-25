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

    /// Also generate structs for SQL views
    #[arg(long)]
    pub views: bool,

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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_args(url: &str) -> Args {
        Args {
            database_url: url.to_string(),
            output_dir: PathBuf::from("out"),
            schemas: vec!["public".into()],
            derives: vec![],
            type_overrides: vec![],
            single_file: false,
            tables: None,
            views: false,
            dry_run: false,
        }
    }

    fn make_args_with_overrides(overrides: Vec<&str>) -> Args {
        Args {
            database_url: "postgres://localhost/db".to_string(),
            output_dir: PathBuf::from("out"),
            schemas: vec!["public".into()],
            derives: vec![],
            type_overrides: overrides.into_iter().map(|s| s.to_string()).collect(),
            single_file: false,
            tables: None,
            views: false,
            dry_run: false,
        }
    }

    // ========== database_kind ==========

    #[test]
    fn test_postgres_url() {
        let args = make_args("postgres://localhost/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Postgres);
    }

    #[test]
    fn test_postgresql_url() {
        let args = make_args("postgresql://localhost/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Postgres);
    }

    #[test]
    fn test_postgres_full_url() {
        let args = make_args("postgres://user:pass@host:5432/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Postgres);
    }

    #[test]
    fn test_mysql_url() {
        let args = make_args("mysql://localhost/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Mysql);
    }

    #[test]
    fn test_mysql_full_url() {
        let args = make_args("mysql://user:pass@host:3306/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Mysql);
    }

    #[test]
    fn test_sqlite_url() {
        let args = make_args("sqlite://path.db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Sqlite);
    }

    #[test]
    fn test_sqlite_colon() {
        let args = make_args("sqlite:path.db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Sqlite);
    }

    #[test]
    fn test_sqlite_memory() {
        let args = make_args("sqlite::memory:");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Sqlite);
    }

    #[test]
    fn test_http_url_fails() {
        let args = make_args("http://example.com");
        assert!(args.database_kind().is_err());
    }

    #[test]
    fn test_empty_url_fails() {
        let args = make_args("");
        assert!(args.database_kind().is_err());
    }

    #[test]
    fn test_mongo_url_fails() {
        let args = make_args("mongo://localhost");
        assert!(args.database_kind().is_err());
    }

    #[test]
    fn test_uppercase_postgres_fails() {
        let args = make_args("POSTGRES://localhost");
        assert!(args.database_kind().is_err());
    }

    // ========== parse_type_overrides ==========

    #[test]
    fn test_overrides_empty() {
        let args = make_args_with_overrides(vec![]);
        assert!(args.parse_type_overrides().is_empty());
    }

    #[test]
    fn test_overrides_single() {
        let args = make_args_with_overrides(vec!["jsonb=MyJson"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("jsonb").unwrap(), "MyJson");
    }

    #[test]
    fn test_overrides_multiple() {
        let args = make_args_with_overrides(vec!["jsonb=MyJson", "uuid=MyUuid"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("jsonb").unwrap(), "MyJson");
        assert_eq!(map.get("uuid").unwrap(), "MyUuid");
    }

    #[test]
    fn test_overrides_malformed_skipped() {
        let args = make_args_with_overrides(vec!["noequals"]);
        assert!(args.parse_type_overrides().is_empty());
    }

    #[test]
    fn test_overrides_mixed_valid_invalid() {
        let args = make_args_with_overrides(vec!["good=val", "bad"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("good").unwrap(), "val");
    }

    #[test]
    fn test_overrides_equals_in_value() {
        let args = make_args_with_overrides(vec!["key=val=ue"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("key").unwrap(), "val=ue");
    }

    #[test]
    fn test_overrides_empty_key() {
        let args = make_args_with_overrides(vec!["=value"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("").unwrap(), "value");
    }

    #[test]
    fn test_overrides_empty_value() {
        let args = make_args_with_overrides(vec!["key="]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("key").unwrap(), "");
    }
}
