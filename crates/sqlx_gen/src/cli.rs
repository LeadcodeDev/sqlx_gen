use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "sqlx-gen", about = "Generate Rust structs from database schema")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Generate code from database schema
    Generate {
        #[command(subcommand)]
        subcommand: GenerateCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum GenerateCommand {
    /// Generate entity structs, enums, composites, and domains
    Entities(EntitiesArgs),
    /// Generate CRUD repository for a table or view
    Crud(CrudArgs),
}

#[derive(Parser, Debug)]
pub struct DatabaseArgs {
    /// Database connection URL
    #[arg(short = 'u', long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Schemas to introspect (comma-separated, PG default: public)
    #[arg(short = 's', long, value_delimiter = ',', default_value = "public")]
    pub schemas: Vec<String>,
}

impl DatabaseArgs {
    pub fn database_kind(&self) -> crate::error::Result<DatabaseKind> {
        let url = &self.database_url;
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(DatabaseKind::Postgres)
        } else if url.starts_with("mysql://") {
            Ok(DatabaseKind::Mysql)
        } else if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
            Ok(DatabaseKind::Sqlite)
        } else {
            Err(crate::error::Error::Config(
                "Cannot detect database type from URL. Expected postgres://, mysql://, or sqlite:// prefix.".to_string(),
            ))
        }
    }
}

#[derive(Parser, Debug)]
pub struct EntitiesArgs {
    #[command(flatten)]
    pub db: DatabaseArgs,

    /// Output directory for generated files
    #[arg(short = 'o', long, default_value = "src/models")]
    pub output_dir: PathBuf,

    /// Additional derives (e.g. Serialize,Deserialize,PartialEq)
    #[arg(short = 'D', long, value_delimiter = ',')]
    pub derives: Vec<String>,

    /// Type overrides (e.g. jsonb=MyJsonType,uuid=MyUuid)
    #[arg(short = 'T', long, value_delimiter = ',')]
    pub type_overrides: Vec<String>,

    /// Generate everything into a single file instead of one file per table
    #[arg(short = 'S', long)]
    pub single_file: bool,

    /// Only generate for these tables (comma-separated)
    #[arg(short = 't', long, value_delimiter = ',')]
    pub tables: Option<Vec<String>>,

    /// Exclude these tables/views from generation (comma-separated)
    #[arg(short = 'x', long, value_delimiter = ',')]
    pub exclude_tables: Option<Vec<String>>,

    /// Also generate structs for SQL views
    #[arg(short = 'v', long)]
    pub views: bool,

    /// Time crate to use for date/time types: chrono (default) or time
    #[arg(long, default_value = "chrono")]
    pub time_crate: TimeCrate,

    /// Print to stdout without writing files
    #[arg(short = 'n', long)]
    pub dry_run: bool,
}

impl EntitiesArgs {
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

#[derive(Parser, Debug)]
pub struct CrudArgs {
    /// Path to the generated entity .rs file
    #[arg(short = 'f', long)]
    pub entity_file: PathBuf,

    /// Database kind (postgres, mysql, sqlite)
    #[arg(short = 'd', long)]
    pub db_kind: String,

    /// Module path of generated entities (e.g. "crate::models::users").
    /// If omitted, derived from --entity-file by finding `src/` and converting the path.
    #[arg(short = 'e', long)]
    pub entities_module: Option<String>,

    /// Output directory for generated repository files
    #[arg(short = 'o', long, default_value = "src/crud")]
    pub output_dir: PathBuf,

    /// Methods to generate (comma-separated): *, get_all, paginate, get, insert, update, delete
    #[arg(short = 'm', long, value_delimiter = ',')]
    pub methods: Vec<String>,


    /// Use sqlx::query_as!() compile-time checked macros instead of query_as::<_, T>() functions
    #[arg(short = 'q', long)]
    pub query_macro: bool,

    /// Visibility of the pool field in generated repository structs: private, pub, pub(crate)
    #[arg(short = 'p', long, default_value = "private")]
    pub pool_visibility: PoolVisibility,

    /// Print to stdout without writing files
    #[arg(short = 'n', long)]
    pub dry_run: bool,
}

impl CrudArgs {
    pub fn database_kind(&self) -> crate::error::Result<DatabaseKind> {
        match self.db_kind.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(DatabaseKind::Postgres),
            "mysql" => Ok(DatabaseKind::Mysql),
            "sqlite" => Ok(DatabaseKind::Sqlite),
            other => Err(crate::error::Error::Config(format!(
                "Unknown database kind '{}'. Expected: postgres, mysql, sqlite",
                other
            ))),
        }
    }

    /// Resolve the entities module path: use the explicit value if provided,
    /// otherwise derive it from the entity file path.
    pub fn resolve_entities_module(&self) -> crate::error::Result<String> {
        match &self.entities_module {
            Some(m) => Ok(m.clone()),
            None => module_path_from_file(&self.entity_file),
        }
    }
}

/// Derive a Rust module path from a file path by finding `src/` and converting.
/// e.g. `some/project/src/models/users.rs` → `crate::models::users`
/// e.g. `src/db/entities/mod.rs` → `crate::db::entities`
fn module_path_from_file(path: &std::path::Path) -> crate::error::Result<String> {
    let path_str = path.to_string_lossy().replace('\\', "/");

    let after_src = match path_str.rfind("/src/") {
        Some(pos) => &path_str[pos + 5..],
        None if path_str.starts_with("src/") => &path_str[4..],
        _ => {
            return Err(crate::error::Error::Config(format!(
                "Cannot derive module path from '{}': no 'src/' found. Use --entities-module explicitly.",
                path.display()
            )));
        }
    };

    let without_ext = after_src.strip_suffix(".rs").unwrap_or(after_src);
    let module = without_ext.strip_suffix("/mod").unwrap_or(without_ext);

    let module_path = format!("crate::{}", module.replace('/', "::"));
    Ok(module_path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    Postgres,
    Mysql,
    Sqlite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeCrate {
    #[default]
    Chrono,
    Time,
}

impl std::str::FromStr for TimeCrate {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "chrono" => Ok(Self::Chrono),
            "time" => Ok(Self::Time),
            other => Err(format!(
                "Unknown time crate '{}'. Expected: chrono, time",
                other
            )),
        }
    }
}

impl std::fmt::Display for TimeCrate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chrono => write!(f, "chrono"),
            Self::Time => write!(f, "time"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolVisibility {
    #[default]
    Private,
    Pub,
    PubCrate,
}

impl std::str::FromStr for PoolVisibility {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "private" => Ok(Self::Private),
            "pub" => Ok(Self::Pub),
            "pub(crate)" => Ok(Self::PubCrate),
            other => Err(format!(
                "Unknown pool visibility '{}'. Expected: private, pub, pub(crate)",
                other
            )),
        }
    }
}

/// Which CRUD methods to generate. All fields default to `false`.
/// Use `Methods::from_list` to parse from CLI input.
#[derive(Debug, Clone, Default)]
pub struct Methods {
    pub get_all: bool,
    pub paginate: bool,
    pub get: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
}

const ALL_METHODS: &[&str] = &["get_all", "paginate", "get", "insert", "update", "delete"];

impl Methods {
    /// Parse a list of method names. `"*"` enables all methods.
    pub fn from_list(names: &[String]) -> Result<Self, String> {
        let mut m = Self::default();
        for name in names {
            match name.as_str() {
                "*" => return Ok(Self::all()),
                "get_all" => m.get_all = true,
                "paginate" => m.paginate = true,
                "get" => m.get = true,
                "insert" => m.insert = true,
                "update" => m.update = true,
                "delete" => m.delete = true,
                other => {
                    return Err(format!(
                        "Unknown method '{}'. Valid values: *, {}",
                        other,
                        ALL_METHODS.join(", ")
                    ))
                }
            }
        }
        Ok(m)
    }

    pub fn all() -> Self {
        Self {
            get_all: true,
            paginate: true,
            get: true,
            insert: true,
            update: true,
            delete: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_db_args(url: &str) -> DatabaseArgs {
        DatabaseArgs {
            database_url: url.to_string(),
            schemas: vec!["public".into()],
        }
    }

    fn make_entities_args_with_overrides(overrides: Vec<&str>) -> EntitiesArgs {
        EntitiesArgs {
            db: make_db_args("postgres://localhost/db"),
            output_dir: PathBuf::from("out"),
            derives: vec![],
            type_overrides: overrides.into_iter().map(|s| s.to_string()).collect(),
            single_file: false,
            tables: None,
            exclude_tables: None,
            views: false,
            time_crate: TimeCrate::Chrono,
            dry_run: false,
        }
    }

    // ========== database_kind ==========

    #[test]
    fn test_postgres_url() {
        let args = make_db_args("postgres://localhost/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Postgres);
    }

    #[test]
    fn test_postgresql_url() {
        let args = make_db_args("postgresql://localhost/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Postgres);
    }

    #[test]
    fn test_postgres_full_url() {
        let args = make_db_args("postgres://user:pass@host:5432/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Postgres);
    }

    #[test]
    fn test_mysql_url() {
        let args = make_db_args("mysql://localhost/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Mysql);
    }

    #[test]
    fn test_mysql_full_url() {
        let args = make_db_args("mysql://user:pass@host:3306/db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Mysql);
    }

    #[test]
    fn test_sqlite_url() {
        let args = make_db_args("sqlite://path.db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Sqlite);
    }

    #[test]
    fn test_sqlite_colon() {
        let args = make_db_args("sqlite:path.db");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Sqlite);
    }

    #[test]
    fn test_sqlite_memory() {
        let args = make_db_args("sqlite::memory:");
        assert_eq!(args.database_kind().unwrap(), DatabaseKind::Sqlite);
    }

    #[test]
    fn test_http_url_fails() {
        let args = make_db_args("http://example.com");
        assert!(args.database_kind().is_err());
    }

    #[test]
    fn test_empty_url_fails() {
        let args = make_db_args("");
        assert!(args.database_kind().is_err());
    }

    #[test]
    fn test_mongo_url_fails() {
        let args = make_db_args("mongo://localhost");
        assert!(args.database_kind().is_err());
    }

    #[test]
    fn test_uppercase_postgres_fails() {
        let args = make_db_args("POSTGRES://localhost");
        assert!(args.database_kind().is_err());
    }

    // ========== parse_type_overrides ==========

    #[test]
    fn test_overrides_empty() {
        let args = make_entities_args_with_overrides(vec![]);
        assert!(args.parse_type_overrides().is_empty());
    }

    #[test]
    fn test_overrides_single() {
        let args = make_entities_args_with_overrides(vec!["jsonb=MyJson"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("jsonb").unwrap(), "MyJson");
    }

    #[test]
    fn test_overrides_multiple() {
        let args = make_entities_args_with_overrides(vec!["jsonb=MyJson", "uuid=MyUuid"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("jsonb").unwrap(), "MyJson");
        assert_eq!(map.get("uuid").unwrap(), "MyUuid");
    }

    #[test]
    fn test_overrides_malformed_skipped() {
        let args = make_entities_args_with_overrides(vec!["noequals"]);
        assert!(args.parse_type_overrides().is_empty());
    }

    #[test]
    fn test_overrides_mixed_valid_invalid() {
        let args = make_entities_args_with_overrides(vec!["good=val", "bad"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("good").unwrap(), "val");
    }

    #[test]
    fn test_overrides_equals_in_value() {
        let args = make_entities_args_with_overrides(vec!["key=val=ue"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("key").unwrap(), "val=ue");
    }

    #[test]
    fn test_overrides_empty_key() {
        let args = make_entities_args_with_overrides(vec!["=value"]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("").unwrap(), "value");
    }

    #[test]
    fn test_overrides_empty_value() {
        let args = make_entities_args_with_overrides(vec!["key="]);
        let map = args.parse_type_overrides();
        assert_eq!(map.get("key").unwrap(), "");
    }

    // ========== exclude_tables ==========

    #[test]
    fn test_exclude_tables_default_none() {
        let args = make_entities_args_with_overrides(vec![]);
        assert!(args.exclude_tables.is_none());
    }

    #[test]
    fn test_exclude_tables_set() {
        let mut args = make_entities_args_with_overrides(vec![]);
        args.exclude_tables = Some(vec!["_migrations".to_string(), "schema_versions".to_string()]);
        assert_eq!(args.exclude_tables.as_ref().unwrap().len(), 2);
        assert!(args.exclude_tables.as_ref().unwrap().contains(&"_migrations".to_string()));
    }

    // ========== methods ==========

    #[test]
    fn test_methods_default_all_false() {
        let m = Methods::default();
        assert!(!m.get_all);
        assert!(!m.paginate);
        assert!(!m.get);
        assert!(!m.insert);
        assert!(!m.update);
        assert!(!m.delete);
    }

    #[test]
    fn test_methods_star() {
        let m = Methods::from_list(&["*".to_string()]).unwrap();
        assert!(m.get_all);
        assert!(m.paginate);
        assert!(m.get);
        assert!(m.insert);
        assert!(m.update);
        assert!(m.delete);
    }

    #[test]
    fn test_methods_single() {
        let m = Methods::from_list(&["get".to_string()]).unwrap();
        assert!(m.get);
        assert!(!m.get_all);
        assert!(!m.insert);
    }

    #[test]
    fn test_methods_multiple() {
        let m = Methods::from_list(&["get_all".to_string(), "delete".to_string()]).unwrap();
        assert!(m.get_all);
        assert!(m.delete);
        assert!(!m.insert);
        assert!(!m.paginate);
    }

    #[test]
    fn test_methods_unknown_fails() {
        let result = Methods::from_list(&["unknown".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown method"));
    }

    #[test]
    fn test_methods_all() {
        let m = Methods::all();
        assert!(m.get_all);
        assert!(m.paginate);
        assert!(m.get);
        assert!(m.insert);
        assert!(m.update);
        assert!(m.delete);
    }

    // ========== module_path_from_file ==========

    #[test]
    fn test_module_path_simple() {
        let p = PathBuf::from("src/models/users.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::models::users");
    }

    #[test]
    fn test_module_path_mod_rs() {
        let p = PathBuf::from("src/models/mod.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::models");
    }

    #[test]
    fn test_module_path_nested() {
        let p = PathBuf::from("src/db/entities/agent.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::db::entities::agent");
    }

    #[test]
    fn test_module_path_absolute_with_src() {
        let p = PathBuf::from("/home/user/project/src/models/users.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::models::users");
    }

    #[test]
    fn test_module_path_relative_with_src() {
        let p = PathBuf::from("../other_project/src/models/users.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::models::users");
    }

    #[test]
    fn test_module_path_no_src_fails() {
        let p = PathBuf::from("models/users.rs");
        assert!(module_path_from_file(&p).is_err());
    }

    #[test]
    fn test_module_path_deeply_nested_mod() {
        let p = PathBuf::from("src/a/b/c/mod.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::a::b::c");
    }

    #[test]
    fn test_module_path_src_root_file() {
        let p = PathBuf::from("src/lib.rs");
        assert_eq!(module_path_from_file(&p).unwrap(), "crate::lib");
    }
}
