use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database connection failed ({redacted_url}): {source}")]
    Connection {
        redacted_url: String,
        #[source]
        source: sqlx::Error,
    },

    #[error("Permission denied while introspecting: {detail}. Check the DB user's privileges on information_schema / pg_catalog / sqlite_master.")]
    PermissionDenied { detail: String },

    #[error("Schema or relation not found: {detail}. Check `--schemas` and ensure the database contains the expected tables.")]
    SchemaNotFound { detail: String },

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Inspect a [`sqlx::Error`] and, if it carries a SQLSTATE we know how to
/// explain, return a richer [`Error`] variant. Otherwise the input is wrapped
/// in [`Error::Database`] unchanged so callers can keep using `?`.
pub fn contextualize_sqlx_error(err: sqlx::Error) -> Error {
    use sqlx::Error as Sx;
    let code: Option<String> = match &err {
        Sx::Database(db) => db.code().map(|c| c.to_string()),
        _ => None,
    };
    if let Some(code) = code {
        // PG: 42501 insufficient_privilege; MySQL: 42000 / 28000.
        // PG: 42P01 undefined_table, 3F000 invalid_schema_name; MySQL: 42S02.
        match code.as_str() {
            "42501" | "28000" => {
                return Error::PermissionDenied {
                    detail: err.to_string(),
                };
            }
            "42P01" | "3F000" | "42S02" => {
                return Error::SchemaNotFound {
                    detail: err.to_string(),
                };
            }
            _ => {}
        }
    }
    Error::Database(err)
}

/// Redact `user:password@host` → `user:****@host` in a database URL so it can
/// be embedded in error messages and logs without leaking credentials.
pub fn redact_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some(pair) => pair,
        None => return url.to_string(),
    };
    let (userinfo, host_part) = match rest.split_once('@') {
        Some(pair) => pair,
        None => return url.to_string(),
    };
    let redacted_userinfo = match userinfo.split_once(':') {
        Some((user, _pw)) => format!("{}:****", user),
        None => userinfo.to_string(),
    };
    format!("{}://{}@{}", scheme, redacted_userinfo, host_part)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password_in_postgres_url() {
        assert_eq!(
            redact_url("postgres://alice:s3cret@localhost:5432/db"),
            "postgres://alice:****@localhost:5432/db"
        );
    }

    #[test]
    fn redacts_password_in_mysql_url() {
        assert_eq!(
            redact_url("mysql://root:hunter2@db:3306/app"),
            "mysql://root:****@db:3306/app"
        );
    }

    #[test]
    fn redacts_password_in_postgresql_url() {
        assert_eq!(
            redact_url("postgresql://u:p@h/d"),
            "postgresql://u:****@h/d"
        );
    }

    #[test]
    fn leaves_passwordless_sqlite_url_unchanged() {
        assert_eq!(redact_url("sqlite:///tmp/test.db"), "sqlite:///tmp/test.db");
    }

    #[test]
    fn leaves_no_userinfo_unchanged() {
        assert_eq!(
            redact_url("postgres://localhost/db"),
            "postgres://localhost/db"
        );
    }

    #[test]
    fn leaves_userinfo_without_password_unchanged() {
        assert_eq!(
            redact_url("postgres://alice@localhost/db"),
            "postgres://alice@localhost/db"
        );
    }

    #[test]
    fn leaves_non_url_string_unchanged() {
        assert_eq!(redact_url("not-a-url"), "not-a-url");
    }

    #[test]
    fn contextualize_non_database_error_wraps_unchanged() {
        let err = sqlx::Error::PoolTimedOut;
        match contextualize_sqlx_error(err) {
            Error::Database(_) => {}
            other => panic!("expected Database, got {:?}", other),
        }
    }
}
