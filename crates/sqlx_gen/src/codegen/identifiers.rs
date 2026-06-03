use crate::cli::DatabaseKind;

/// Quote a SQL identifier (table/column/schema) per database dialect.
/// Doubles any internal quote characters for safety.
pub fn quote_ident(name: &str, db: DatabaseKind) -> String {
    match db {
        DatabaseKind::Mysql => format!("`{}`", name.replace('`', "``")),
        DatabaseKind::Postgres | DatabaseKind::Sqlite => {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }
}

/// Quote a qualified table reference (`schema.table`) per dialect, or the
/// bare table when no schema is provided.
pub fn quote_qualified(schema: Option<&str>, table: &str, db: DatabaseKind) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s, db), quote_ident(table, db)),
        None => quote_ident(table, db),
    }
}

/// True if `name` is a safe SQL identifier candidate (alphanumeric + underscore,
/// non-empty, does not start with a digit).
pub fn is_safe_ident(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_postgres_double_quote() {
        assert_eq!(quote_ident("users", DatabaseKind::Postgres), "\"users\"");
    }

    #[test]
    fn quotes_sqlite_double_quote() {
        assert_eq!(quote_ident("users", DatabaseKind::Sqlite), "\"users\"");
    }

    #[test]
    fn quotes_mysql_backtick() {
        assert_eq!(quote_ident("users", DatabaseKind::Mysql), "`users`");
    }

    #[test]
    fn escapes_postgres_internal_quote() {
        assert_eq!(
            quote_ident("user\"; DROP TABLE x; --", DatabaseKind::Postgres),
            "\"user\"\"; DROP TABLE x; --\""
        );
    }

    #[test]
    fn escapes_mysql_internal_backtick() {
        assert_eq!(quote_ident("ev`il", DatabaseKind::Mysql), "`ev``il`");
    }

    #[test]
    fn qualified_with_schema_postgres() {
        assert_eq!(
            quote_qualified(Some("auth"), "users", DatabaseKind::Postgres),
            "\"auth\".\"users\""
        );
    }

    #[test]
    fn qualified_with_schema_mysql() {
        assert_eq!(
            quote_qualified(Some("app"), "users", DatabaseKind::Mysql),
            "`app`.`users`"
        );
    }

    #[test]
    fn qualified_without_schema() {
        assert_eq!(
            quote_qualified(None, "users", DatabaseKind::Mysql),
            "`users`"
        );
    }

    #[test]
    fn safe_ident_rejects_dash() {
        assert!(!is_safe_ident("user-id"));
    }

    #[test]
    fn safe_ident_rejects_leading_digit() {
        assert!(!is_safe_ident("123abc"));
    }

    #[test]
    fn safe_ident_rejects_empty() {
        assert!(!is_safe_ident(""));
    }

    #[test]
    fn safe_ident_rejects_space() {
        assert!(!is_safe_ident("user id"));
    }

    #[test]
    fn safe_ident_accepts_underscore_prefix() {
        assert!(is_safe_ident("_private"));
    }

    #[test]
    fn safe_ident_accepts_mixed_case() {
        assert!(is_safe_ident("UserAccount2"));
    }
}
