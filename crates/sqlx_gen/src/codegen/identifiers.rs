use crate::cli::DatabaseKind;

/// SQL keywords reserved across at least one of Postgres / MySQL / SQLite.
/// Sorted so we can binary-search in [`is_reserved_keyword`]. Conservative
/// list — when in doubt, an identifier matching one of these gets quoted.
const SQL_RESERVED: &[&str] = &[
    "ABORT",
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "AUTHORIZATION",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BOTH",
    "BY",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "COMMIT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEFAULT",
    "DEFERRABLE",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DO",
    "DROP",
    "ELSE",
    "END",
    "EXCEPT",
    "EXISTS",
    "FALSE",
    "FETCH",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "GRANT",
    "GROUP",
    "HAVING",
    "IF",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LATERAL",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NOT",
    "NULL",
    "OF",
    "OFFSET",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "OUTER",
    "OVERLAPS",
    "PLACING",
    "PRIMARY",
    "REFERENCES",
    "RETURNING",
    "RIGHT",
    "ROLLBACK",
    "SCHEMA",
    "SELECT",
    "SESSION_USER",
    "SET",
    "SIMILAR",
    "SOME",
    "SYMMETRIC",
    "TABLE",
    "THEN",
    "TO",
    "TRAILING",
    "TRIGGER",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USER",
    "USING",
    "VALUES",
    "VARIADIC",
    "VIEW",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

/// Case-insensitive reserved-word lookup. The list is sorted, so binary_search
/// keeps this O(log n).
fn is_reserved_keyword(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    SQL_RESERVED.binary_search(&upper.as_str()).is_ok()
}

/// True when `name` is safe to emit unquoted in SQL for the given dialect.
///
/// "Safe" means: starts with a letter or underscore (lowercase only — PG
/// folds unquoted idents to lowercase, so uppercase letters force a quote),
/// then ASCII letters / digits / underscores, and is not a reserved keyword.
pub fn is_safe_unquoted(name: &str, _db: DatabaseKind) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = name.as_bytes();
    let first = bytes[0];
    let first_ok = first == b'_' || first.is_ascii_lowercase();
    if !first_ok {
        return false;
    }
    for &b in &bytes[1..] {
        let ok = b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit();
        if !ok {
            return false;
        }
    }
    !is_reserved_keyword(name)
}

/// Quote a SQL identifier (table/column/schema) per database dialect, but
/// only when quoting is syntactically required. Trivially-safe identifiers
/// pass through untouched.
pub fn quote_ident(name: &str, db: DatabaseKind) -> String {
    if is_safe_unquoted(name, db) {
        name.to_string()
    } else {
        quote_ident_always(name, db)
    }
}

/// Always quote, regardless of safety. Doubles internal quote characters.
pub fn quote_ident_always(name: &str, db: DatabaseKind) -> String {
    match db {
        DatabaseKind::Mysql => format!("`{}`", name.replace('`', "``")),
        DatabaseKind::Postgres | DatabaseKind::Sqlite => {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }
}

/// Quote a qualified table reference (`schema.table`) per dialect, or the
/// bare table when no schema is provided. Each part is conditionally quoted.
pub fn quote_qualified(schema: Option<&str>, table: &str, db: DatabaseKind) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s, db), quote_ident(table, db)),
        None => quote_ident(table, db),
    }
}

/// True if `name` is a safe SQL identifier candidate (alphanumeric + underscore,
/// non-empty, does not start with a digit). Loosely the same predicate as
/// [`is_safe_unquoted`] minus the case sensitivity and reserved-word check —
/// kept as a separate helper because it's a generic "could this be a safe
/// identifier" question used by filename validation.
pub fn is_safe_ident(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- conditional quote_ident ----------

    #[test]
    fn safe_identifier_pg_not_quoted() {
        assert_eq!(quote_ident("users", DatabaseKind::Postgres), "users");
        assert_eq!(quote_ident("agent_id", DatabaseKind::Postgres), "agent_id");
        assert_eq!(
            quote_ident("agent__connector", DatabaseKind::Postgres),
            "agent__connector"
        );
    }

    #[test]
    fn safe_identifier_mysql_not_quoted() {
        assert_eq!(quote_ident("users", DatabaseKind::Mysql), "users");
    }

    #[test]
    fn uppercase_identifier_pg_quoted() {
        // PG folds unquoted identifiers to lowercase; uppercase needs quoting
        // to preserve the case of the actual table name.
        assert_eq!(quote_ident("Users", DatabaseKind::Postgres), "\"Users\"");
    }

    #[test]
    fn reserved_word_quoted_in_pg() {
        assert_eq!(quote_ident("select", DatabaseKind::Postgres), "\"select\"");
        assert_eq!(quote_ident("user", DatabaseKind::Postgres), "\"user\"");
        assert_eq!(quote_ident("order", DatabaseKind::Postgres), "\"order\"");
    }

    #[test]
    fn reserved_word_quoted_in_mysql() {
        assert_eq!(quote_ident("select", DatabaseKind::Mysql), "`select`");
    }

    #[test]
    fn identifier_with_dash_quoted() {
        assert_eq!(
            quote_ident("user-id", DatabaseKind::Postgres),
            "\"user-id\""
        );
    }

    #[test]
    fn identifier_starting_with_digit_quoted() {
        assert_eq!(quote_ident("123abc", DatabaseKind::Postgres), "\"123abc\"");
    }

    #[test]
    fn empty_identifier_quoted() {
        // Empty isn't valid SQL but we don't want to drop the quotes and emit
        // bare whitespace either.
        assert_eq!(quote_ident("", DatabaseKind::Postgres), "\"\"");
    }

    #[test]
    fn injection_attempt_quoted_and_escaped() {
        assert_eq!(
            quote_ident("user\"; DROP TABLE x; --", DatabaseKind::Postgres),
            "\"user\"\"; DROP TABLE x; --\""
        );
    }

    // ---------- quote_ident_always ----------

    #[test]
    fn always_quote_safe_identifier_pg() {
        assert_eq!(
            quote_ident_always("users", DatabaseKind::Postgres),
            "\"users\""
        );
    }

    #[test]
    fn always_quote_safe_identifier_mysql() {
        assert_eq!(quote_ident_always("users", DatabaseKind::Mysql), "`users`");
    }

    #[test]
    fn always_quote_escapes_internal_backtick() {
        assert_eq!(quote_ident_always("ev`il", DatabaseKind::Mysql), "`ev``il`");
    }

    // ---------- quote_qualified ----------

    #[test]
    fn qualified_safe_idents_not_quoted() {
        assert_eq!(
            quote_qualified(Some("agent"), "agent_connector", DatabaseKind::Postgres),
            "agent.agent_connector"
        );
        assert_eq!(
            quote_qualified(Some("app"), "users", DatabaseKind::Mysql),
            "app.users"
        );
    }

    #[test]
    fn qualified_with_reserved_schema_quoted() {
        assert_eq!(
            quote_qualified(Some("user"), "items", DatabaseKind::Postgres),
            "\"user\".items"
        );
    }

    #[test]
    fn qualified_without_schema() {
        assert_eq!(quote_qualified(None, "users", DatabaseKind::Mysql), "users");
    }

    // ---------- is_safe_ident (filename helper) ----------

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
    fn safe_ident_accepts_underscore_prefix() {
        assert!(is_safe_ident("_private"));
    }

    #[test]
    fn safe_ident_accepts_mixed_case() {
        assert!(is_safe_ident("UserAccount2"));
    }

    // ---------- reserved list sanity ----------

    #[test]
    fn reserved_list_is_sorted() {
        for pair in SQL_RESERVED.windows(2) {
            assert!(
                pair[0] < pair[1],
                "SQL_RESERVED must be sorted; '{}' >= '{}'",
                pair[0],
                pair[1]
            );
        }
    }
}
