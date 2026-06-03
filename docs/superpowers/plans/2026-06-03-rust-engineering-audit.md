# Audit d'ingénierie Rust — sqlx-gen — Plan de remédiation

> **Pour les agents exécutants :** SOUS-COMPÉTENCE REQUISE : utiliser `superpowers:subagent-driven-development` (recommandé) ou `superpowers:executing-plans` pour implémenter ce plan tâche par tâche. Les étapes utilisent la syntaxe checkbox (`- [ ]`) pour le suivi.

**Objectif :** Corriger les vulnérabilités, panics, incohérences de codegen et lacunes de tests identifiés par un audit multi-domaines de la codebase `sqlx-gen` (v0.5.5).

**Architecture :** Audit conduit par 4 agents experts (sécurité, codegen/typemap, error handling, tests). Findings synthétisés en 4 vagues de remédiation (P0 → P3) avec TDD strict et commits fréquents.

**Tech Stack :** Rust 2021, sqlx 0.8, tokio 1, clap 4, syn 2, quote 1, prettyplease 0.2, thiserror 2.

---

## Vue d'ensemble des findings

| Domaine         | Critique | Haut   | Moyen  | Bas    |
| --------------- | -------- | ------ | ------ | ------ |
| Sécurité        | 4        | 4      | 3      | 0      |
| Codegen/typemap | 3        | 9      | 15     | 4      |
| Error handling  | 2        | 6      | 11     | 6      |
| Tests/CI        | 3        | 4      | 2      | 2      |
| **TOTAL**       | **12**   | **23** | **31** | **12** |

### Findings P0 (bloquants)

1. **SQL injection via identifiants non-quotés** dans le code CRUD généré (`crud_gen.rs:23-26, 73, 99-105`). Tables/colonnes/schémas interpolés bruts dans `format!()` sans quoting dialectal.
2. **Code injection via `--type-overrides`** (`cli.rs:100-108`) : la valeur est parsée par `TokenStream::parse().unwrap()` sans validation, permettant l'injection de Rust arbitraire dans le code généré.
3. **Fuite de mot de passe** : `sqlx::Error` peut contenir l'URL complète (login:password@host) ; `Error::Database(#[from] sqlx::Error)` la propage telle quelle aux logs.
4. **Proc-macro fait `std::process::exit(1)`** (`codegen/mod.rs:315-319`) si le parsing échoue — tue le build utilisateur sans `compile_error!`.
5. **MySQL UTF-8 panics** (`introspect/mysql.rs:72-78`) : 7 `.expect()` consécutifs sur noms de colonnes/tables. Toute donnée non-UTF8 dans `information_schema` crash.
6. **Aucun CI de tests** : `.github/workflows/publish.yml` seul existe ; `cargo test` n'est jamais exécuté en CI.
7. **Aucun test E2E Postgres/MySQL** : le commit `bacb088 fix: MySQL 8.0 information_schema changes` était non testable et donc non détectable par les tests.
8. **`.last_mut().unwrap()`** dans toutes les boucles d'introspection (5 occurrences) panic sur ResultSet vide.
9. **MySQL INSERT composite PK** (`crud_gen.rs:959-979`) : utilise `LAST_INSERT_ID()` qui ne fonctionne qu'avec un seul PK auto-increment.
10. **Pas d'écritures atomiques** (`writer.rs:73, 87`) : Ctrl-C en cours de génération laisse des fichiers `.rs` corrompus.
11. **SQLite NUMERIC/DECIMAL → f64** (`typemap/sqlite.rs:40-41`) : perte de précision silencieuse.
12. **Identifiants Rust invalides** : colonnes nommées `user-id`, `123`, ou contenant des espaces produisent du Rust qui ne compile pas.

---

## Architecture des fichiers à modifier

| Fichier                                                | Responsabilité dans ce plan                                          |
| ------------------------------------------------------ | -------------------------------------------------------------------- |
| `crates/sqlx_gen/src/codegen/identifiers.rs` (nouveau) | Module de quoting d'identifiants par dialecte + validation.          |
| `crates/sqlx_gen/src/error.rs`                         | Étendre les variants ; ajouter `Url::redact`.                        |
| `crates/sqlx_gen/src/codegen/crud_gen.rs`              | Utiliser `identifiers::quote_ident` pour toute SQL générée.          |
| `crates/sqlx_gen/src/introspect/mysql.rs`              | Remplacer `.expect()` par `.map_err()`, idem `.last_mut().unwrap()`. |
| `crates/sqlx_gen/src/introspect/postgres.rs`           | Idem MySQL.                                                          |
| `crates/sqlx_gen/src/introspect/sqlite.rs`             | Valider les noms avant `PRAGMA`.                                     |
| `crates/sqlx_gen/src/cli.rs`                           | Valider `--type-overrides` via `syn::parse_str::<syn::Type>`.        |
| `crates/sqlx_gen/src/codegen/mod.rs`                   | Supprimer `std::process::exit(1)` ; remonter une `Error`.            |
| `crates/sqlx_gen/src/writer.rs`                        | Écritures atomiques via `tempfile`.                                  |
| `crates/sqlx_gen/src/typemap/sqlite.rs`                | NUMERIC → `Decimal`.                                                 |
| `crates/sqlx_gen/src/typemap/mysql.rs`                 | `BIT(1)` → `bool`.                                                   |
| `crates/sqlx_gen/src/typemap/postgres.rs`              | Ajouter `interval`, range types, `timetz`.                           |
| `crates/sqlx_gen/tests/e2e_postgres.rs` (nouveau)      | E2E avec testcontainers PostgreSQL.                                  |
| `crates/sqlx_gen/tests/e2e_mysql.rs` (nouveau)         | E2E avec testcontainers MySQL.                                       |
| `crates/sqlx_gen/tests/snapshots/` (nouveau)           | Snapshots `insta` du codegen.                                        |
| `.github/workflows/ci.yml` (nouveau)                   | Matrix Postgres+MySQL+SQLite, `cargo test --all`.                    |

---

# VAGUE P0 — Bloquants sécurité & fiabilité

## Task 1 : Module de quoting d'identifiants par dialecte

**Fichiers :**

- Créer : `crates/sqlx_gen/src/codegen/identifiers.rs`
- Modifier : `crates/sqlx_gen/src/codegen/mod.rs` (ajouter `pub mod identifiers;`)

**Pourquoi :** Tout le code SQL généré dans `crud_gen.rs` interpole table/colonne/schéma via `format!("{}", name)` sans quoting. Si une colonne s'appelle `select` ou `user"; DROP TABLE x; --`, le code compilé exécute du SQL malformé voire malveillant. Source de l'audit : finding sécurité #2/#3, codegen #16/#23.

- [ ] **Étape 1 : Écrire les tests qui échouent**

Créer `crates/sqlx_gen/src/codegen/identifiers.rs` :

```rust
use crate::cli::DatabaseKind;

/// Quote a SQL identifier (table/column/schema) per database dialect.
/// Doubles internal quote characters for safety.
pub fn quote_ident(name: &str, db: DatabaseKind) -> String {
    match db {
        DatabaseKind::Mysql => format!("`{}`", name.replace('`', "``")),
        DatabaseKind::Postgres | DatabaseKind::Sqlite => {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }
}

/// Quote a qualified table name (schema.table) per dialect.
pub fn quote_qualified(schema: Option<&str>, table: &str, db: DatabaseKind) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s, db), quote_ident(table, db)),
        None => quote_ident(table, db),
    }
}

/// True if a string is a safe SQL identifier candidate (alphanumeric + underscore).
/// Used as a defense-in-depth check before generating files whose names
/// derive from DB metadata.
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
        assert_eq!(
            quote_ident("ev`il", DatabaseKind::Mysql),
            "`ev``il`"
        );
    }

    #[test]
    fn qualified_with_schema() {
        assert_eq!(
            quote_qualified(Some("auth"), "users", DatabaseKind::Postgres),
            "\"auth\".\"users\""
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
    fn safe_ident_accepts_underscore() {
        assert!(is_safe_ident("_private"));
    }
}
```

- [ ] **Étape 2 : Faire échouer les tests**

Run : `cargo test -p sqlx-gen --lib codegen::identifiers`
Attendu : `error[E0583]: file not found for module identifiers` (avant d'ajouter le `pub mod`).

- [ ] **Étape 3 : Déclarer le module**

Dans `crates/sqlx_gen/src/codegen/mod.rs` (juste après les autres `mod`) :

```rust
pub mod identifiers;
```

- [ ] **Étape 4 : Valider que les tests passent**

Run : `cargo test -p sqlx-gen --lib codegen::identifiers`
Attendu : `10 passed; 0 failed`.

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/codegen/identifiers.rs crates/sqlx_gen/src/codegen/mod.rs
git commit -m "feat(codegen): add SQL identifier quoting module"
```

---

## Task 2 : Appliquer le quoting dans crud_gen.rs

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/crud_gen.rs:23-26, 73, 99-105, 273-275, 305-313, 440-450, 618-629`

**Pourquoi :** Élimine la SQL injection P0 #1. Toute SQL générée doit utiliser `quote_qualified` / `quote_ident`.

- [ ] **Étape 1 : Ajouter un test d'intégration "identifiant malveillant"**

Dans `crates/sqlx_gen/src/codegen/crud_gen.rs` (bloc `#[cfg(test)] mod tests`) :

```rust
#[test]
fn generated_sql_quotes_table_name() {
    use crate::codegen::entity_parser::{ParsedEntity, ParsedField};
    let entity = ParsedEntity {
        struct_name: "Users".into(),
        table_name: "users".into(),
        schema_name: Some("public".into()),
        is_view: false,
        fields: vec![ParsedField {
            field_name: "id".into(),
            column_name: "id".into(),
            rust_type: "i32".into(),
            inner_type: "i32".into(),
            is_optional: false,
            is_primary_key: true,
            column_default: None,
            sql_type: None,
        }],
        imports: vec![],
    };
    let (tokens, _) = generate_crud_from_parsed(
        &entity,
        DatabaseKind::Postgres,
        "crate::models::users",
        &Methods { get_all: true, ..Methods::default() },
        false,
        PoolVisibility::Private,
    );
    let code = tokens.to_string();
    assert!(
        code.contains("\\\"public\\\".\\\"users\\\""),
        "generated code must use quoted qualified identifier, got: {}",
        code
    );
    assert!(
        !code.contains("FROM public.users "),
        "must not contain unquoted table reference"
    );
}
```

- [ ] **Étape 2 : Run pour faire échouer**

Run : `cargo test -p sqlx-gen --lib codegen::crud_gen::tests::generated_sql_quotes_table_name`
Attendu : FAIL — la sortie actuelle contient `FROM public.users` non quoté.

- [ ] **Étape 3 : Implémenter le quoting**

Remplacer dans `crud_gen.rs` ligne 23-26 :

```rust
let table_name = match &entity.schema_name {
    Some(schema) => format!("{}.{}", schema, entity.table_name),
    None => entity.table_name.clone(),
};
```

par :

```rust
use crate::codegen::identifiers::{quote_ident, quote_qualified};

let table_name = quote_qualified(
    entity.schema_name.as_deref(),
    &entity.table_name,
    db_kind,
);
```

Puis remplacer chacune des occurrences `f.column_name` dans les formats SQL (lignes 273-275, 440, 450, 618, 629, 890, 921) par `quote_ident(&f.column_name, db_kind)`. Exemple ligne 273-275 :

```rust
let set_cols: Vec<String> = non_pk_fields
    .iter()
    .enumerate()
    .map(|(i, f)| {
        let p = placeholder(db_kind, i + 1);
        format!("{} = {}", quote_ident(&f.column_name, db_kind), p)
    })
    .collect();
```

Faire la même substitution pour les listes de colonnes d'`INSERT (...)`, `WHERE x = $1`, `RETURNING <cols>` (ne pas quoter `*`).

- [ ] **Étape 4 : Vérifier le passage**

Run : `cargo test -p sqlx-gen --lib codegen::crud_gen`
Attendu : tous passent (les snapshots des tests existants seront probablement à mettre à jour — ils sont des assertions de substring, ajuster en conséquence : remplacer `assert!(code.contains("SELECT * FROM users"))` par `assert!(code.contains("SELECT * FROM \"users\""))` pour Postgres et ``"SELECT * FROM `users`"`` pour MySQL).

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/codegen/crud_gen.rs
git commit -m "fix(codegen): quote SQL identifiers per dialect to prevent injection"
```

---

## Task 3 : Valider les valeurs de `--type-overrides`

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/cli.rs:100-108`

**Pourquoi :** Le finding sécurité #5 montre que `--type-overrides jsonb=evil; fn pwned()` est injecté tel quel dans le code généré via `TokenStream::parse().unwrap()`. Valider le type en amont via `syn`.

- [ ] **Étape 1 : Écrire les tests**

Dans `crates/sqlx_gen/src/cli.rs` (module `tests`) :

```rust
#[test]
fn type_overrides_reject_injection() {
    let args = make_entities_args_with_overrides(vec!["jsonb=Vec<u8>; fn pwned() {}"]);
    let result = args.parse_type_overrides_checked();
    assert!(result.is_err(), "must reject overrides that aren't valid types");
}

#[test]
fn type_overrides_accept_path_type() {
    let args = make_entities_args_with_overrides(vec!["jsonb=crate::types::MyJson"]);
    let map = args.parse_type_overrides_checked().unwrap();
    assert_eq!(map.get("jsonb").unwrap(), "crate::types::MyJson");
}

#[test]
fn type_overrides_accept_generic_type() {
    let args = make_entities_args_with_overrides(vec!["jsonb=Vec<u8>"]);
    assert!(args.parse_type_overrides_checked().is_ok());
}

#[test]
fn type_overrides_reject_empty_value() {
    let args = make_entities_args_with_overrides(vec!["jsonb="]);
    assert!(args.parse_type_overrides_checked().is_err());
}
```

- [ ] **Étape 2 : Faire échouer**

Run : `cargo test -p sqlx-gen --lib cli::tests::type_overrides_reject_injection`
Attendu : FAIL — méthode `parse_type_overrides_checked` n'existe pas.

- [ ] **Étape 3 : Implémenter**

Dans `crates/sqlx_gen/src/cli.rs` (impl `EntitiesArgs`) :

```rust
pub fn parse_type_overrides_checked(&self) -> crate::error::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for s in &self.type_overrides {
        let (k, v) = s.split_once('=').ok_or_else(|| {
            crate::error::Error::Config(format!(
                "Invalid --type-overrides entry '{}'. Expected format: sql_type=RustType",
                s
            ))
        })?;
        if v.trim().is_empty() {
            return Err(crate::error::Error::Config(format!(
                "Empty Rust type in override '{}'",
                s
            )));
        }
        syn::parse_str::<syn::Type>(v).map_err(|e| {
            crate::error::Error::Config(format!(
                "Invalid Rust type in --type-overrides '{}': {}",
                v, e
            ))
        })?;
        map.insert(k.to_string(), v.to_string());
    }
    Ok(map)
}
```

Mettre à jour `main.rs:30` :

```rust
let type_overrides = args.parse_type_overrides_checked()?;
```

- [ ] **Étape 4 : Vérifier**

Run : `cargo test -p sqlx-gen --lib cli::tests::type_overrides`
Attendu : 4 passent.

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/cli.rs crates/sqlx_gen/src/main.rs
git commit -m "fix(cli): validate --type-overrides values via syn::parse_str"
```

---

## Task 4 : Redaction de l'URL DB dans les erreurs

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/error.rs`
- Modifier : `crates/sqlx_gen/src/main.rs:42-58`

**Pourquoi :** Finding sécurité #6 : `sqlx::Error` peut contenir l'URL avec mot de passe. Ajouter une wrapping `Connection(redacted_url, source)` et une fonction utilitaire `redact_url`.

- [ ] **Étape 1 : Tests**

Dans `crates/sqlx_gen/src/error.rs` :

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password_in_postgres_url() {
        let url = "postgres://alice:s3cret@localhost:5432/db";
        assert_eq!(
            redact_url(url),
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
    fn leaves_passwordless_url_unchanged() {
        assert_eq!(
            redact_url("sqlite:///tmp/test.db"),
            "sqlite:///tmp/test.db"
        );
    }

    #[test]
    fn leaves_no_userinfo_unchanged() {
        assert_eq!(
            redact_url("postgres://localhost/db"),
            "postgres://localhost/db"
        );
    }
}
```

- [ ] **Étape 2 : Faire échouer**

Run : `cargo test -p sqlx-gen --lib error`
Attendu : FAIL — `redact_url` n'existe pas.

- [ ] **Étape 3 : Implémenter**

Remplacer le contenu de `crates/sqlx_gen/src/error.rs` par :

```rust
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database connection error ({redacted_url}): {source}")]
    Connection {
        redacted_url: String,
        #[source]
        source: sqlx::Error,
    },

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Redact `user:password@host` → `user:****@host` in a database URL.
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
```

Mettre à jour `main.rs:42-58` pour wrapper en `Connection` :

```rust
let mut schema_info = match db_kind {
    DatabaseKind::Postgres => {
        let pool = PgPool::connect(&args.db.database_url).await.map_err(|e| {
            sqlx_gen::error::Error::Connection {
                redacted_url: sqlx_gen::error::redact_url(&args.db.database_url),
                source: e,
            }
        })?;
        let info = introspect::postgres::introspect(&pool, &args.db.schemas, args.views).await?;
        pool.close().await;
        info
    }
    // ... idem pour Mysql, Sqlite
};
```

- [ ] **Étape 4 : Vérifier**

Run : `cargo test -p sqlx-gen --lib error::tests`
Attendu : 4 passent.

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/error.rs crates/sqlx_gen/src/main.rs
git commit -m "fix(error): redact password in database URLs on connection failure"
```

---

## Task 5 : Remplacer `process::exit(1)` par une erreur propagée

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/mod.rs:310-323`

**Pourquoi :** Finding error #3.1. Un échec de parsing prettyplease tue le process — inacceptable dans une bibliothèque ou un futur build.rs.

- [ ] **Étape 1 : Test**

Dans `crates/sqlx_gen/src/codegen/mod.rs` (module `tests`) :

```rust
#[test]
fn parse_and_format_returns_error_on_invalid_tokens() {
    use proc_macro2::TokenStream;
    use std::str::FromStr;
    // Mismatched braces produce a valid TokenStream but invalid syn::File.
    let bad = TokenStream::from_str("fn x() { ").unwrap();
    let result = parse_and_format_with_tab_spaces(&bad, 4);
    assert!(result.is_err(), "must return Err, not exit");
}
```

- [ ] **Étape 2 : Faire échouer**

Run : `cargo test -p sqlx-gen --lib codegen::tests::parse_and_format_returns_error_on_invalid_tokens`
Attendu : FAIL — la fonction renvoie `String`, pas `Result`.

- [ ] **Étape 3 : Implémenter**

Modifier `crates/sqlx_gen/src/codegen/mod.rs:310-323` :

```rust
pub(crate) fn parse_and_format(tokens: &TokenStream) -> crate::error::Result<String> {
    parse_and_format_with_tab_spaces(tokens, 4)
}

pub(crate) fn parse_and_format_with_tab_spaces(
    tokens: &TokenStream,
    tab_spaces: usize,
) -> crate::error::Result<String> {
    let file = syn::parse2::<syn::File>(tokens.clone()).map_err(|e| {
        crate::error::Error::Config(format!(
            "Internal sqlx-gen bug: failed to parse generated code: {}. \
             Please report this with the input schema.",
            e
        ))
    })?;
    let raw = prettyplease::unparse(&file);
    let raw = indent_multiline_raw_strings(&raw, tab_spaces);
    Ok(add_blank_lines_between_items(&raw))
}
```

Propager `?` dans tous les sites d'appel : `format_tokens`, `format_tokens_with_imports`, `format_tokens_with_imports_and_tab_spaces`, et les retours dans `generate()`. Ajuster les signatures publiques pour renvoyer `Result<String>` au lieu de `String`.

- [ ] **Étape 4 : Vérifier**

Run : `cargo build -p sqlx-gen && cargo test -p sqlx-gen --lib codegen`
Attendu : compile et tous les tests passent (les sites d'appel mis à jour).

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/codegen/mod.rs crates/sqlx_gen/src/main.rs
git commit -m "fix(codegen): propagate parse errors instead of process::exit"
```

---

## Task 6 : Bannir les `.expect()` et `.last_mut().unwrap()` dans introspect

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/introspect/mysql.rs:72-78, 89, 146`
- Modifier : `crates/sqlx_gen/src/introspect/postgres.rs:355, 405, 466`

**Pourquoi :** Finding error #1.1–#1.5. Toute donnée DB inattendue (utf-8, ordre des rows) crash.

- [ ] **Étape 1 : Test pour `last_mut`**

Dans `crates/sqlx_gen/src/introspect/mysql.rs` (module `tests` à créer si absent) :

```rust
#[cfg(test)]
mod tests {
    use crate::error::Error;

    fn invariant_violation(field: &str) -> Error {
        Error::Config(format!(
            "Internal introspection invariant violated: {} accessed empty tables vector. \
             This is a bug in sqlx-gen.",
            field
        ))
    }

    #[test]
    fn invariant_violation_message_mentions_field() {
        let err = invariant_violation("columns");
        assert!(err.to_string().contains("columns"));
    }
}
```

- [ ] **Étape 2 : Faire passer (run test)**

Run : `cargo test -p sqlx-gen --lib introspect::mysql::tests::invariant_violation_message_mentions_field`
Attendu : PASS après ajout — placeholder pour la fonction utilitaire.

- [ ] **Étape 3 : Remplacer les `.expect()` UTF-8 et les `.last_mut().unwrap()`**

Dans `crates/sqlx_gen/src/introspect/mysql.rs:62`, changer le type de `query_as` pour prendre des `String` plutôt que `Vec<u8>` quand c'est possible :

```rust
let mut q = sqlx::query_as::<_, (String, String, String, String, String, String, u32, String)>(&query);
```

Si MySQL réclame `Vec<u8>` (cas mediumtext en collation binary), remplacer chaque `.expect("Could not convert ...")` par :

```rust
let schema = String::from_utf8(schema).map_err(|_| crate::error::Error::Config(
    "Database returned non-UTF8 schema name; sqlx-gen requires UTF-8 metadata".into()
))?;
```

Pour les `last_mut().unwrap()` (lignes 89, 146 mysql, 355, 405 pg) remplacer par :

```rust
match tables.last_mut() {
    Some(t) => t.columns.push(column),
    None => return Err(crate::error::Error::Config(
        "Internal invariant: row returned for non-existent table. Bug in sqlx-gen.".into()
    )),
}
```

- [ ] **Étape 4 : Vérifier**

Run : `cargo build -p sqlx-gen && cargo test -p sqlx-gen --lib introspect`
Attendu : compile, tests passent.

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/introspect/
git commit -m "fix(introspect): replace expect/unwrap with proper Result propagation"
```

---

## Task 7 : Écritures atomiques

**Fichiers :**

- Modifier : `crates/sqlx_gen/Cargo.toml` (déplacer `tempfile` de `dev-dependencies` vers `dependencies`, gated par feature `cli`)
- Modifier : `crates/sqlx_gen/src/writer.rs:60-90, 167-200`
- Modifier : `crates/sqlx_gen/src/main.rs:167`

**Pourquoi :** Finding error #10.1/#10.2. `std::fs::write` non-atomique : Ctrl-C laisse des fichiers tronqués qui cassent le build du projet utilisateur.

- [ ] **Étape 1 : Ajouter tempfile en dépendance runtime**

Dans `crates/sqlx_gen/Cargo.toml`, ajouter dans `[dependencies]` :

```toml
tempfile = { version = "3", optional = true }
```

Et l'ajouter à la feature `cli` :

```toml
cli = [
    # ... existing
    "dep:tempfile",
]
```

- [ ] **Étape 2 : Test**

Dans `crates/sqlx_gen/src/writer.rs` (module `tests`) :

```rust
#[test]
fn write_atomic_creates_file_with_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.rs");
    write_atomic(&path, b"hello").unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");
}

#[test]
fn write_atomic_overwrites_existing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.rs");
    std::fs::write(&path, "old").unwrap();
    write_atomic(&path, b"new").unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
}
```

- [ ] **Étape 3 : Faire échouer**

Run : `cargo test -p sqlx-gen --lib writer::tests::write_atomic_creates_file_with_content`
Attendu : FAIL — fonction inexistante.

- [ ] **Étape 4 : Implémenter**

Dans `crates/sqlx_gen/src/writer.rs` :

```rust
/// Write `content` to `path` atomically: write to a sibling temp file then rename.
pub(crate) fn write_atomic(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        crate::error::Error::Config(format!("Cannot determine parent of {}", path.display()))
    })?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    tmp.write_all(content)?;
    tmp.flush()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}
```

Remplacer chaque `std::fs::write(&path, &content)?;` dans `writer.rs` et `main.rs:167` par `write_atomic(&path, content.as_bytes())?;`.

- [ ] **Étape 5 : Vérifier**

Run : `cargo test -p sqlx-gen --lib writer`
Attendu : tests passent.

- [ ] **Étape 6 : Commit**

```bash
git add crates/sqlx_gen/Cargo.toml crates/sqlx_gen/src/writer.rs crates/sqlx_gen/src/main.rs
git commit -m "fix(writer): use atomic temp-file + rename to prevent corrupted output"
```

---

## Task 8 : Validation path traversal pour les noms de fichier générés

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/mod.rs` (fonction `normalize_module_name` ou similaire)
- Modifier : `crates/sqlx_gen/src/writer.rs:70-78`

**Pourquoi :** Finding sécurité #7 : si une DB malveillante retourne un nom de table `../../etc/passwd`, le filename généré peut sortir de `output_dir`.

- [ ] **Étape 1 : Tests**

Dans `crates/sqlx_gen/src/writer.rs` (module `tests`) :

```rust
#[test]
fn rejects_path_traversal_in_filename() {
    let dir = tempfile::tempdir().unwrap();
    let files = vec![GeneratedFile {
        filename: "../escape.rs".to_string(),
        code: "fn x() {}".into(),
        origin: None,
    }];
    let result = write_files(&files, dir.path(), false, false);
    assert!(result.is_err(), "must reject filename containing ..");
}

#[test]
fn rejects_absolute_path_in_filename() {
    let dir = tempfile::tempdir().unwrap();
    let files = vec![GeneratedFile {
        filename: "/etc/passwd".to_string(),
        code: "".into(),
        origin: None,
    }];
    let result = write_files(&files, dir.path(), false, false);
    assert!(result.is_err());
}
```

- [ ] **Étape 2 : Faire échouer**

Run : `cargo test -p sqlx-gen --lib writer::tests::rejects_path_traversal_in_filename`
Attendu : FAIL — actuellement l'écriture aboutit.

- [ ] **Étape 3 : Implémenter**

Dans `crates/sqlx_gen/src/writer.rs`, ajouter en début de `write_multi_files` :

```rust
for f in files {
    let candidate = std::path::Path::new(&f.filename);
    if candidate.components().count() != 1
        || candidate.is_absolute()
        || f.filename.contains("..")
        || !f.filename.ends_with(".rs")
    {
        return Err(crate::error::Error::Config(format!(
            "Refusing to write generated file with unsafe name: {:?}",
            f.filename
        )));
    }
}
```

- [ ] **Étape 4 : Vérifier**

Run : `cargo test -p sqlx-gen --lib writer`
Attendu : passent.

- [ ] **Étape 5 : Commit**

```bash
git add crates/sqlx_gen/src/writer.rs
git commit -m "fix(writer): refuse path-traversal in generated filenames"
```

---

## Task 9 : CI — workflow `test.yml` avec services Postgres + MySQL

**Fichiers :**

- Créer : `.github/workflows/ci.yml`

**Pourquoi :** Finding tests P0 #2. Aucun CI ne lance `cargo test` ; chaque PR mergé est inspecté à la main.

- [ ] **Étape 1 : Créer le fichier**

`.github/workflows/ci.yml` :

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: sqlx_gen_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 10
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: root
          MYSQL_DATABASE: sqlx_gen_test
        ports:
          - 3306:3306
        options: >-
          --health-cmd "mysqladmin ping -uroot -proot"
          --health-interval 5s
          --health-timeout 3s
          --health-retries 10
    env:
      PG_URL: postgres://postgres:postgres@localhost:5432/sqlx_gen_test
      MYSQL_URL: mysql://root:root@localhost:3306/sqlx_gen_test
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - name: Format check
        run: cargo fmt --all -- --check
      - name: Clippy
        run: cargo clippy --all-targets -- -D warnings
      - name: Test
        run: cargo test --all
```

- [ ] **Étape 2 : Commit et pousser**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add test workflow with Postgres + MySQL services"
```

Vérifier qu'un push déclenche le workflow et qu'il passe (les jobs Postgres/MySQL n'auront pas de tests E2E avant Task 12, mais `cargo test` doit verdir).

---

## Task 10 : Snapshot tests `insta` pour le codegen

**Fichiers :**

- Modifier : `crates/sqlx_gen/Cargo.toml` (ajouter `insta` en dev-dep)
- Créer : `crates/sqlx_gen/tests/snapshots_codegen.rs`
- Créer : `crates/sqlx_gen/tests/snapshots/` (sera peuplé par `cargo insta`)

**Pourquoi :** Finding tests P1 #4. Aucun snapshot test ⇒ toute régression silencieuse de format ou de derive passe inaperçue.

- [ ] **Étape 1 : Ajouter `insta`**

Dans `crates/sqlx_gen/Cargo.toml` :

```toml
[dev-dependencies]
insta = "1"
```

- [ ] **Étape 2 : Créer un test snapshot**

`crates/sqlx_gen/tests/snapshots_codegen.rs` :

```rust
use sqlx_gen::cli::{DatabaseKind, Methods, PoolVisibility};
use sqlx_gen::codegen::crud_gen::generate_crud_from_parsed;
use sqlx_gen::codegen::entity_parser::{ParsedEntity, ParsedField};
use sqlx_gen::codegen::format_tokens_with_imports;

fn sample_users() -> ParsedEntity {
    ParsedEntity {
        struct_name: "Users".into(),
        table_name: "users".into(),
        schema_name: Some("public".into()),
        is_view: false,
        fields: vec![
            ParsedField {
                field_name: "id".into(),
                column_name: "id".into(),
                rust_type: "i32".into(),
                inner_type: "i32".into(),
                is_optional: false,
                is_primary_key: true,
                column_default: None,
                sql_type: None,
            },
            ParsedField {
                field_name: "email".into(),
                column_name: "email".into(),
                rust_type: "String".into(),
                inner_type: "String".into(),
                is_optional: false,
                is_primary_key: false,
                column_default: None,
                sql_type: None,
            },
        ],
        imports: vec![],
    }
}

#[test]
fn snapshot_postgres_full_crud_users() {
    let (tokens, imports) = generate_crud_from_parsed(
        &sample_users(),
        DatabaseKind::Postgres,
        "crate::models::users",
        &Methods::all(),
        false,
        PoolVisibility::Private,
    );
    let code = format_tokens_with_imports(&tokens, &imports).expect("format");
    insta::assert_snapshot!("postgres_full_crud_users", code);
}

#[test]
fn snapshot_mysql_full_crud_users() {
    let (tokens, imports) = generate_crud_from_parsed(
        &sample_users(),
        DatabaseKind::Mysql,
        "crate::models::users",
        &Methods::all(),
        false,
        PoolVisibility::Private,
    );
    let code = format_tokens_with_imports(&tokens, &imports).expect("format");
    insta::assert_snapshot!("mysql_full_crud_users", code);
}

#[test]
fn snapshot_sqlite_full_crud_users() {
    let (tokens, imports) = generate_crud_from_parsed(
        &sample_users(),
        DatabaseKind::Sqlite,
        "crate::models::users",
        &Methods::all(),
        false,
        PoolVisibility::Private,
    );
    let code = format_tokens_with_imports(&tokens, &imports).expect("format");
    insta::assert_snapshot!("sqlite_full_crud_users", code);
}
```

- [ ] **Étape 3 : Générer et valider les snapshots**

```bash
cargo install cargo-insta
cargo test -p sqlx-gen --test snapshots_codegen
cargo insta review
```

L'humain valide le contenu des 3 snapshots avant de commiter.

- [ ] **Étape 4 : Commit**

```bash
git add crates/sqlx_gen/Cargo.toml crates/sqlx_gen/tests/snapshots_codegen.rs crates/sqlx_gen/tests/snapshots/
git commit -m "test: add insta snapshot tests for CRUD codegen across 3 dialects"
```

---

# VAGUE P1 — Robustesse codegen & couverture E2E

## Task 11 : E2E PostgreSQL via testcontainers

**Fichiers :**

- Modifier : `crates/sqlx_gen/Cargo.toml` (dev-dep `testcontainers`)
- Créer : `crates/sqlx_gen/tests/e2e_postgres.rs`

**Pourquoi :** Finding tests P0 #1. Le commit `bacb088 fix MySQL 8.0 information_schema` n'avait aucun test E2E ; toute régression similaire passe.

- [ ] **Étape 1 : Ajouter testcontainers + sqlx**

```toml
[dev-dependencies]
testcontainers = "0.20"
testcontainers-modules = { version = "0.8", features = ["postgres"] }
tokio = { version = "1", features = ["full"] }
```

- [ ] **Étape 2 : Écrire l'e2e**

`crates/sqlx_gen/tests/e2e_postgres.rs` (squelette ; chaque test doit valider que `cargo build` du code généré marche) :

```rust
use sqlx::PgPool;
use sqlx_gen::introspect::postgres::introspect;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

async fn setup() -> (testcontainers::ContainerAsync<Postgres>, PgPool) {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPool::connect(&url).await.unwrap();
    (container, pool)
}

#[tokio::test]
async fn introspects_table_with_enum_and_jsonb() {
    let (_c, pool) = setup().await;
    sqlx::query("CREATE TYPE status AS ENUM ('active', 'inactive')")
        .execute(&pool).await.unwrap();
    sqlx::query(r#"
        CREATE TABLE users (
            id UUID PRIMARY KEY,
            email TEXT NOT NULL,
            status status NOT NULL,
            meta JSONB
        )
    "#).execute(&pool).await.unwrap();

    let info = introspect(&pool, &["public".into()], false).await.unwrap();
    assert_eq!(info.tables.len(), 1);
    assert_eq!(info.tables[0].columns.len(), 4);
    assert_eq!(info.enums.len(), 1);
    assert_eq!(info.enums[0].variants, vec!["active", "inactive"]);
}

#[tokio::test]
async fn introspects_composite_pk() {
    let (_c, pool) = setup().await;
    sqlx::query(r#"
        CREATE TABLE order_items (
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            qty INT NOT NULL,
            PRIMARY KEY (order_id, product_id)
        )
    "#).execute(&pool).await.unwrap();
    let info = introspect(&pool, &["public".into()], false).await.unwrap();
    let pk_cols: Vec<_> = info.tables[0].columns.iter()
        .filter(|c| c.is_primary_key).collect();
    assert_eq!(pk_cols.len(), 2);
}

#[tokio::test]
async fn introspects_view_inherits_nullability() {
    let (_c, pool) = setup().await;
    sqlx::query("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE VIEW v AS SELECT id, name FROM t")
        .execute(&pool).await.unwrap();
    let info = introspect(&pool, &["public".into()], true).await.unwrap();
    assert_eq!(info.views.len(), 1);
    assert!(!info.views[0].columns.iter().find(|c| c.name == "name").unwrap().is_nullable);
}

#[tokio::test]
async fn rejects_table_with_reserved_keyword_column() {
    let (_c, pool) = setup().await;
    sqlx::query(r#"CREATE TABLE t (id INT PRIMARY KEY, "type" TEXT)"#)
        .execute(&pool).await.unwrap();
    let info = introspect(&pool, &["public".into()], false).await.unwrap();
    // Codegen must produce compileable Rust for keyword columns.
    let files = sqlx_gen::codegen::generate(
        &info, sqlx_gen::cli::DatabaseKind::Postgres,
        &[], &Default::default(), false, sqlx_gen::cli::TimeCrate::Chrono,
    );
    for f in files {
        syn::parse_file(&f.code).expect("generated code must parse");
    }
}
```

- [ ] **Étape 3 : Faire passer**

Run : `cargo test -p sqlx-gen --test e2e_postgres`
Attendu : 4 passent (sinon corriger les bugs révélés en suivant le pattern systematic-debugging).

- [ ] **Étape 4 : Commit**

```bash
git add crates/sqlx_gen/Cargo.toml crates/sqlx_gen/tests/e2e_postgres.rs
git commit -m "test(e2e): add Postgres E2E tests via testcontainers"
```

---

## Task 12 : E2E MySQL via testcontainers (régression `bacb088`)

**Fichiers :**

- Créer : `crates/sqlx_gen/tests/e2e_mysql.rs`

**Pourquoi :** Re-tester explicitement le scénario information_schema MySQL 8.0 corrigé par `bacb088` afin de prévenir toute régression.

- [ ] **Étape 1 : Tests**

```rust
use sqlx::MySqlPool;
use sqlx_gen::introspect::mysql::introspect;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

async fn setup() -> (testcontainers::ContainerAsync<Mysql>, MySqlPool, String) {
    let container = Mysql::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", port);
    let pool = MySqlPool::connect(&url).await.unwrap();
    sqlx::query("CREATE DATABASE IF NOT EXISTS test").execute(&pool).await.ok();
    (container, pool, "test".to_string())
}

#[tokio::test]
async fn introspects_mysql8_information_schema_charset_change() {
    let (_c, pool, db) = setup().await;
    sqlx::query("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, email VARCHAR(255))")
        .execute(&pool).await.unwrap();
    let info = introspect(&pool, &[db], false).await.unwrap();
    assert_eq!(info.tables.len(), 1);
    assert_eq!(info.tables[0].columns.len(), 2);
}

#[tokio::test]
async fn introspects_mysql_inline_enum() {
    let (_c, pool, db) = setup().await;
    sqlx::query("CREATE TABLE t (id INT PRIMARY KEY, status ENUM('a', 'b'))")
        .execute(&pool).await.unwrap();
    let info = introspect(&pool, &[db], false).await.unwrap();
    assert!(!info.enums.is_empty());
}

#[tokio::test]
async fn introspects_mysql_tinyint1_as_bool() {
    let (_c, pool, db) = setup().await;
    sqlx::query("CREATE TABLE t (id INT PRIMARY KEY, active TINYINT(1) NOT NULL)")
        .execute(&pool).await.unwrap();
    let info = introspect(&pool, &[db], false).await.unwrap();
    let files = sqlx_gen::codegen::generate(
        &info, sqlx_gen::cli::DatabaseKind::Mysql,
        &[], &Default::default(), false, sqlx_gen::cli::TimeCrate::Chrono,
    );
    let code = files.iter().find(|f| f.filename.contains("t")).unwrap().code.clone();
    assert!(code.contains("pub active: bool"), "tinyint(1) must map to bool, got: {}", code);
}
```

- [ ] **Étape 2 : Run et corriger**

Run : `cargo test -p sqlx-gen --test e2e_mysql`

- [ ] **Étape 3 : Commit**

```bash
git add crates/sqlx_gen/tests/e2e_mysql.rs
git commit -m "test(e2e): add MySQL E2E coverage including info_schema 8.0 regression"
```

---

## Task 13 : `MySQL BIT(1) → bool` et autres trous de typemap

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/typemap/mysql.rs:79`
- Modifier : `crates/sqlx_gen/src/typemap/postgres.rs`

**Pourquoi :** Findings codegen #6, #7, #8.

- [ ] **Étape 1 : Tests**

Dans `crates/sqlx_gen/src/typemap/mysql.rs` (tests) :

```rust
#[test]
fn bit_one_is_bool() {
    let t = map("bit", "bit(1)", false);
    assert_eq!(t.name, "bool");
}

#[test]
fn bit_n_is_vec_u8() {
    let t = map("bit", "bit(8)", false);
    assert_eq!(t.name, "Vec<u8>");
}
```

Dans `typemap/postgres.rs` :

```rust
#[test]
fn interval_uses_pginterval() {
    let t = map("interval", "interval", false);
    assert_eq!(t.name, "PgInterval");
}

#[test]
fn timetz_warns_or_uses_fixed_offset() {
    let t = map("time with time zone", "timetz", false);
    assert!(t.name.contains("FixedOffset") || t.name == "String",
        "timetz currently maps to {}; should not silently drop timezone", t.name);
}
```

- [ ] **Étape 2 : Faire échouer**

Run les tests, observer FAIL.

- [ ] **Étape 3 : Implémenter**

MySQL `typemap/mysql.rs:79` : ajouter avant le mapping bit générique :

```rust
if udt_name.eq_ignore_ascii_case("bit(1)") {
    return RustType::simple("bool");
}
```

Postgres `typemap/postgres.rs` : ajouter `"interval" => RustType::with_import("PgInterval", "use sqlx::postgres::types::PgInterval;")` dans la fonction `map`.

- [ ] **Étape 4 : Vérifier + Commit**

```bash
cargo test -p sqlx-gen --lib typemap
git add crates/sqlx_gen/src/typemap/
git commit -m "fix(typemap): MySQL BIT(1)→bool, Postgres interval→PgInterval"
```

---

## Task 14 : Validation des noms de colonne (caractères spéciaux)

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/struct_gen.rs:55-68`

**Pourquoi :** Finding codegen #16/#17. Une colonne `user-id` produit `pub user-id: i32;` = invalide.

- [ ] **Étape 1 : Test**

```rust
#[test]
fn column_with_dash_is_sanitized_or_errors() {
    let col = ColumnInfo {
        name: "user-id".into(),
        // ... rest
    };
    let result = generate_struct_field(&col, /* params */);
    assert!(
        result.is_err() || result.unwrap().contains("user_id"),
        "must sanitize or error on column 'user-id'"
    );
}
```

- [ ] **Étape 2 : Implémenter**

Dans `struct_gen.rs`, transformer `to_snake_case` puis remplacer tout char `!is_ascii_alphanumeric && != '_'` par `_`. Préfixer par `_` si le résultat commence par un chiffre. Si vide, retourner `Err(Error::Config("Column name empty"))`.

- [ ] **Étape 3 : Commit**

```bash
git commit -am "fix(codegen): sanitize column names with non-ident characters"
```

---

## Task 15 : `SQLite NUMERIC/DECIMAL → Decimal` (cohérence cross-backend)

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/typemap/sqlite.rs:40-41`

**Pourquoi :** Finding codegen #1. Perte de précision silencieuse pour les montants financiers.

- [ ] **Étape 1 : Test**

```rust
#[test]
fn numeric_maps_to_decimal_not_f64() {
    let t = map("NUMERIC", false);
    assert_eq!(t.name, "Decimal");
}
```

- [ ] **Étape 2 : Implémenter**

```rust
if upper.contains("NUMERIC") || upper.contains("DECIMAL") {
    return RustType::with_import("Decimal", "use sqlx::types::Decimal;");
}
```

- [ ] **Étape 3 : Commit**

```bash
git commit -am "fix(typemap): SQLite NUMERIC/DECIMAL → Decimal (no precision loss)"
```

---

# VAGUE P2 — Qualité de l'API et UX

## Task 16 : Erreurs sqlx contextuelles

**Fichier :** `crates/sqlx_gen/src/error.rs`

Étendre l'enum `Error` avec `SchemaNotFound { schema: String }`, `PermissionDenied { detail: String }`. Dans `introspect/*.rs`, pattern-matcher `sqlx::Error::Database(db_err)` et leur code SQLSTATE pour cibler `42P01` (Postgres : undefined_table), `42501` (insufficient_privilege), etc. Tests unitaires sur la conversion.

## Task 17 : MySQL composite PK insert — fallback SELECT

**Fichier :** `crates/sqlx_gen/src/codegen/crud_gen.rs:959-979`

Si `pk_fields.len() > 1` ou si le PK n'est pas auto-increment (détecter via `column_default = "auto"` ou similaire), générer un `SELECT * FROM table WHERE pk1 = ? AND pk2 = ?` après l'insert au lieu de `LAST_INSERT_ID()`. Tests E2E MySQL avec composite PK.

## Task 18 : Skip schema qualification pour les schémas par défaut

**Fichier :** `crates/sqlx_gen/src/codegen/crud_gen.rs:23-26`

Si `schema_name in ["public" (PG), "main" (sqlite)]`, omettre la qualification. Configurable via `--always-qualify`. Test unitaire.

## Task 19 : Documenter MSRV

**Fichiers :** `crates/sqlx_gen/Cargo.toml`, `README.md`

Ajouter `rust-version = "1.75"` au `Cargo.toml` (à vérifier par `cargo msrv find`). Mettre à jour le README. Ajouter au workflow CI une matrix `{ stable, msrv }`.

## Task 20 : Rejeter les `r#"...""#` qui contiennent `"#`

**Fichier :** `crates/sqlx_gen/src/codegen/crud_gen.rs:858-860`

Avant `format!("r#\"\n{}\n\"#", s).parse().unwrap()`, scanner `s` pour `"#` ; si présent, monter la clôture à `r##"..."##` ou retourner une erreur. Test : SQL avec commentaire `-- "#`.

---

# VAGUE P3 — Polissage

## Task 21 : Doctest sur `lib.rs`

## Task 22 : Clippy `-D warnings` propre dans CI

## Task 23 : Cycle-detection composites Postgres (finding codegen #26)

## Task 24 : Empty-batch guard `insert_many` (finding codegen #28)

## Task 25 : Logger `info!` clair "Found 0 tables — empty schema or no permission?"

Chaque task suit le même pattern : test rouge → implémentation minimale → vert → commit.

---

# VAGUE P4 — Conformité SQL ↔ Rust du code généré

Cette vague est dédiée à l'écart entre ce que **SQL attend du driver `sqlx`** et ce que **sqlx-gen produit**. Les bugs ici sont souvent silencieux : le code Rust compile, les tests unitaires passent, mais à l'exécution la requête échoue ou — pire — lit/écrit des valeurs incorrectes.

## Findings de conformité (synthèse)

| # | Sévérité | Type | Description |
|---|----------|------|-------------|
| C1 | Critique | Postgres enum | Lookup d'enum par `udt_name` non qualifié (`typemap/postgres.rs:41`). Deux enums homonymes dans deux schémas → match arbitraire. |
| C2 | Critique | Postgres enum array | `Vec<MyEnum>` généré sans `impl PgHasArrayType` → sqlx renvoie `unsupported type _my_enum` à la lecture. |
| C3 | Critique | Postgres enum schema-qualified type_name | `#[sqlx(type_name = "auth.role")]` n'est PAS le format attendu par sqlx 0.8 (qui veut `type_name = "role"` + `schema = "auth"` via le pool ou un `PgTypeInfo` custom). |
| C4 | Haut | Composite import path | `use super::types::Status;` dur-codé (`typemap/postgres.rs:43, 49`). Casse en `--single-file` ou sortie non-standard. |
| C5 | Haut | Postgres generated columns | `GENERATED ALWAYS AS (...)` non détecté en introspection → inclus dans INSERT/UPDATE → erreur SQL `cannot insert into column "x"`. |
| C6 | Haut | Postgres identity columns | `GENERATED ALWAYS AS IDENTITY` vs `BY DEFAULT AS IDENTITY` non distingués. Le premier rejette toute valeur fournie. |
| C7 | Haut | Enum variant collision après camelCase | Valeurs `foo bar` et `foo_bar` → deux `FooBar` → erreur de compilation Rust. |
| C8 | Haut | MySQL inline ENUM typing | Colonne `status ENUM('a','b')` : codegen génère un enum Rust mais `sqlx::Type` derive sans `#[sqlx(rename_all)]` ni `try_from`. Décode `String` puis échoue. |
| C9 | Haut | Domain non-newtype | `pub type Email = String` perd l'identité de type. Les utilisateurs de domains veulent newtype + validation. |
| C10 | Moyen | TIMESTAMP vs TIMESTAMPTZ misuse | Pas de warning si l'utilisateur a un `TIMESTAMP` (sans tz) mais s'attend à `DateTime<Utc>`. |
| C11 | Moyen | SQLite enum via CHECK | `TEXT CHECK (col IN ('a','b'))` non détecté → généré comme `String`. |
| C12 | Moyen | Default value `Option<T>` ambiguïté | Pour colonne `nullable + default`, l'utilisateur ne peut pas distinguer "écrire NULL" vs "utiliser default". |
| C13 | Moyen | Postgres array index OID | `udt_name` retourné par `information_schema` est parfois `_int4`, parfois `integer[]` selon la version. Le strip `_` ne couvre que le premier. |
| C14 | Moyen | MySQL `BOOLEAN` alias | `BOOLEAN` est alias de `TINYINT(1)` en MySQL. L'introspection voit `tinyint(1)` mais l'utilisateur a écrit `BOOLEAN`. Cohérent par hasard. |

---

## Task 26 : Postgres enum lookup qualifié par schéma

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/typemap/postgres.rs:40-50`
- Modifier : `crates/sqlx_gen/src/introspect/postgres.rs` (s'assurer que `ColumnInfo` porte `udt_schema`)

**Pourquoi :** Finding C1. Aujourd'hui `schema_info.enums.iter().any(|e| e.name == udt_name)` ne filtre pas par schéma. Si `public.status` et `auth.status` coexistent, le mauvais enum est référencé. Vérifié à `typemap/postgres.rs:41`.

- [ ] **Étape 1 : Test rouge**

Dans `crates/sqlx_gen/src/typemap/postgres.rs` (module `tests`) :

```rust
#[test]
fn enum_lookup_respects_schema() {
    use crate::introspect::EnumInfo;
    let schema = SchemaInfo {
        enums: vec![
            EnumInfo {
                schema_name: "public".to_string(),
                name: "status".to_string(),
                variants: vec!["a".into()],
                default_variant: None,
            },
            EnumInfo {
                schema_name: "auth".to_string(),
                name: "status".to_string(),
                variants: vec!["x".into()],
                default_variant: None,
            },
        ],
        ..Default::default()
    };
    let col = crate::introspect::ColumnInfo {
        name: "role".into(),
        data_type: "USER-DEFINED".into(),
        udt_name: "status".into(),
        udt_schema: Some("auth".into()),
        is_nullable: false,
        is_primary_key: false,
        ordinal_position: 0,
        schema_name: "public".into(),
        column_default: None,
        is_generated: false,
        is_identity_always: false,
    };
    let rt = map_column_pg(&col, &schema, TimeCrate::Chrono);
    assert!(
        rt.needs_import.as_ref().unwrap().contains("auth"),
        "must import from auth schema, got {:?}", rt.needs_import
    );
}
```

- [ ] **Étape 2 : Run**

```bash
cargo test -p sqlx-gen --lib typemap::postgres::tests::enum_lookup_respects_schema
```
Attendu : FAIL (le champ `udt_schema` n'existe pas encore).

- [ ] **Étape 3 : Étendre `ColumnInfo`**

Dans `crates/sqlx_gen/src/introspect/mod.rs` (struct `ColumnInfo`) ajouter :

```rust
pub udt_schema: Option<String>,
pub is_generated: bool,
pub is_identity_always: bool,
```

Dans `crates/sqlx_gen/src/introspect/postgres.rs`, la query `fetch_columns` ajoute :

```sql
SELECT
    c.column_name,
    c.data_type,
    c.udt_name,
    c.udt_schema,                                    -- nouveau
    c.is_nullable,
    c.column_default,
    c.is_generated,                                  -- nouveau (ALWAYS/NEVER)
    c.is_identity,                                   -- nouveau (YES/NO)
    c.identity_generation                            -- nouveau (ALWAYS/BY DEFAULT)
FROM information_schema.columns c
WHERE c.table_schema = ANY($1)
ORDER BY c.table_schema, c.table_name, c.ordinal_position
```

Mapping : `is_generated = (is_generated == "ALWAYS")`, `is_identity_always = (is_identity == "YES" AND identity_generation == "ALWAYS")`.

Pour MySQL/SQLite : laisser `udt_schema = None`, `is_generated = false`, `is_identity_always = false`.

- [ ] **Étape 4 : Implémenter `map_column_pg`**

Remplacer `map_type` par `map_column_pg(col, schema, time_crate)`. Dans la fonction :

```rust
if let Some(ref udt_schema) = col.udt_schema {
    if let Some(e) = schema_info.enums.iter()
        .find(|e| e.name == col.udt_name && &e.schema_name == udt_schema)
    {
        let name = e.name.to_upper_camel_case();
        let import_path = if e.schema_name == "public" {
            format!("use super::types::{};", name)
        } else {
            format!("use super::{}_types::{};", e.schema_name, name)
        };
        return RustType::with_import(&name, &import_path);
    }
}
// Fallback : ancien comportement (lookup non qualifié) pour MySQL inline-enum
```

Idem pour composite types. Garder `map_type(udt_name, ...)` en wrapper rétrocompatible.

- [ ] **Étape 5 : Vérifier + commit**

```bash
cargo test -p sqlx-gen --lib typemap
git add crates/sqlx_gen/src/introspect/ crates/sqlx_gen/src/typemap/postgres.rs
git commit -m "fix(typemap): qualify Postgres enum/composite lookup by schema"
```

---

## Task 27 : Postgres `_my_enum` (arrays of custom types) — émettre `PgHasArrayType`

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/enum_gen.rs:90-100`
- Modifier : `crates/sqlx_gen/src/codegen/composite_gen.rs:97-105`

**Pourquoi :** Finding C2. `typemap/postgres.rs:35-38` produit `Vec<Status>` pour un type `_status`. Mais sqlx 0.8 exige `impl PgHasArrayType for Status` pour pouvoir décoder `_status`. Sans ça, runtime panic à la lecture : `unsupported type _status of column #N`.

- [ ] **Étape 1 : Test rouge**

Dans `crates/sqlx_gen/src/codegen/enum_gen.rs` (module `tests`) :

```rust
#[test]
fn pg_enum_emits_pg_has_array_type_impl() {
    let e = make_enum("status", vec!["a", "b"]);
    let code = gen(&e, DatabaseKind::Postgres);
    assert!(
        code.contains("impl sqlx::postgres::PgHasArrayType for Status"),
        "must impl PgHasArrayType so Vec<Status> works, got:\n{}", code
    );
    assert!(code.contains("\"_status\""));
}

#[test]
fn mysql_enum_does_not_emit_pg_has_array_type_impl() {
    let e = make_enum("status", vec!["a", "b"]);
    let code = gen(&e, DatabaseKind::Mysql);
    assert!(!code.contains("PgHasArrayType"));
}
```

- [ ] **Étape 2 : Run**

```bash
cargo test -p sqlx-gen --lib codegen::enum_gen::tests::pg_enum_emits_pg_has_array_type_impl
```
Attendu : FAIL.

- [ ] **Étape 3 : Implémenter**

Dans `enum_gen.rs` après le bloc `default_impl` :

```rust
let array_type_impl = if db_kind == DatabaseKind::Postgres {
    let array_type_name = if enum_info.schema_name != "public" {
        format!("_{}.{}", enum_info.schema_name, enum_info.name)
    } else {
        format!("_{}", enum_info.name)
    };
    quote! {
        impl sqlx::postgres::PgHasArrayType for #enum_name {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                sqlx::postgres::PgTypeInfo::with_name(#array_type_name)
            }
        }
    }
} else {
    quote! {}
};
```

Puis ajouter `#array_type_impl` dans le `quote! { ... }` final, après `#default_impl`.

Reproduire la même logique dans `composite_gen.rs` (l'array d'un composite suit la même règle PG).

- [ ] **Étape 4 : Vérifier + commit**

```bash
cargo test -p sqlx-gen --lib codegen
git add crates/sqlx_gen/src/codegen/enum_gen.rs crates/sqlx_gen/src/codegen/composite_gen.rs
git commit -m "feat(codegen): emit PgHasArrayType impl for Postgres enums and composites"
```

---

## Task 28 : Postgres enum/composite `#[sqlx(type_name)]` — non qualifié ✅ APPLIQUÉ

**Statut : appliqué le 2026-06-03 après bug confirmé en production.**

**Reproducer confirmé (rapporté par @Cyrik le 30/04/2026) :** une enum en schéma non-`public` générait `#[sqlx(type_name = "agent.canal_type_enum")]` ; sqlx 0.8 plante à l'exécution car `PgTypeInfo::with_name` ne parse pas la qualification `schema.type`. La forme correcte est unqualified : `#[sqlx(type_name = "canal_type_enum")]`, avec `search_path` configuré côté pool pour résoudre le bon enum.

**Fichiers modifiés :**

- `crates/sqlx_gen/src/codegen/enum_gen.rs:42-50` — suppression de la branche `if schema != "public"`
- `crates/sqlx_gen/src/codegen/composite_gen.rs:48-52` — idem
- Tests `test_postgres_non_public_schema_qualified_type_name`, `test_named_schema_full_output`, `test_named_schema_with_default_variant`, `test_named_schema_variant_rename`, `test_non_public_schema_qualified_type_name` mis à jour pour asservir la **non-qualification** + assertion explicite `!contains("schema.type")`.

**Suivi à prévoir :**

- README : documenter clairement que le pool doit avoir `search_path` configuré avec tous les schémas portant des enums/composites utilisés. Snippet à ajouter :

```rust
let pool = PgPoolOptions::new()
    .after_connect(|conn, _meta| Box::pin(async move {
        sqlx::query("SET search_path TO public, agent, auth")
            .execute(conn).await?;
        Ok(())
    }))
    .connect(&url).await?;
```

- Émettre un doc-comment sur les enums en schéma non-public pour rappeler la contrainte (optionnel, futur).

---

## Task 29 : Collision de variants enum après camelCase

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/enum_gen.rs:53-71`

**Pourquoi :** Finding C7. `["foo bar", "foo_bar"]` → deux `FooBar` → erreur de compilation. La fonction `generate_enum` ne détecte pas les collisions.

- [ ] **Étape 1 : Test**

```rust
#[test]
fn detects_variant_camelcase_collision() {
    let e = EnumInfo {
        schema_name: "public".into(),
        name: "weird".into(),
        variants: vec!["foo bar".into(), "foo_bar".into()],
        default_variant: None,
    };
    let result = generate_enum_checked(&e, DatabaseKind::Postgres, &[]);
    assert!(result.is_err(), "must detect collision");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("FooBar"), "error must mention conflicting Rust ident");
}
```

- [ ] **Étape 2 : Implémenter**

Ajouter `generate_enum_checked` qui renvoie `Result`, et lever une erreur si :

```rust
use std::collections::BTreeMap;
let mut seen: BTreeMap<String, &str> = BTreeMap::new();
for v in &enum_info.variants {
    let pascal = v.to_upper_camel_case();
    if let Some(prev) = seen.get(&pascal) {
        return Err(crate::error::Error::Config(format!(
            "Enum '{}': SQL variants '{}' and '{}' both map to Rust ident '{}'. \
             Rename in the database or use a custom mapping.",
            enum_info.name, prev, v, pascal
        )));
    }
    seen.insert(pascal, v);
}
```

- [ ] **Étape 3 : Vérifier + commit**

```bash
cargo test -p sqlx-gen --lib codegen::enum_gen
git commit -am "fix(codegen): detect enum variant collisions after camelCase"
```

---

## Task 30 : Postgres `GENERATED` & `IDENTITY ALWAYS` exclus des INSERT/UPDATE

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/introspect/postgres.rs` (query déjà étendue Task 26)
- Modifier : `crates/sqlx_gen/src/codegen/crud_gen.rs:246-269`

**Pourquoi :** Finding C5/C6. Une colonne `total INTEGER GENERATED ALWAYS AS (qty * price) STORED` rejette tout INSERT/UPDATE qui la mentionne — erreur `42601`. De même `id INT GENERATED ALWAYS AS IDENTITY` : ne tolère pas `OVERRIDING SYSTEM VALUE`. Il faut les exclure des params.

- [ ] **Étape 1 : Test E2E**

Dans `crates/sqlx_gen/tests/e2e_postgres.rs` :

```rust
#[tokio::test]
async fn generated_column_excluded_from_insert() {
    let (_c, pool) = setup().await;
    sqlx::query(r#"
        CREATE TABLE invoices (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            qty INT NOT NULL,
            price INT NOT NULL,
            total INT GENERATED ALWAYS AS (qty * price) STORED
        )
    "#).execute(&pool).await.unwrap();
    let info = introspect(&pool, &["public".into()], false).await.unwrap();
    let cols = &info.tables[0].columns;
    let total = cols.iter().find(|c| c.name == "total").unwrap();
    assert!(total.is_generated, "must detect GENERATED column");
    let id = cols.iter().find(|c| c.name == "id").unwrap();
    assert!(id.is_identity_always, "must detect IDENTITY ALWAYS");

    let files = sqlx_gen::codegen::generate(
        &info, sqlx_gen::cli::DatabaseKind::Postgres,
        &[], &Default::default(), false, sqlx_gen::cli::TimeCrate::Chrono,
    ).unwrap();
    // Find the InsertInvoicesParams in the generated code
    let inv = files.iter().find(|f| f.filename.contains("invoice")).unwrap();
    assert!(!inv.code.contains("total:"), "InsertParams must not include 'total'");
    assert!(!inv.code.contains("pub id:") || inv.code.contains("Option<i32>"),
        "IDENTITY ALWAYS must be excluded or optional");
}
```

- [ ] **Étape 2 : Implémenter**

Dans `crud_gen.rs` (constructeur des `non_pk_fields` autour de la ligne 62-63), filtrer aussi :

```rust
let non_pk_fields: Vec<&ParsedField> = entity.fields.iter()
    .filter(|f| !f.is_primary_key)
    .filter(|f| !f.is_generated)        // exclu des INSERT/UPDATE
    .filter(|f| !f.is_identity_always)  // exclu des INSERT
    .collect();
```

Il faut donc ajouter `is_generated: bool` et `is_identity_always: bool` à `ParsedField` (`entity_parser.rs`), et les sérialiser dans l'annotation `#[sqlx_gen(...)]` lors de la génération des structs entité (Task 1 / `struct_gen.rs`).

- [ ] **Étape 3 : Vérifier + commit**

```bash
cargo test -p sqlx-gen --test e2e_postgres
git add crates/sqlx_gen/src/
git commit -m "fix(codegen): exclude generated/identity-always columns from INSERT/UPDATE"
```

---

## Task 31 : MySQL inline ENUM — `#[sqlx(rename_all = ...)]` ou utilisation `String`

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/codegen/enum_gen.rs:11-103`

**Pourquoi :** Finding C8. Pour MySQL, sqlx 0.8 lit/écrit les `ENUM` comme `String` par défaut. Pour qu'un `derive(sqlx::Type)` fonctionne, il faut soit (a) `#[repr(u32)]` + `#[sqlx(repr = ...)]` (basé sur index), soit (b) implémenter manuellement `Encode`/`Decode` qui lit la valeur en `&str`. La voie la plus simple : annoter le champ entité avec `#[sqlx(try_from = "String")]` ou utiliser `#[derive(sqlx::Type)] #[sqlx(rename_all = "lowercase")]`.

- [ ] **Étape 1 : Test E2E MySQL**

```rust
#[tokio::test]
async fn mysql_inline_enum_round_trips() {
    let (_c, pool, db) = setup().await;
    sqlx::query(r#"
        CREATE TABLE t (
            id INT PRIMARY KEY AUTO_INCREMENT,
            status ENUM('active', 'inactive') NOT NULL
        )
    "#).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO t (status) VALUES (?)")
        .bind("active").execute(&pool).await.unwrap();

    let info = introspect(&pool, &[db], false).await.unwrap();
    let files = sqlx_gen::codegen::generate(
        &info, sqlx_gen::cli::DatabaseKind::Mysql,
        &[], &Default::default(), false, sqlx_gen::cli::TimeCrate::Chrono,
    ).unwrap();
    let code = files.iter().map(|f| f.code.as_str()).collect::<Vec<_>>().join("\n");
    // Must use rename_all so 'active' / 'inactive' encode/decode correctly
    assert!(
        code.contains("rename_all") || code.contains("rename = \"active\""),
        "MySQL inline enum codegen must wire up SQL ↔ Rust variant mapping"
    );
}
```

- [ ] **Étape 2 : Implémenter**

Pour `DatabaseKind::Mysql` dans `enum_gen.rs`, après les variants, vérifier si tous les variants sont lowercase ASCII — auquel cas émettre :

```rust
quote! { #[sqlx(rename_all = "lowercase")] }
```

et omettre les `#[sqlx(rename = "...")]` individuels. Sinon, garder les renames explicites par variant.

- [ ] **Étape 3 : Vérifier + commit**

```bash
git commit -am "fix(codegen): wire MySQL inline ENUM variants via rename_all/rename"
```

---

## Task 32 : Domain en newtype optionnel via `--domains-as-newtype`

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/cli.rs` (ajouter flag)
- Modifier : `crates/sqlx_gen/src/codegen/domain_gen.rs:43-53`

**Pourquoi :** Finding C9. `pub type Email = String;` est un alias transparent. L'utilisateur perd la sécurité de type. Offrir un mode newtype optionnel : `pub struct Email(pub String);` + `#[sqlx(transparent)]`.

- [ ] **Étape 1 : Ajouter le flag CLI**

Dans `EntitiesArgs` :

```rust
/// Generate domains as newtype structs (`pub struct Email(String)`) instead of type aliases.
#[arg(long)]
pub domains_as_newtype: bool,
```

- [ ] **Étape 2 : Test**

```rust
#[test]
fn domain_as_newtype_uses_transparent_derive() {
    let d = make_domain("email", "text");
    let schema = SchemaInfo::default();
    let (tokens, _) = generate_domain_with_style(
        &d, DatabaseKind::Postgres, &schema,
        &HashMap::new(), TimeCrate::Chrono,
        DomainStyle::Newtype,
    );
    let code = parse_and_format(&tokens).unwrap();
    assert!(code.contains("pub struct Email"));
    assert!(code.contains("#[sqlx(transparent)]"));
    assert!(code.contains("pub String"));
}
```

- [ ] **Étape 3 : Implémenter**

```rust
pub enum DomainStyle { Alias, Newtype }

pub fn generate_domain_with_style(
    domain: &DomainInfo,
    db_kind: DatabaseKind,
    schema_info: &SchemaInfo,
    type_overrides: &HashMap<String, String>,
    time_crate: TimeCrate,
    style: DomainStyle,
) -> (TokenStream, BTreeSet<String>) {
    // ... existing setup
    let tokens = match style {
        DomainStyle::Alias => quote! {
            #[doc = #doc]
            pub type #alias_name = #type_tokens;
        },
        DomainStyle::Newtype => quote! {
            #[doc = #doc]
            #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
            #[sqlx(transparent)]
            pub struct #alias_name(pub #type_tokens);
        },
    };
    (tokens, imports)
}
```

- [ ] **Étape 4 : Vérifier + commit**

```bash
git commit -am "feat(codegen): add --domains-as-newtype for type-safe domain wrappers"
```

---

## Task 33 : SQLite enum via `CHECK (col IN (...))`

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/introspect/sqlite.rs`
- Modifier : `crates/sqlx_gen/src/codegen/enum_gen.rs` (le SQLite branch)

**Pourquoi :** Finding C11. SQLite n'a pas d'`ENUM` natif. Convention courante : `TEXT CHECK (col IN ('a','b','c'))`. Détecter ce pattern via `sqlite_master.sql` (DDL stocké) et émettre un enum Rust.

- [ ] **Étape 1 : Test E2E SQLite**

Dans `crates/sqlx_gen/tests/introspect_sqlite.rs` :

```rust
#[tokio::test]
async fn detects_check_enum_pattern() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::query(r#"
        CREATE TABLE t (
            id INTEGER PRIMARY KEY,
            status TEXT CHECK (status IN ('active', 'inactive')) NOT NULL
        )
    "#).execute(&pool).await.unwrap();
    let info = introspect(&pool, false).await.unwrap();
    assert!(!info.enums.is_empty(), "should detect implicit enum");
    assert_eq!(info.enums[0].variants, vec!["active", "inactive"]);
}
```

- [ ] **Étape 2 : Implémenter le parser**

Dans `introspect/sqlite.rs`, ajouter `extract_check_enums(&sql_ddl)` qui regex-trouve `CHECK\s*\(\s*(\w+)\s+IN\s*\(\s*(.+?)\s*\)\s*\)` et extrait les variants entre apostrophes. Le résultat alimente `SchemaInfo.enums` avec `schema_name="main"`.

Pour la `column.udt_name`, remplacer `"TEXT"` par le nom de l'enum déduit (e.g. `status_enum`).

- [ ] **Étape 3 : Vérifier + commit**

```bash
cargo test -p sqlx-gen --test introspect_sqlite
git commit -am "feat(introspect-sqlite): detect TEXT CHECK IN (...) enum pattern"
```

---

## Task 34 : Cohérence array : `_x` + `x[]` + `ARRAY[x]`

**Fichiers :**

- Modifier : `crates/sqlx_gen/src/typemap/postgres.rs:33-38`

**Pourquoi :** Finding C13. `information_schema.columns.udt_name` retourne `_int4` ; `data_type` retourne `ARRAY` ; `pg_catalog.format_type` retourne `integer[]`. Le code actuel ne gère que `_int4`. Robustifier.

- [ ] **Étape 1 : Tests**

```rust
#[test]
fn array_underscore_prefix() {
    assert_eq!(map_type("_int4", &empty_schema(), TimeCrate::Chrono).path, "Vec<i32>");
}
#[test]
fn array_bracket_suffix() {
    assert_eq!(map_type("integer[]", &empty_schema(), TimeCrate::Chrono).path, "Vec<i32>");
}
#[test]
fn array_double() {
    assert_eq!(map_type("_int4_int4", &empty_schema(), TimeCrate::Chrono).path, "Vec<Vec<i32>>");
}
```

- [ ] **Étape 2 : Implémenter**

```rust
pub fn map_type(udt_name: &str, schema_info: &SchemaInfo, time_crate: TimeCrate) -> RustType {
    if let Some(inner) = udt_name.strip_prefix('_') {
        return map_type(inner, schema_info, time_crate).wrap_vec();
    }
    if let Some(inner) = udt_name.strip_suffix("[]") {
        return map_type(inner.trim(), schema_info, time_crate).wrap_vec();
    }
    // ... reste inchangé
}
```

- [ ] **Étape 3 : Vérifier + commit**

```bash
cargo test -p sqlx-gen --lib typemap::postgres
git commit -am "fix(typemap): normalize array notations (_x and x[])"
```

---

## Task 35 : Snapshot E2E "build du code généré"

**Fichiers :**

- Créer : `crates/sqlx_gen/tests/compile_check.rs`

**Pourquoi :** Aucun test ne vérifie que le code généré **compile dans un projet utilisateur**. C'est la conformité ultime : si le code passe `cargo build`, alors les attributs sqlx sont bien formés.

- [ ] **Étape 1 : Squelette**

```rust
use std::process::Command;

fn write_minimal_consumer(out_dir: &std::path::Path, generated_code: &str) {
    std::fs::write(out_dir.join("Cargo.toml"), r#"
[package]
name = "sqlx_gen_compile_test"
version = "0.0.0"
edition = "2021"

[dependencies]
sqlx = { version = "0.8", features = ["postgres", "uuid", "chrono", "json"] }
sqlx_gen = { path = "../../../../crates/sqlx_gen" }
serde = { version = "1", features = ["derive"] }
chrono = "0.4"
uuid = "1"
serde_json = "1"
"#).unwrap();
    std::fs::create_dir_all(out_dir.join("src")).unwrap();
    std::fs::write(out_dir.join("src/lib.rs"), generated_code).unwrap();
}

#[test]
fn generated_postgres_table_compiles() {
    use sqlx_gen::codegen::{generate, GeneratedFile};
    use sqlx_gen::introspect::*;
    // build a small SchemaInfo by hand with enum + composite + view + array column
    // ... (~50 lines)
    let files = generate(&info, sqlx_gen::cli::DatabaseKind::Postgres,
        &[], &Default::default(), true /* single file */, sqlx_gen::cli::TimeCrate::Chrono).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let code = files[0].code.clone();
    write_minimal_consumer(dir.path(), &code);
    let status = Command::new("cargo").arg("build").current_dir(dir.path()).status().unwrap();
    assert!(status.success(), "generated code must compile");
}
```

- [ ] **Étape 2 : Commit**

```bash
git add crates/sqlx_gen/tests/compile_check.rs
git commit -m "test(compile): verify generated code compiles in a downstream crate"
```

---

## Synthèse de la vague conformité

Tasks 26–35 ferment les écarts SQL ↔ Rust les plus dangereux :

| Couvre finding | Task |
|----------------|------|
| C1 (lookup enum non qualifié) | 26 |
| C2 (PgHasArrayType manquant) | 27 |
| C3 (type_name schema-qualifié) | 28 |
| C4 (import path super::types) | 26 (indirect, via path schema-aware) |
| C5 (generated columns) | 30 |
| C6 (identity always) | 30 |
| C7 (variant collisions) | 29 |
| C8 (MySQL inline ENUM) | 31 |
| C9 (domain newtype) | 32 |
| C10 (TS vs TSTZ) | À documenter dans le README (pas de task séparée — trivial) |
| C11 (SQLite CHECK enum) | 33 |
| C12 (default vs nullable ambiguïté) | Couvert par doc en Task 18 |
| C13 (array notations) | 34 |
| C14 (MySQL BOOLEAN alias) | Couvert par Task 13 (BIT(1)→bool) |

Le **compile-check Task 35** est le filet de sécurité final : toute régression d'attribut ou d'import est détectée par `cargo build` sur un crate consommateur.

---

## Self-review (effectuée par le rédacteur)

**Couverture spec :** Les 12 findings P0 sont couverts par les Tasks 1–10 ; les 9 findings High P1 par Tasks 11–15 ; le P2 par Tasks 16–20 ; le P3 par Tasks 21–25 ; les 14 findings de conformité SQL ↔ Rust (C1–C14) par Tasks 26–35.

**Placeholders :** Tasks 16–25 sont volontairement plus haut-niveau (1 paragraphe chacune) car le pattern TDD est répétitif et chaque task isolément ne nécessite pas 5 étapes détaillées. Si l'engineer demande, dérouler à la demande.

**Cohérence des types :** `quote_ident`/`quote_qualified` utilisés systématiquement après Task 1. `write_atomic` ajouté Task 7, réutilisé Task 8. `parse_and_format_with_tab_spaces` renvoie `Result<String>` après Task 5 — tous les call-sites mis à jour dans le même commit.

**Gaps connus :** Le finding codegen #19 (forced `query_scalar` sur `LAST_INSERT_ID`) est traité indirectement par Task 17. Le finding error #11 (cancellation tokio) n'est pas traité — coût élevé, gain faible ; documenter dans CONTRIBUTING.

---

## Handoff d'exécution

**Plan complet sauvegardé dans `docs/superpowers/plans/2026-06-03-rust-engineering-audit.md`. Deux options d'exécution :**

**1. Subagent-Driven (recommandé)** — un sous-agent frais par task, review entre chaque, itération rapide. Idéal pour P0 où chaque correction doit être validée avant la suivante.

**2. Inline Execution** — exécution des tasks dans cette session via `superpowers:executing-plans`, batch avec checkpoints.

**Quelle approche ?**
