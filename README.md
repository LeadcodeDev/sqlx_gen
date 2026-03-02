# sqlx-gen

Generate Rust structs and CRUD repositories from your database schema — with correct types, derives, and `sqlx` annotations.

Supports **PostgreSQL**, **MySQL**, and **SQLite**. Introspects tables, views, enums, composite types, and domains.

[![Crates.io](https://img.shields.io/crates/v/sqlx-gen.svg)](https://crates.io/crates/sqlx-gen)
[![docs.rs](https://docs.rs/sqlx-gen/badge.svg)](https://docs.rs/sqlx-gen)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Features

- Multi-database: PostgreSQL, MySQL, SQLite
- Multi-schema support (PostgreSQL)
- Generates `#[derive(sqlx::FromRow)]` structs with `Serialize`, `Deserialize`, `PartialEq`, `Eq`
- PostgreSQL enums → `#[derive(sqlx::Type)]` enums
- PostgreSQL composite types and domains
- MySQL inline ENUM detection
- Correct nullable handling (`Option<T>`)
- Primary key detection across all backends
- Custom derives (`--derives Hash`)
- Type overrides (`--type-overrides jsonb=MyType`)
- SQL views support (`--views`)
- Table filtering (`--tables users,orders`) and exclusion (`--exclude-tables _migrations`)
- Single-file or multi-file output
- Dry-run mode (preview on stdout)
- **CRUD repository generation** from generated entity files (no DB connection required)
- `#[sqlx_gen(...)]` annotations on all generated types for tooling integration
- Automatic `rustfmt` formatting (edition detected from `Cargo.toml`)
- Automatic `mod.rs` management for generated CRUD files

## Installation

```sh
cargo install sqlx-gen
```

## Commands

sqlx-gen uses subcommands:

```
sqlx-gen generate entities   # Generate entity structs from DB schema
sqlx-gen generate crud       # Generate CRUD repository from an entity file
```

## Generate Entities

### PostgreSQL (multi-schema)
```sh
sqlx-gen generate entities -u postgres://user:pass@localhost/mydb -s public,auth -o src/models
```

### MySQL
```sh
sqlx-gen generate entities -u mysql://user:pass@localhost/mydb -o src/models
```

### SQLite
```sh
sqlx-gen generate entities -u sqlite:./local.db -o src/models
```

### With extra derives
```sh
sqlx-gen generate entities -u postgres://... -D Hash -o src/models
```

### Exclude specific tables
```sh
sqlx-gen generate entities -u postgres://... -x _migrations,schema_versions -o src/models
```

### Include SQL views
```sh
sqlx-gen generate entities -u postgres://... -v -o src/models
```

### Dry run (preview without writing)
```sh
sqlx-gen generate entities -u postgres://... -n
```

### Entities CLI Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--database-url` | `-u` | Database connection URL (or `DATABASE_URL` env var) | required |
| `--schemas` | `-s` | Schemas to introspect (comma-separated) | `public` |
| `--output-dir` | `-o` | Output directory | `src/models` |
| `--derives` | `-D` | Additional derive macros (comma-separated) | none |
| `--type-overrides` | `-T` | Type overrides `sql_type=RustType` (comma-separated) | none |
| `--single-file` | `-S` | Write everything to a single `models.rs` | `false` |
| `--tables` | `-t` | Only generate these tables (comma-separated) | all |
| `--exclude-tables` | `-x` | Exclude these tables/views (comma-separated) | none |
| `--views` | `-v` | Also generate structs for SQL views | `false` |
| `--dry-run` | `-n` | Print to stdout, don't write files | `false` |

## Generate CRUD

Generate a repository from an already-generated entity file. No database connection is required — the generator reads the Rust source file directly.

You must specify which methods to generate with `--methods` (`-m`):

```sh
# Generate all CRUD methods
sqlx-gen generate crud \
  -f src/models/users.rs \
  -d postgres \
  -m '*' \
  -o src/repositories

# Generate only specific methods
sqlx-gen generate crud \
  -f src/models/users.rs \
  -d postgres \
  -m get_all,get,insert

# With explicit module path (auto-detected by default)
sqlx-gen generate crud \
  -f src/models/users.rs \
  -d postgres \
  -e crate::models::users \
  -m '*'

# With compile-time checked macros
sqlx-gen generate crud \
  -f src/models/users.rs \
  -d postgres \
  -m '*' \
  -q
```

### Module path auto-detection

The `--entities-module` (`-e`) option is **optional**. When omitted, the module path is automatically derived from the `--entity-file` path by locating `src/` and converting to a Rust module path:

| File path | Derived module |
|-----------|---------------|
| `src/models/users.rs` | `crate::models::users` |
| `src/db/entities/agent.rs` | `crate::db::entities::agent` |
| `src/models/mod.rs` | `crate::models` |
| `../project/src/models/users.rs` | `crate::models::users` |

### Views

Views are automatically detected via the `#[sqlx_gen(kind = "view")]` annotation — write methods (`insert`, `update`, `delete`) are never generated for views even if requested.

### Pool field visibility

By default, the `pool` field in generated repositories is private. Use `--pool-visibility` (`-p`) to change it:

```sh
# Public pool field
sqlx-gen generate crud -f src/models/users.rs -d postgres -m '*' -p pub

# Crate-visible pool field
sqlx-gen generate crud -f src/models/users.rs -d postgres -m '*' -p 'pub(crate)'
```

### Compile-time checked macros

By default, the CRUD generator uses `sqlx::query_as::<_, T>()` with `.bind()` chains (runtime). Pass `--query-macro` (`-q`) to generate `sqlx::query_as!()` / `sqlx::query!()` macros instead, which are checked at compile time (requires `DATABASE_URL` at build time).

### Available methods

| Method | Description |
|--------|------------|
| `*` | Generate all methods below |
| `get_all` | `SELECT *` returning `Vec<T>` |
| `paginate` | `SELECT *` with `LIMIT` / `OFFSET` returning `Vec<T>` |
| `get` | `SELECT *` by primary key returning `Option<T>` |
| `insert` | `INSERT` with a params struct, `RETURNING *` |
| `update` | `UPDATE` by primary key with a params struct, `RETURNING *` |
| `delete` | `DELETE` by primary key |

### mod.rs management

When writing a CRUD file (not in dry-run mode), sqlx-gen automatically updates or creates a `mod.rs` in the output directory with the corresponding `pub mod` declaration.

### Formatting

Generated files are automatically formatted with `rustfmt`. The Rust edition is detected from the nearest `Cargo.toml` in the output directory's parent chain (defaults to `2021` if not found).

### CRUD CLI Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--entity-file` | `-f` | Path to the generated entity `.rs` file | required |
| `--db-kind` | `-d` | Database kind: `postgres`, `mysql`, `sqlite` | required |
| `--entities-module` | `-e` | Rust module path (e.g. `crate::models::users`). Auto-detected from file path if omitted. | auto |
| `--output-dir` | `-o` | Output directory | `src/crud` |
| `--methods` | `-m` | Methods to generate (comma-separated): `*`, `get_all`, `paginate`, `get`, `insert`, `update`, `delete` | required |
| `--query-macro` | `-q` | Use `sqlx::query_as!()` macros (compile-time checked) | `false` |
| `--pool-visibility` | `-p` | Visibility of the `pool` field: `private`, `pub`, `pub(crate)` | `private` |
| `--dry-run` | `-n` | Print to stdout, don't write files | `false` |

## Example Output

### Entity (table)

```rust
// Auto-generated by sqlx-gen. Do not edit.
// Table: public.users

use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
#[sqlx_gen(kind = "table", table = "users")]
pub struct Users {
    #[sqlx_gen(primary_key)]
    pub id: Uuid,
    pub email: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
}
```

### Entity (view)

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
#[sqlx_gen(kind = "view", table = "active_users")]
pub struct ActiveUsers {
    pub id: Uuid,
    pub email: String,
}
```

### Enum

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx_gen(kind = "enum")]
#[sqlx(type_name = "status")]
pub enum Status {
    #[sqlx(rename = "active")]
    Active,

    #[sqlx(rename = "inactive")]
    Inactive,
}
```

### CRUD Repository (default — runtime)

```rust
impl UsersRepository {
    pub async fn get(&self, id: &Uuid) -> Result<Option<Users>, sqlx::Error> {
        sqlx::query_as::<_, Users>("SELECT * FROM users WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn insert(&self, params: &InsertUsersParams) -> Result<Users, sqlx::Error> {
        sqlx::query_as::<_, Users>(
            "INSERT INTO users (email, name, created_at) VALUES ($1, $2, $3) RETURNING *",
        )
            .bind(&params.email)
            .bind(&params.name)
            .bind(&params.created_at)
            .fetch_one(&self.pool)
            .await
    }
    // ...
}
```

### CRUD Repository (`--query-macro` — compile-time checked)

```rust
impl UsersRepository {
    pub async fn get(&self, id: &Uuid) -> Result<Option<Users>, sqlx::Error> {
        sqlx::query_as!(Users, "SELECT * FROM users WHERE id = $1", id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn insert(&self, params: &InsertUsersParams) -> Result<Users, sqlx::Error> {
        sqlx::query_as!(
            Users,
            "INSERT INTO users (email, name, created_at) VALUES ($1, $2, $3) RETURNING *",
            params.email, params.name, params.created_at
        )
            .fetch_one(&self.pool)
            .await
    }
    // ...
}
```

## Annotations

All generated types include `#[sqlx_gen(...)]` annotations for tooling:

| Type | Annotation |
|------|-----------|
| Table struct | `#[sqlx_gen(kind = "table", table = "name")]` |
| View struct | `#[sqlx_gen(kind = "view", table = "name")]` |
| Enum | `#[sqlx_gen(kind = "enum")]` |
| Composite type | `#[sqlx_gen(kind = "composite")]` |
| Domain type | `#[sqlx_gen(kind = "domain")]` |
| Primary key field | `#[sqlx_gen(primary_key)]` |

## License

MIT
