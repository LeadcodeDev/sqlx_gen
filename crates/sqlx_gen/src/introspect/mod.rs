pub mod mysql;
pub mod postgres;
pub mod sqlite;

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct ColumnInfo {
    pub name: String,
    /// High-level data type (e.g. "integer", "character varying")
    pub data_type: String,
    /// Underlying type name: udt_name (PG), column_type (MySQL), declared type (SQLite)
    pub udt_name: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub ordinal_position: i32,
    pub schema_name: String,
    /// Raw column default expression (e.g. `'idle'::task_status`)
    pub column_default: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema_name: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct EnumInfo {
    pub schema_name: String,
    pub name: String,
    pub variants: Vec<String>,
    /// Default variant name (raw SQL value, e.g. "idle"), if any column uses this enum with a DEFAULT.
    pub default_variant: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CompositeTypeInfo {
    pub schema_name: String,
    pub name: String,
    pub fields: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct DomainInfo {
    pub schema_name: String,
    pub name: String,
    /// The underlying SQL type
    pub base_type: String,
}

#[derive(Debug, Clone, Default)]
pub struct SchemaInfo {
    pub tables: Vec<TableInfo>,
    pub views: Vec<TableInfo>,
    pub enums: Vec<EnumInfo>,
    pub composite_types: Vec<CompositeTypeInfo>,
    pub domains: Vec<DomainInfo>,
}
