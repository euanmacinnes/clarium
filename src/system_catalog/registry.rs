use polars::prelude::{DataFrame, Series, NamedFrom};
use once_cell::sync::OnceCell;
use std::sync::{Arc, Mutex};
use crate::storage::SharedStore;
use std::collections::HashMap;
use polars::prelude::DataType;

#[derive(Clone, Copy, Debug)]
pub enum ColType {
    Integer,
    BigInt,
    Boolean,
    Text,
}

#[derive(Clone, Copy, Debug)]
pub struct ColumnDef {
    pub name: &'static str,
    pub coltype: ColType,
}

pub trait SystemTable: Send + Sync {
    fn schema(&self) -> &'static str;
    fn name(&self) -> &'static str;
    fn columns(&self) -> &'static [ColumnDef];
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // Default no-op build using declared columns
        Some(build_empty(self.columns()))
    }
}

pub struct NoOpSystemTable {
    schema: &'static str,
    name: &'static str,
    columns: &'static [ColumnDef],
}

impl NoOpSystemTable {
    pub const fn new(schema: &'static str, name: &'static str, columns: &'static [ColumnDef]) -> Self {
        Self { schema, name, columns }
    }
}

impl SystemTable for NoOpSystemTable {
    fn schema(&self) -> &'static str { self.schema }
    fn name(&self) -> &'static str { self.name }
    fn columns(&self) -> &'static [ColumnDef] { self.columns }
}

pub fn build_empty(cols: &[ColumnDef]) -> DataFrame {
    let mut series: Vec<Series> = Vec::with_capacity(cols.len());
    for c in cols {
        match c.coltype {
            ColType::Integer => series.push(Series::new(c.name.into(), Vec::<i32>::new())),
            ColType::BigInt => series.push(Series::new(c.name.into(), Vec::<i64>::new())),
            ColType::Boolean => series.push(Series::new(c.name.into(), Vec::<bool>::new())),
            ColType::Text => series.push(Series::new(c.name.into(), Vec::<String>::new())),
        }
    }
    DataFrame::new(series.into_iter().map(|s| s.into()).collect()).unwrap()
}

static REGISTRY: OnceCell<Mutex<Vec<Arc<dyn SystemTable>>>> = OnceCell::new();

fn registry() -> &'static Mutex<Vec<Arc<dyn SystemTable>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

pub fn register(table: Box<dyn SystemTable>) {
    let mut reg = registry().lock().unwrap();
    reg.push(Arc::from(table));
}

pub fn ensure_registered() {
    // If empty, populate defaults
    let reg = registry().lock().unwrap();
    if !reg.is_empty() { return; }
    drop(reg);
    // Call default registrar
    super::pg_catalog::register_defaults();
    super::information_schema::register_defaults();
}

pub fn all() -> Vec<Arc<dyn SystemTable>> {
    let reg = registry().lock().unwrap();
    reg.iter().cloned().collect()
}

// Helper to clone trait objects by recreating NoOp variants from their metadata.
// For now, we only need read-only iteration (schema/name/columns), so we can
// return lightweight wrappers that delegate to original columns.
pub fn find(schema: &str, name: &str) -> Option<Arc<dyn SystemTable>> {
    let reg = registry().lock().unwrap();
    for t in reg.iter() {
        if t.schema().eq_ignore_ascii_case(schema) && t.name().eq_ignore_ascii_case(name) {
            return Some(Arc::clone(t));
        }
    }
    None
}

/// Map `ColType` to Polars `DataType` used by storage/schema layer.
fn coltype_to_dtype(ct: ColType) -> DataType {
    match ct {
        ColType::Integer => DataType::Int64,
        ColType::BigInt => DataType::Int64,
        ColType::Boolean => DataType::Boolean,
        ColType::Text => DataType::String,
    }
}

/// Build a schema map (column name -> DataType) from a system table's column defs.
pub fn schema_map_for(table: &dyn SystemTable) -> HashMap<String, DataType> {
    let mut m: HashMap<String, DataType> = HashMap::new();
    for c in table.columns() {
        m.insert(c.name.to_string(), coltype_to_dtype(c.coltype));
    }
    m
}

/// Attempt to resolve an input identifier (which may be a path like
/// "db/schema/pg_type" or just a bare name) to a registered system table.
/// The resolution strategy mirrors `system::system_table_df` but limited to
/// choosing the system catalog entry.
pub fn lookup_from_str(input: &str) -> Option<Arc<dyn SystemTable>> {
    ensure_registered();
    // Normalize: trim, strip alias after whitespace, strip quotes and semicolon
    let mut base = input.trim().to_string();
    if let Some(idx) = base.find(|c: char| c.is_whitespace()) { base = base[..idx].to_string(); }
    if base.ends_with(';') { base.pop(); }
    if (base.starts_with('"') && base.ends_with('"')) || (base.starts_with('\'') && base.ends_with('\'')) {
        base = base[1..base.len()-1].to_string();
    }
    let ident = base.replace('\\', "/").to_lowercase();
    let dotted = ident.replace('/', ".");
    let parts: Vec<&str> = dotted.split('.').collect();
    let last1 = parts.last().copied().unwrap_or("");
    let last2 = if parts.len() >= 2 { (parts[parts.len()-2], parts[parts.len()-1]) } else { ("", last1) };
    // Prefer explicit schema.table
    if !last2.0.is_empty() && !last2.1.is_empty() {
        if let Some(t) = find(last2.0, last2.1) { return Some(t); }
    }
    // Bare name: try pg_catalog then information_schema
    if let Some(t) = find("pg_catalog", last1) { return Some(t); }
    if let Some(t) = find("information_schema", last1) { return Some(t); }
    None
}
