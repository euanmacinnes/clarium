//! exec_views
//! -----------
//! VIEW DDL handling and helpers: CREATE VIEW, DROP VIEW, SHOW VIEW,
//! and loading view definitions for use in FROM resolution.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;
use polars::prelude::*;

use crate::server::query;
use crate::storage::SharedStore;
use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewFile {
    pub name: String,
    pub columns: Vec<(String, String)>, // (name, dtype key: string|int64|float64|bool)
    pub definition_sql: String,
}

fn dtype_key_of(dt: &polars::prelude::DataType) -> String {
    // Map to our simple keys using Debug representation to avoid tight coupling to specific enum variants
    let s = format!("{:?}", dt).to_lowercase();
    if s.contains("int") || s.contains("date") || s.contains("time") { return "int64".into(); }
    if s.contains("float") || s.contains("double") || s.contains("decimal") || s.contains("numeric") { return "float64".into(); }
    if s.contains("bool") { return "bool".into(); }
    "string".into()
}

fn qualify_view_name(name: &str) -> String {
    // Use current session defaults (USE DATABASE/SCHEMA)
    let d = crate::system::current_query_defaults();
    crate::ident::qualify_regular_ident(name, &d)
}

fn view_path_for(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p.set_extension("view");
    p
}

pub fn read_view_file(store: &SharedStore, qualified: &str) -> Result<Option<ViewFile>> {
    let path = view_path_for(store, qualified);
    if !path.exists() { return Ok(None); }
    let text = std::fs::read_to_string(&path)?;
    let v: ViewFile = serde_json::from_str(&text)?;
    Ok(Some(v))
}

fn write_view_file(store: &SharedStore, qualified: &str, vf: &ViewFile) -> Result<()> {
    let path = view_path_for(store, qualified);
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
    std::fs::write(&path, serde_json::to_string_pretty(vf)?)?;
    Ok(())
}

fn delete_view_file(store: &SharedStore, qualified: &str) -> Result<()> {
    let path = view_path_for(store, qualified);
    if path.exists() { std::fs::remove_file(&path).ok(); }
    Ok(())
}

fn infer_columns_from_sql(store: &SharedStore, def_sql: &str) -> Result<Vec<(String, String)>> {
    let cmd = query::parse(def_sql)?;
    use query::Command;
    let df = match cmd {
        Command::Select(q) => crate::server::exec::exec_select::run_select(store, &q)?,
        Command::SelectUnion { queries, all } => crate::server::exec::exec_select::handle_select_union(store, &queries, all)?,
        other => return Err(AppError::Ddl { code: "view_definition".into(), message: format!("View definition must be SELECT or SELECT UNION, got: {:?}", other) }.into()),
    };
    let mut cols: Vec<(String, String)> = Vec::new();
    for n in df.get_column_names() {
        let dtype = df.column(n.as_str())?.dtype().clone();
        cols.push((n.to_string(), dtype_key_of(&dtype)));
    }
    Ok(cols)
}

pub fn execute_views(store: &SharedStore, cmd: query::Command) -> Result<serde_json::Value> {
    match cmd {
        query::Command::CreateView { name, or_alter, if_not_exists, definition_sql } => {
            let qualified = qualify_view_name(&name);
            let exists = read_view_file(store, &qualified)?.is_some();
            if exists {
                if if_not_exists { return Ok(serde_json::json!({"status":"ok"})); }
                if !or_alter { return Err(AppError::Conflict { code: "name_conflict".into(), message: format!("View already exists: {}", qualified) }.into()); }
            }
            // Enforce uniqueness across objects: a view name must not clash with an existing table folder
            {
                let root = store.0.lock().root_path().clone();
                let table_dir = root.join(qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
                let time_dir = root.join(format!("{}{}time",
                    qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()),
                    std::path::MAIN_SEPARATOR
                ));
                if table_dir.is_dir() {
                    return Err(AppError::Conflict { code: "name_conflict".into(), message: format!("A TABLE exists with name '{}'. View names must be unique across tables.", qualified) }.into());
                }
                if time_dir.is_dir() {
                    // If table is stored as <name>.time directory
                    return Err(AppError::Conflict { code: "name_conflict".into(), message: format!("A TIME TABLE exists with base name '{}'. View names must be unique across objects.", qualified) }.into());
                }
            }
            // Infer columns by executing the definition
            let columns = infer_columns_from_sql(store, &definition_sql)?;
            let vf = ViewFile { name: qualified.clone(), columns, definition_sql };
            write_view_file(store, &qualified, &vf)?;
            info!(target: "clarium::ddl", "CREATE VIEW saved '{}.view'", qualified);
            Ok(serde_json::json!({"status":"ok"}))
        }
        query::Command::DropView { name, if_exists } => {
            let qualified = qualify_view_name(&name);
            let exists = read_view_file(store, &qualified)?.is_some();
            if !exists && if_exists { return Ok(serde_json::json!({"status":"ok"})); }
            if !exists { return Err(AppError::NotFound { code: "not_found".into(), message: format!("View not found: {}", qualified) }.into()); }
            delete_view_file(store, &qualified)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        query::Command::ShowView { name } => {
            let qualified = qualify_view_name(&name);
            if let Some(vf) = read_view_file(store, &qualified)? {
                let df = DataFrame::new(vec![
                    Series::new("name".into(), vec![vf.name]).into(),
                    Series::new("definition".into(), vec![vf.definition_sql]).into(),
                ])?;
                return Ok(crate::server::exec::exec_helpers::dataframe_to_json(&df));
            }
            return Err(AppError::NotFound { code: "not_found".into(), message: format!("View not found: {}", qualified) }.into());
        }
        _ => return Err(AppError::Ddl { code: "unsupported_views".into(), message: "unsupported views command".into() }.into()),
    }
}
