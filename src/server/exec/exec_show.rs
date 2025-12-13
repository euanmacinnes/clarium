use anyhow::Result;
use serde_json::Value;
// use polars::prelude::*; // not needed directly here

use crate::server::query::Command;
// use crate::scripts::scripts_dir_for; // unused in this module
use crate::storage::SharedStore;
use crate::server::graphstore::graphstore_status_df;
use crate::lua_bc::DEFAULT_DB;

pub async fn execute_show(store: &SharedStore, cmd: Command) -> Result<Value> {
    match cmd {
        Command::ShowGraphStatus { name } => {
            // Determine graph: explicit arg wins; else fall back to session default graph
            let graph = if let Some(n) = name { n } else {
                crate::system::get_current_graph_opt()
                    .ok_or_else(|| anyhow::anyhow!("SHOW GRAPH STATUS: missing graph name; set it with USE GRAPH or provide explicitly"))?
            };
            let df = graphstore_status_df(store, &graph)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowTransactionIsolation => single_kv("transaction_isolation", "read committed"),
        Command::ShowStandardConformingStrings => single_kv("standard_conforming_strings", "on"),
        Command::ShowServerVersion => single_kv("server_version", "14.0"),
        Command::ShowClientEncoding => single_kv("client_encoding", "UTF8"),
        Command::ShowServerEncoding => single_kv("server_encoding", "UTF8"),
        Command::ShowDateStyle => single_kv("DateStyle", "ISO, MDY"),
        Command::ShowIntegerDateTimes => single_kv("integer_datetimes", "on"),
        Command::ShowTimeZone => single_kv("TimeZone", "UTC"),
        Command::ShowSearchPath => single_kv("search_path", crate::ident::DEFAULT_SCHEMA),
        Command::ShowDefaultTransactionIsolation => single_kv("default_transaction_isolation", "read committed"),
        Command::ShowTransactionReadOnly => single_kv("transaction_read_only", "off"),
        Command::ShowApplicationName => single_kv("application_name", "clarium"),
        Command::ShowExtraFloatDigits => single_kv("extra_float_digits", "3"),
        Command::ShowAll => show_all(),
        Command::ShowSchemas => show_schemas(store),
        Command::ShowTables => show_tables(store),
        Command::ShowObjects => show_objects(store),
        Command::ShowScripts => show_scripts(store),
        // -------------------------------------------------
        // FILESTORE SHOW commands → delegate to filestore::show
        Command::ShowFilestores { database } => {
            let db = database.unwrap_or_else(|| DEFAULT_DB.to_string());
            let df = crate::server::exec::filestore::show_filestores_df(store, &db)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowFilestoreConfig { filestore, folder_prefix } => {
            let df = crate::server::exec::filestore::show_filestore_config_df(store, DEFAULT_DB, &filestore, folder_prefix.as_deref())?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowFilesInFilestore { filestore, prefix, limit, offset } => {
            let off = offset.unwrap_or(0).max(0) as usize;
            let lim = limit.and_then(|n| if n > 0 { Some(n as usize) } else { None });
            let df = crate::server::exec::filestore::show_files_df_paged(store, DEFAULT_DB, &filestore, prefix.as_deref(), off, lim)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowTreesInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_trees_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowCommitsInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_commits_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowDiffInFilestore { filestore, left_tree_id, right_tree_id, live_prefix } => {
            let df = crate::server::exec::filestore::show_diff_df(store, DEFAULT_DB, &filestore, &left_tree_id, right_tree_id.as_deref(), live_prefix.as_deref())?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowChunksInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_chunks_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowAliasesInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_aliases_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowAdminInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_admin_counts_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowHealthInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_health_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        // -------------------------------------------------
        other => anyhow::bail!(format!("unsupported SHOW variant in exec_show: {:?}", other)),
    }
}

fn single_kv(key: &str, val: &str) -> Result<Value> {
    Ok(serde_json::json!([{ key: val }]))
}

fn show_all() -> Result<Value> {
    let rows = vec![
        kv("server_version", "14.0"),
        kv("client_encoding", "UTF8"),
        kv("standard_conforming_strings", "on"),
        kv("TimeZone", "UTC"),
        kv("search_path", crate::ident::DEFAULT_SCHEMA),
        kv("default_transaction_isolation", "read committed"),
        kv("transaction_read_only", "off"),
        kv("extra_float_digits", "3"),
    ];
    Ok(Value::Array(rows))
}

fn kv(k: &str, v: &str) -> Value { serde_json::json!({ k: v }) }

fn root_path(store: &SharedStore) -> std::path::PathBuf { let g = store.0.lock(); g.root_path().clone() }

fn show_schemas(store: &SharedStore) -> Result<Value> {
    // tvf df_show_schemas() returns schemas for all databases with columns:
    //   schema_database, schema_name
    // SHOW SCHEMAS (command) should restrict to current database context.
    let df_all = crate::server::exec::show::df_show_schemas(store)?;
//     let current_db = crate::system::get_current_database();
//     // Build boolean mask per Junie Polars guidelines
//     let col = df_all.column("schema_database")?;
//     let mut mask: Vec<bool> = Vec::with_capacity(col.len());
//     for i in 0..col.len() {
//         let keep = match col.get(i) {
//             Ok(v) => v.get_str().map(|s| s == current_db.as_str()).unwrap_or(false),
//             Err(_) => false,
//         };
//         mask.push(keep);
//     }
//     let mask_series = Series::new("__mask".into(), mask);
//     let df = df_all.filter(mask_series.bool()?)?;
    Ok(crate::server::exec::dataframe_to_json(&df_all))
}

fn show_tables(store: &SharedStore) -> Result<Value> {
    let df = crate::server::exec::show::df_show_tables(store)?;
    Ok(crate::server::exec::dataframe_to_json(&df))
}

fn show_objects(store: &SharedStore) -> Result<Value> {
    let df = crate::server::exec::show::df_show_objects(store)?;
    Ok(crate::server::exec::dataframe_to_json(&df))
}

fn show_scripts(store: &SharedStore) -> Result<Value> {
    let df = crate::server::exec::show::df_show_scripts(store)?;
    Ok(crate::server::exec::dataframe_to_json(&df))
}
