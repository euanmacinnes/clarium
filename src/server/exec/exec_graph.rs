//! exec_graph
//! ----------
//! GRAPH catalog management: CREATE/DROP/SHOW for sidecar `.graph` files stored
//! as `<db>/<schema>/<name>.graph`. These catalogs map logical labels and edge
//! types to backing tables and columns.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::{query, storage::SharedStore};
use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNodeDef { pub label: String, pub key: String, pub table: Option<String>, pub key_column: Option<String> }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdgeDef {
    pub r#type: String,
    pub from: String,
    pub to: String,
    pub table: Option<String>,
    pub src_column: Option<String>,
    pub dst_column: Option<String>,
    pub cost_column: Option<String>,
    pub time_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphFile {
    pub version: i32,
    pub name: String,
    pub qualified: String,
    pub nodes: Vec<GraphNodeDef>,
    pub edges: Vec<GraphEdgeDef>,
    pub created_at: Option<String>,
}

fn qualify_name(name: &str) -> String {
    let d = crate::system::current_query_defaults();
    crate::ident::qualify_regular_ident(name, &d)
}

fn path_for_graph(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p.set_extension("graph");
    p
}

fn read_graph_file(store: &SharedStore, qualified: &str) -> Result<Option<GraphFile>> {
    let path = path_for_graph(store, qualified);
    if !path.exists() { return Ok(None); }
    let text = std::fs::read_to_string(&path)?;
    let v: GraphFile = serde_json::from_str(&text)?;
    Ok(Some(v))
}

fn write_graph_file(store: &SharedStore, qualified: &str, gf: &GraphFile) -> Result<()> {
    let path = path_for_graph(store, qualified);
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
    std::fs::write(&path, serde_json::to_string_pretty(gf)?)?;
    Ok(())
}

fn delete_graph_file(store: &SharedStore, qualified: &str) -> Result<()> {
    let path = path_for_graph(store, qualified);
    if path.exists() { std::fs::remove_file(&path).ok(); }
    Ok(())
}

fn now_iso() -> String { chrono::Utc::now().to_rfc3339() }

fn list_graphs(store: &SharedStore) -> Result<Value> {
    let root = store.0.lock().root_path().clone();
    let mut out: Vec<serde_json::Value> = Vec::new();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = std::fs::read_dir(&db_path) {
                for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                    let sp = schema_dir.path();
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_file() && tp.extension().and_then(|s| s.to_str()) == Some("graph") {
                                if let Ok(text) = std::fs::read_to_string(&tp) {
                                    if let Ok(v) = serde_json::from_str::<GraphFile>(&text) {
                                        out.push(serde_json::json!({
                                            "name": v.name,
                                            "nodes": v.nodes.len(),
                                            "edges": v.edges.len()
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(Value::Array(out))
}

pub fn execute_graph(store: &SharedStore, cmd: query::Command) -> Result<Value> {
    match cmd {
        query::Command::CreateGraph { name, nodes, edges, nodes_table, edges_table } => {
            let qualified = qualify_name(&name);
            if read_graph_file(store, &qualified)?.is_some() {
                return Err(AppError::Conflict { code: "name_conflict".into(), message: format!("Graph already exists: {}", qualified) }.into());
            }
            // Map parsed tuples into GraphNodeDef/GraphEdgeDef; attach global tables if provided
            let mut node_defs: Vec<GraphNodeDef> = Vec::new();
            for (label, key) in nodes.into_iter() {
                node_defs.push(GraphNodeDef { label, key, table: nodes_table.clone(), key_column: None });
            }
            let mut edge_defs: Vec<GraphEdgeDef> = Vec::new();
            for (etype, from, to) in edges.into_iter() {
                edge_defs.push(GraphEdgeDef { r#type: etype, from, to, table: edges_table.clone(), src_column: None, dst_column: None, cost_column: None, time_column: None });
            }
            let gf = GraphFile { version: 1, name: qualified.clone(), qualified: qualified.clone(), nodes: node_defs, edges: edge_defs, created_at: Some(now_iso()) };
            write_graph_file(store, &qualified, &gf)?;
            info!(target: "clarium::ddl", "CREATE GRAPH saved '{}.graph'", qualified);
            Ok(serde_json::json!({"status":"ok"}))
        }
        query::Command::DropGraph { name } => {
            let qualified = qualify_name(&name);
            if read_graph_file(store, &qualified)?.is_none() {
                return Err(AppError::NotFound { code: "not_found".into(), message: format!("Graph not found: {}", qualified) }.into());
            }
            delete_graph_file(store, &qualified)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        query::Command::ShowGraph { name } => {
            let qualified = qualify_name(&name);
            if let Some(gf) = read_graph_file(store, &qualified)? {
                let row = serde_json::json!({
                    "name": gf.name,
                    "nodes": gf.nodes,
                    "edges": gf.edges
                });
                return Ok(serde_json::json!([row]));
            }
            return Err(AppError::NotFound { code: "not_found".into(), message: format!("Graph not found: {}", qualified) }.into());
        }
        query::Command::ShowGraphs => {
            list_graphs(store)
        }
        _ => Err(AppError::Ddl { code: "unsupported_graph".into(), message: "unsupported graph command".into() }.into()),
    }
}
