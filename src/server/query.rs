use anyhow::{Result, bail};

pub mod query_common;
pub mod query_parse_arith_expr;
pub mod query_parse_create;
pub mod query_parse_database;
pub mod query_parse_delete;
pub mod query_parse_drop;
pub mod query_parse_insert;
pub mod query_parse_misc;
pub mod query_parse_rename;
pub mod query_parse_select;
pub mod query_parse_select_list;
pub mod query_parse_show;
pub mod query_parse_slice;
pub mod query_parse_match;
pub mod query_parse_gc;
pub mod query_parse_update;
pub mod query_parse_user;
pub mod query_parse_where_tokens;
pub mod query_parse_where;
pub mod query_parse_txn;
pub mod query_parse_alter;
pub mod query_parse_vector;
pub mod query_parse_filestore;

// Import MATCH parser entrypoint for top-level dispatch
use crate::server::query::query_parse_match::parse_match;
use crate::server::query::query_parse_gc::parse_gc;

// Re-export common query types and helpers so existing paths like
// `crate::server::query::Query` continue to work after restructuring.
pub use query_common::*;
pub use query_parse_create::*;
pub use query_parse_database::*;
pub use query_parse_delete::*;
pub use query_parse_drop::*;
pub use query_parse_insert::*;
pub use query_parse_misc::*;
pub use query_parse_rename::*;
pub use query_parse_select_list::*;
pub use query_parse_select::*;
pub use query_parse_show::*;
pub use query_parse_slice::*;
pub use query_parse_match::*;
pub use query_parse_gc::*;
pub use query_parse_update::*;
pub use query_parse_user::*;
pub use query_parse_where_tokens::*;
pub use query_parse_where::*;
pub use query_parse_alter::*;
pub use query_parse_vector::*;
pub use query_parse_filestore::*;



#[derive(Debug, Clone)]
pub enum Command {
    Select(Query),
    // UNION or UNION ALL of multiple SELECT queries
    SelectUnion { queries: Vec<Query>, all: bool },
    // VIEW DDL
    // CREATE [OR ALTER] VIEW <name> AS <SELECT...>
    CreateView { name: String, or_alter: bool, definition_sql: String },
    // DROP VIEW [IF EXISTS] <name>
    DropView { name: String, if_exists: bool },
    // SHOW VIEW <name>
    ShowView { name: String },
    Calculate { target_sensor: String, query: Query },
    // UPDATE <table> SET col = value[, ...] [WHERE ...]
    Update { table: String, assignments: Vec<(String, ArithTerm)>, where_clause: Option<WhereExpr> },
    DeleteRows { database: String, where_clause: Option<WhereExpr> },
    DeleteColumns { database: String, columns: Vec<String>, where_clause: Option<WhereExpr> },
    SchemaShow { database: String },
    // Allow optional PRIMARY KEY and PARTITION BY on schema additions for regular tables
    SchemaAdd { database: String, entries: Vec<(String, String)>, primary_key: Option<Vec<String>>, partitions: Option<Vec<String>> },
    // Legacy/compat
    DatabaseAdd { database: String },
    DatabaseDelete { database: String },
    // New DDL
    CreateDatabase { name: String },
    DropDatabase { name: String },
    RenameDatabase { from: String, to: String },
    CreateSchema { path: String },
    DropSchema { path: String },
    RenameSchema { from: String, to: String },
    CreateTimeTable { table: String },
    DropTimeTable { table: String },
    RenameTimeTable { from: String, to: String },
    // Regular parquet table DDL
    // Optional PRIMARY KEY / PARTITION BY metadata on create
    CreateTable { table: String, primary_key: Option<Vec<String>>, partitions: Option<Vec<String>> },
    DropTable { table: String, if_exists: bool },
    RenameTable { from: String, to: String },
    // ALTER TABLE for regular tables
    AlterTable { table: String, ops: Vec<AlterOp> },
    // KV store/keys DDL/DML
    CreateStore { database: String, store: String },
    DropStore { database: String, store: String },
    RenameStore { database: String, from: String, to: String },
    // Discovery/listing
    ListStores { database: String },
    ListKeys { database: String, store: String },
    DescribeKey { database: String, store: String, key: String },
    // Key operations
    WriteKey { database: String, store: String, key: String, value: String, ttl_ms: Option<i64>, reset_on_access: Option<bool> },
    ReadKey { database: String, store: String, key: String },
    DropKey { database: String, store: String, key: String },
    RenameKey { database: String, store: String, from: String, to: String },
    // Scripts/bytecode cache maintenance
    ClearScriptCache { scope: ScriptCacheScope, persistent: bool },
    UserAdd { username: String, password: String, is_admin: bool, perms: Vec<String>, scope_db: Option<String> },
    UserDelete { username: String, scope_db: Option<String> },
    UserAlter { username: String, new_password: Option<String>, is_admin: Option<bool>, perms: Option<Vec<String>>, scope_db: Option<String> },
    // Scripts
    CreateScript { path: String, code: String },
    DropScript { path: String },
    RenameScript { from: String, to: String },
    LoadScript { path: Option<String> },
    // Global session-affecting commands
    UseDatabase { name: String },
    UseSchema { name: String },
    Set { variable: String, value: String },
    // SHOW commands
    ShowTransactionIsolation,
    ShowStandardConformingStrings,
    ShowServerVersion,
    ShowClientEncoding,
    ShowServerEncoding,
    ShowDateStyle,
    ShowIntegerDateTimes,
    ShowTimeZone,
    ShowSearchPath,
    ShowDefaultTransactionIsolation,
    ShowTransactionReadOnly,
    ShowApplicationName,
    ShowExtraFloatDigits,
    ShowAll,
    ShowSchemas,
    ShowTables,
    ShowObjects,
    ShowScripts,
    // Vector index catalog
    CreateVectorIndex { name: String, table: String, column: String, algo: String, options: Vec<(String, String)> },
    DropVectorIndex { name: String },
    ShowVectorIndex { name: String },
    ShowVectorIndexes,
    AlterVectorIndexSetMode { name: String, mode: String },
    // Vector index lifecycle
    BuildVectorIndex { name: String, options: Vec<(String, String)> },
    ReindexVectorIndex { name: String },
    ShowVectorIndexStatus { name: Option<String> },
    // Graph catalog
    // Optional integration with GraphStore engine via `USING GRAPHSTORE [CONFIG <name>] [WITH (k=v, ...)]`.
    CreateGraph {
        name: String,
        nodes: Vec<(String, String)>,
        edges: Vec<(String, String, String)>,
        nodes_table: Option<String>,
        edges_table: Option<String>,
        // New optional GraphStore clause
        graph_engine: Option<String>,                 // e.g., Some("graphstore")
        graphstore_config: Option<String>,            // named configuration to use
        graphstore_options: Option<Vec<(String, String)>>, // inline options
    },
    DropGraph { name: String },
    ShowGraph { name: String },
    ShowGraphs,
    // Graph runtime status
    ShowGraphStatus { name: Option<String> },
    // Session graph defaults
    UseGraph { name: String },
    UnsetGraph,
    ShowCurrentGraph,
    // Transactional GraphStore DDL (experimental)
    // BEGIN [GRAPH <name>]
    BeginGraphTxn { graph: Option<String> },
    // COMMIT; ABORT; (prepared for future general DML too)
    CommitGraphTxn,
    AbortGraphTxn,
    // INSERT NODE <Label> KEY <key_literal> [AS <node_id>] [GRAPH <name>]
    InsertNodeTxn { graph: Option<String>, label: String, key: String, node_id: Option<u64> },
    // INSERT EDGE <src_id> -> <dst_id> [ETYPE <etype_id>] [PART <n>] [GRAPH <name>]
    InsertEdgeTxn { graph: Option<String>, src: u64, dst: u64, etype_id: Option<u16>, part: Option<u32> },
    // MATCH (rewritten to SELECT)
    MatchRewrite { sql: String },
    // GC DDL
    GcGraph { name: Option<String> },
    // DESCRIBE <object> (table/view) and DESCRIBE KEY ... (existing)
    // For backward compatibility, DESCRIBE KEY is parsed specially; otherwise
    // we treat DESCRIBE <object> as DescribeObject with a possibly unqualified name.
    DescribeObject { name: String },
    Slice(SlicePlan),
    Insert { table: String, columns: Vec<String>, values: Vec<Vec<ArithTerm>> },
    // EXPLAIN <stmt>
    Explain { sql: String },
    // FILESTORE SHOW variants
    ShowFilestores { database: Option<String> },
    ShowFilestoreConfig { filestore: String, folder_prefix: Option<String> },
    ShowFilesInFilestore { filestore: String, prefix: Option<String>, limit: Option<i64>, offset: Option<i64> },
    ShowTreesInFilestore { filestore: String },
    ShowCommitsInFilestore { filestore: String },
    ShowDiffInFilestore { filestore: String, left_tree_id: String, right_tree_id: Option<String>, live_prefix: Option<String> },
    ShowChunksInFilestore { filestore: String },
    ShowAliasesInFilestore { filestore: String },
    ShowAdminInFilestore { filestore: String },
    ShowHealthInFilestore { filestore: String },
    // FILESTORE DDL/mutations/versioning
    CreateFilestoreCmd { filestore: String, cfg_json: Option<String> },
    AlterFilestoreCmd { filestore: String, update_json: String },
    DropFilestoreCmd { filestore: String, force: bool },
    IngestFileFromBytesCmd { filestore: String, logical_path: String, payload: String, content_type: Option<String> },
    IngestFileFromHostPathCmd { filestore: String, logical_path: String, host_path: String, content_type: Option<String> },
    UpdateFileFromBytesCmd { filestore: String, logical_path: String, if_match: String, payload: String, content_type: Option<String> },
    RenameFilePathCmd { filestore: String, from: String, to: String },
    DeleteFilePathCmd { filestore: String, logical_path: String },
    CreateTreeCmd { filestore: String, prefix: Option<String> },
    CommitTreeCmd { filestore: String, tree_id: String, parents: Vec<String>, branch: Option<String>, author_name: Option<String>, author_email: Option<String>, message: Option<String>, tags: Vec<String> },
}

#[derive(Debug, Clone)]
pub enum ScriptCacheScope {
    All,
    CurrentSchema, // default scope if not specified; execution layer will map to defaults
    Name(String),
}






pub fn parse(input: &str) -> Result<Command> {
    let s = input.trim();
    let sup = s.to_uppercase();
    if sup.starts_with("EXPLAIN ") {
        let rest = s[7..].trim();
        if rest.is_empty() { bail!("EXPLAIN requires a statement"); }
        return Ok(Command::Explain { sql: rest.to_string() });
    }
    // Vector lifecycle commands (BUILD/REINDEX/SHOW STATUS)
    if let Some(res) = parse_vector_ddl(s) { return res; }
    if sup.starts_with("SLICE ") || sup == "SLICE" {
        let plan = parse_slice(s)?;
        return Ok(Command::Slice(plan));
    }
    if sup.starts_with("CALCULATE ") {
        // CALCULATE sensor_1, _time as SELECT ...
        let rest = s[10..].trim();
        let parts: Vec<&str> = rest.splitn(2, " as ").collect();
        if parts.len() != 2 {
            bail!("Invalid CALCULATE syntax");
        }
        let left = parts[0].trim();
        let target_sensor = left.split(',').next().unwrap().trim().to_string();
        let select_part = parts[1].trim();
        let q = parse_select(select_part)?;
        return Ok(Command::Calculate { target_sensor, query: q });
    }
    if sup.starts_with("WITH ") || sup.starts_with("SELECT") {
        // Detect UNION / UNION ALL at top-level using a parser that respects nesting
        let (parts, all) = split_union_queries(s)?;
        if parts.len() > 1 {
            let mut queries: Vec<Query> = Vec::new();
            for part in parts { queries.push(parse_select(part)?); }
            return Ok(Command::SelectUnion { queries, all });
        } else {
            let q = parse_select(s)?;
            return Ok(Command::Select(q));
        }
    }
    if sup.starts_with("MATCH ") || sup == "MATCH" {
        return parse_match(s);
    }
    if sup.starts_with("GC ") || sup == "GC" {
        return parse_gc(s);
    }
    if sup.starts_with("SHOW ") || sup == "SHOW" {
        return parse_show(s);
    }
    // FILESTORE commands
    if sup.starts_with("CREATE FILESTORE")
        || sup.starts_with("ALTER FILESTORE")
        || sup.starts_with("DROP FILESTORE")
        || sup.starts_with("INGEST FILESTORE")
        || sup.starts_with("UPDATE FILESTORE")
        || sup.starts_with("RENAME FILESTORE")
        || sup.starts_with("DELETE FILESTORE")
        || sup.starts_with("CREATE TREE IN FILESTORE")
        || sup.starts_with("COMMIT TREE IN FILESTORE")
    {
        return query_parse_filestore::parse_filestore(s);
    }
    if sup.starts_with("USE ") {
        return parse_use(s);
    }
    if sup.starts_with("LOAD ") {
        return parse_load(s);
    }
    if sup.starts_with("DELETE ") {
        return parse_delete(s);
    }
    if sup.starts_with("UPDATE ") {
        return parse_update(s);
    }
    if sup.starts_with("SCHEMA ") {
        return parse_schema(s);
    }
    if sup.starts_with("DATABASE ") {
        return parse_database(s);
    }
    if sup.starts_with("CREATE ") {
        return parse_create(s);
    }
    if sup.starts_with("DROP ") {
        return parse_drop(s);
    }
    if sup.starts_with("RENAME ") {
        return parse_rename(s);
    }
    if sup.starts_with("ALTER ") {
        return parse_alter(s);
    }
    if sup.starts_with("WRITE ") {
        return parse_write(s);
    }
    if sup.starts_with("READ ") {
        return parse_read(s);
    }
    if sup.starts_with("LIST ") {
        return parse_list(s);
    }
    if sup.starts_with("DESCRIBE ") {
        return parse_describe(s);
    }
    if sup.starts_with("USER ") {
        return parse_user(s);
    }
    if sup.starts_with("SET ") {
        return parse_set(s);
    }
    if sup.starts_with("CLEAR ") {
        return parse_clear(s);
    }
    // GraphStore transactional inserts take precedence over regular SQL INSERT
    if sup.starts_with("INSERT NODE") || sup.starts_with("INSERT EDGE") || sup == "BEGIN" || sup.starts_with("BEGIN ") || sup == "COMMIT" || sup == "ABORT" {
        return crate::server::query::query_parse_txn::parse_txn(s);
    }
    if sup.starts_with("INSERT ") {
        return parse_insert(s);
    }
    bail!("Unsupported DDL-SQL command: {} ", sup)
}


fn parse_load(s: &str) -> Result<Command> {
    // LOAD SCRIPT <path> | LOAD SCRIPT ALL
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("SCRIPT ") {
        let arg = rest[7..].trim();
        if arg.eq_ignore_ascii_case("ALL") { return Ok(Command::LoadScript { path: None }); }
        if arg.is_empty() { anyhow::bail!("Invalid LOAD SCRIPT: missing name"); }
        return Ok(Command::LoadScript { path: Some(arg.to_string()) });
    }
    anyhow::bail!("Invalid LOAD syntax")
}

fn parse_clear(s: &str) -> Result<Command> {
    // CLEAR SCRIPT CACHE [ALL | NAME <ident>] [WITH PERSISTENT]
    let rest = s[5..].trim();
    let up = rest.to_uppercase();
    if !up.starts_with("SCRIPT ") { anyhow::bail!("Unsupported CLEAR command"); }
    let rest2 = rest[6..].trim();
    let up2 = rest2.to_uppercase();
    if !up2.starts_with("CACHE") { anyhow::bail!("Unsupported CLEAR SCRIPT command; expected CACHE"); }
    let mut tail = rest2[5..].trim();
    let mut scope = ScriptCacheScope::CurrentSchema;
    let mut persistent = false;
    if !tail.is_empty() {
        let up3 = tail.to_uppercase();
        if up3.starts_with("ALL") {
            scope = ScriptCacheScope::All;
            tail = tail[3..].trim();
        } else if up3.starts_with("NAME ") {
            let ident = tail[5..].trim();
            if ident.is_empty() { anyhow::bail!("CLEAR SCRIPT CACHE NAME: missing identifier"); }
            // Allow quoted identifiers
            let ident = ident.trim_matches(['"', '\'']);
            scope = ScriptCacheScope::Name(ident.to_string());
            tail = "";
        }
        // Optional WITH PERSISTENT
        let up4 = tail.to_uppercase();
        if up4.contains("WITH") && up4.contains("PERSISTENT") { persistent = true; }
    }
    Ok(Command::ClearScriptCache { scope, persistent })
}





#[cfg(test)]
mod tests;

