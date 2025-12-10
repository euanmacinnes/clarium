use std::collections::HashMap;

#[derive(Clone)]
pub struct PreparedStatement {
    name: String,
    sql: String,
    param_types: Vec<i32>,
}

#[derive(Clone)]
pub struct Portal {
    name: String,
    stmt_name: String,
    // Store raw text parameters (None for NULL)
    params: Vec<Option<String>>,
    param_formats: Vec<i16>,
    result_formats: Vec<i16>,
}

pub struct ConnState {
    current_database: String,
    current_schema: String,
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    // if an error occurred in extended flow, we keep going until Sync
    in_error: bool,
    // inside explicit transaction block (BEGIN..)
    in_tx: bool,
}

#[derive(Debug, Clone)]
pub struct InsertStmt { database: String, columns: Vec<String>, values: Vec<InsertValue> }

#[derive(Debug, Clone)]
pub enum InsertValue { Null, Number(i64), String(String) }