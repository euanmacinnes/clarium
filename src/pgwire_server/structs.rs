use std::collections::HashMap;
use crate::identity::Principal;
use pub_fields::pub_fields;

#[derive(Clone)]
#[pub_fields]
pub(crate) struct PreparedStatement {
    name: String,
    sql: String,
    param_types: Vec<i32>,
}

#[derive(Clone)]
#[pub_fields]
pub(crate) struct Portal {
    name: String,
    stmt_name: String,
    // Store raw text parameters (None for NULL)
    params: Vec<Option<String>>,
    param_formats: Vec<i16>,
    result_formats: Vec<i16>,
}

#[pub_fields]
pub(crate) struct ConnState {
    current_database: String,
    current_schema: String,
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    // if an error occurred in extended flow, we keep going until Sync
    in_error: bool,
    // inside explicit transaction block (BEGIN..)
    in_tx: bool,
    // unified identity principal for this connection (if authenticated)
    principal: Option<Principal>,
    // opaque session token when using LocalAuthProvider (optional)
    session_token: Option<String>,
}

#[derive(Debug, Clone)]
#[pub_fields]
pub(crate) struct InsertStmt { database: String, columns: Vec<String>, values: Vec<InsertValue> }

#[derive(Debug, Clone)]
pub(crate) enum InsertValue { Null, Number(i64), String(String) }