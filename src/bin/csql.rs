//!
//! clarium CLI binary
//! -------------------
//! Command-line tool and interactive interpreter for interacting with a local
//! clarium store or a remote clarium HTTP API. In REPL mode, supports a
//! `connect` command to authenticate and run queries against a server.

use std::env;
use std::fs;
use std::io::{self, Write};

use anyhow::{Context, Result};
use reqwest::Url;

use clarium::server::exec::execute_query_safe;
use clarium::storage::SharedStore;
use clarium::cli::print_query_result;
use clarium::cli::connectivity::*;

// Use the shared library module for table rendering (defined in src/cli_table.rs)

fn print_usage(program: &str) {
    eprintln!(
        "Usage:\n  {program} --query \"<SQL>\" [--root <db_root>] [--connect <url>] [--user <u>] [--password <p>] [--database <db>] [--schema <sch>]\n  {program} -q \"<SQL>\" [--root <db_root>] [--connect <url>] [--user <u>] [--password <p>] [--database <db>] [--schema <sch>]\n  {program} [--root <db_root>]    # reads query text from stdin (local by default)\n  {program} --repl [--root <db_root>] [--connect <url>] [--user <u>] [--password <p>] [--database <db>] [--schema <sch>]  # start interactive interpreter\n\nFlags:\n  --root <path>            Local database root for local queries (default: dbs\\clarium)\n  --connect <url>          Auto-connect using URL scheme to select transport:\n                           http/https → HTTP API; ws/wss → WebSocket; postgres/postgresql → pgwire\n  --user <u>               Username (can also be in URL)\n  --password <p>           Password (can also be in URL)\n  --database <db>          Optional current database (can also be in URL or implied by pg DSN)\n  --schema <sch>           Optional current schema to select (remote modes)\n  --repl                   Start interactive mode\n  -q, --query <SQL>        Run a one-shot query (from arg); if omitted, reads from stdin\n  -h, --help               Show this help\n\nInteractive commands:\n  connect <url> <user> <password>   connect to remote server; transport inferred by URL scheme\n  disconnect                         end remote session (subsequent queries run locally)\n  use database <name>                set current database on remote session\n  use schema <name>                  set current schema on remote session\n  status                             show current connection info\n  help                               show this help\n  quit | exit                        exit the interpreter\n  <SQL>                              run a SQL-like query locally (no connect) or remotely (after connect)\n\nExamples:\n  {program} --query \"SCHEMA SHOW clarium\"\n  {program} --connect http://127.0.0.1:7878 --user clarium --password clarium --query \"SELECT COUNT(*) FROM public.demo\"\n  {program} --connect ws://127.0.0.1:7878 --user clarium --password clarium --query \"SELECT COUNT(*) FROM public.demo\"\n  {program} --connect postgres://clarium:clarium@127.0.0.1:5433/clarium --schema public -q \"SELECT COUNT(*) FROM demo\"\n  {program} --repl --connect http://127.0.0.1:7878 --user clarium --password clarium --database clarium --schema public\n    > SELECT AVG(value) FROM demo BY 5m\n\nDefaults:\n  --root defaults to dbs\\clarium relative to current working directory."
    );
}


/// Entry point for the clarium CLI. Parses flags and either runs a one-shot
/// query (from --query or stdin) against a local store, or starts the interactive
/// interpreter which can connect to a remote clarium server.
fn main() -> Result<()> {
        println!(r"   ________           _               
  / ____/ /___ ______(_)_  ______ ___ 
 / /   / / __ `/ ___/ / / / / __ `__ \
/ /___/ / /_/ / /  / / /_/ / / / / / /
\____/_/\__,_/_/  /_/\__,_/_/ /_/ /_/  
       Command Line Interface");
    // Initialize tracing subscriber so script load errors are visible on the command line
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let mut args: Vec<String> = env::args().collect();
    let program = args.remove(0);

    let mut root: Option<String> = None;
    let mut query: Option<String> = None;
    let mut repl: bool = false;
    // Remote connection flags
    let mut connect_url: Option<String> = None;
    let mut connect_user: Option<String> = None;
    let mut connect_password: Option<String> = None;
    let mut connect_db: Option<String> = None;
    let mut connect_schema: Option<String> = None;

    // System view generator flags
    let mut gen_system_views: bool = false;
    let mut gen_out_dir: Option<String> = None;
    let mut gen_overwrite: bool = false;
    let mut gen_dry_run: bool = false;

    // System tables checker flags
    let mut check_system_tables: bool = false;
    let mut check_strict: bool = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                if i + 1 >= args.len() { eprintln!("--root requires a value"); print_usage(&program); std::process::exit(2); }
                root = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--connect" => {
                if i + 1 >= args.len() { eprintln!("--connect requires a URL"); print_usage(&program); std::process::exit(2); }
                connect_url = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--user" => {
                if i + 1 >= args.len() { eprintln!("--user requires a value"); print_usage(&program); std::process::exit(2); }
                connect_user = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--password" => {
                if i + 1 >= args.len() { eprintln!("--password requires a value"); print_usage(&program); std::process::exit(2); }
                connect_password = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--database" => {
                if i + 1 >= args.len() { eprintln!("--database requires a value"); print_usage(&program); std::process::exit(2); }
                connect_db = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--schema" => {
                if i + 1 >= args.len() { eprintln!("--schema requires a value"); print_usage(&program); std::process::exit(2); }
                connect_schema = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--query" | "-q" => {
                if i + 1 >= args.len() { eprintln!("--query requires a value"); print_usage(&program); std::process::exit(2); }
                query = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--repl" => { repl = true; i += 1; continue; }
            // --- System .view generator flags ---
            "--gen-system-views" => { gen_system_views = true; i += 1; continue; }
            "--out-dir" => {
                if i + 1 >= args.len() { eprintln!("--out-dir requires a value"); print_usage(&program); std::process::exit(2); }
                gen_out_dir = Some(args[i+1].clone()); i += 2; continue;
            }
            "--overwrite" => { gen_overwrite = true; i += 1; continue; }
            "--dry-run" => { gen_dry_run = true; i += 1; continue; }
            // --- System tables checker flags ---
            "--check-system-tables" => { check_system_tables = true; i += 1; continue; }
            "--strict" => { check_strict = true; i += 1; continue; }
            "-h" | "--help" => {
                print_usage(&program);
                return Ok(());
            }
            unk => {
                // Allow passing query without flag as a single arg
                if query.is_none() { query = Some(unk.to_string()); i += 1; continue; }
                eprintln!("Unrecognized argument: {}", unk);
                print_usage(&program);
                std::process::exit(2);
            }
        }
    }

    let root_path = root.unwrap_or_else(|| {
        // default to dbs\\clarium relative to CWD
        let default = if cfg!(windows) { "dbs\\clarium" } else { "dbs/clarium" };
        default.to_string()
    });

    // Ensure root directory exists (create if missing for local mode)
    if let Err(e) = fs::create_dir_all(&root_path) { eprintln!("Failed to ensure root directory '{}': {}", root_path, e); }

    // Command-argument-gated: generate system .view files from plans markdown and exit
    if gen_system_views {
        let out_dir = gen_out_dir.unwrap_or_else(|| {
            // default to repo scripts/system_views
            if cfg!(windows) { "scripts\\system_views".to_string() } else { "scripts/system_views".to_string() }
        });
        let opts = clarium::tools::viewgen::GenOptions {
            out_dir: std::path::PathBuf::from(&out_dir),
            overwrite: gen_overwrite,
            dry_run: gen_dry_run,
        };
        match clarium::tools::viewgen::generate_system_views(&opts) {
            Ok(n) => {
                eprintln!("[viewgen] generated {} .view file(s) by scanning '{}' for original_schema_views.md{}{}",
                    n, out_dir,
                    if gen_overwrite { " (overwrite)" } else { "" },
                    if gen_dry_run { " [dry-run]" } else { "" }
                );
                return Ok(());
            }
            Err(e) => {
                eprintln!("[viewgen] error: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Command-argument-gated: check system tables vs registry and exit
    if check_system_tables {
        let root = gen_out_dir.unwrap_or_else(|| {
            if cfg!(windows) { "scripts\\system_views".to_string() } else { "scripts/system_views".to_string() }
        });
        let opts = clarium::tools::tablecheck::CheckOptions {
            root_dir: std::path::PathBuf::from(&root),
            strict: check_strict,
        };
        match clarium::tools::tablecheck::check_system_tables(&opts) {
            Ok(diff_count) => {
                eprintln!("[tablecheck] scanned '{}' and found {} difference(s){}",
                    root,
                    diff_count,
                    if check_strict && diff_count > 0 { " [STRICT]" } else { "" }
                );
                if check_strict && diff_count > 0 { std::process::exit(3); }
                return Ok(());
            }
            Err(e) => {
                eprintln!("[tablecheck] error: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Build store for local mode
    let store = SharedStore::new(&root_path).with_context(|| format!("Failed to open store at {}", root_path))?;

    // Tokio runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build Tokio runtime")?;

    // Optional: single-shot query execution if --query or positional provided.
    // We do NOT read from stdin when missing; after this we always enter REPL.
    if let Some(qtext) = query {
        // If connect flags were provided, execute remotely; otherwise run locally
        let result = if let Some(url) = connect_url.clone() {
            // Determine transport by URL scheme; support credentials in URL or flags
            let url_parsed = Url::parse(&url).expect("invalid --connect url");
            let scheme = url_parsed.scheme().to_ascii_lowercase();
            if scheme == "http" || scheme == "https" {
                let user = connect_user.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| url_parsed.username().to_string());
                let pass = connect_password.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| url_parsed.password().unwrap_or("").to_string());
                if user.is_empty() || pass.is_empty() { eprintln!("HTTP/WS modes require --user and --password or credentials in the URL"); std::process::exit(2); }
                let session_res = rt.block_on(async { HttpSession::connect(&url, &user, &pass).await });
                match session_res {
                    Ok(session) => {
                        if let Some(db) = connect_db.as_deref() { let _ = rt.block_on(async { session.use_database(db).await }); }
                        if let Some(sch) = connect_schema.as_deref() { let _ = rt.block_on(async { session.use_schema(sch).await }); }
                        rt.block_on(async { session.post_query(&qtext).await })
                    }
                    Err(e) => Err(e),
                }
            } else if scheme == "ws" || scheme == "wss" {
                let user = connect_user.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| url_parsed.username().to_string());
                let pass = connect_password.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| url_parsed.password().unwrap_or("").to_string());
                if user.is_empty() || pass.is_empty() { eprintln!("HTTP/WS modes require --user and --password or credentials in the URL"); std::process::exit(2); }
                // Convert ws(s) URL to http(s) base for login + csrf
                let http_base = {
                    let mut u = url_parsed.clone();
                    if scheme == "wss" { u.set_scheme("https").ok(); } else { u.set_scheme("http").ok(); }
                    // strip any path/query/fragment; keep host and port
                    u.set_path("");
                    u.set_query(None);
                    u.set_fragment(None);
                    u
                };
                let http_session = rt.block_on(async { HttpSession::connect(http_base.as_str(), &user, &pass).await });
                match http_session {
                    Ok(h) => {
                        let ws = rt.block_on(async { WsSession::from_http_session(&h).await }).unwrap();
                        if let Some(db) = connect_db.as_deref() { let _ = rt.block_on(async { ws.use_database(db).await }); }
                        if let Some(sch) = connect_schema.as_deref() { let _ = rt.block_on(async { ws.use_schema(sch).await }); }
                        rt.block_on(async { ws.post_query(&qtext).await })
                    }
                    Err(e) => Err(e),
                }
            } else if scheme == "postgres" || scheme == "postgresql" {
                let schema = connect_schema.clone();
                let pg = rt.block_on(async { PgSession::connect(&url, schema).await });
                match pg {
                    Ok(p) => rt.block_on(async { p.post_query(&qtext).await }),
                    Err(e) => Err(e),
                }
            } else {
                eprintln!("Unsupported --connect scheme: {}", scheme);
                std::process::exit(2);
            }
        } else {
            println!("Running natively on database in this folder");
            rt.block_on(async { execute_query_safe(&store, &qtext).await })
        };

        match result {
            Ok(val) => {
                if !print_query_result(&val) {
                    let s = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string());
                    println!("{}", s);
                }
            }
            Err(err) => {
                eprintln!("Error: {}", err);
            }
        }
    }

    // Always start the REPL after handling any optional single-shot query
    // Before entering the prompt loop, print the current database context
    if connect_url.is_some() {
        let db_msg = connect_db.as_deref().unwrap_or("(server default)");
        println!("Current database: {}", db_msg);
    } else {
        let initial_local_db = connect_db.clone().unwrap_or_else(|| "clarium".to_string());
        let initial_local_schema = connect_schema.clone().unwrap_or_else(|| "public".to_string());
        println!("Local context:\n  root: {}\n  database: {}\n  schema: {}", root_path, initial_local_db, initial_local_schema);
    }

    return run_repl_with_autoconnect(
        rt,
        store,
        root_path,
        connect_url,
        connect_user,
        connect_password,
        connect_db,
        connect_schema,
    );
}

fn run_repl_with_autoconnect(
    rt: tokio::runtime::Runtime,
    store: SharedStore,
    local_root_path: String,
    connect_url: Option<String>,
    connect_user: Option<String>,
    connect_password: Option<String>,
    connect_db: Option<String>,
    connect_schema: Option<String>,
) -> Result<()> {
    let mut session: Option<RemoteTransport> = None;
    // Track local database/schema context even when not connected
    let mut local_db: String = connect_db.clone().unwrap_or_else(|| "clarium".to_string());
    let mut local_schema: String = connect_schema.clone().unwrap_or_else(|| "public".to_string());
    if let Some(url) = connect_url {
        match (connect_user.as_deref(), connect_password.as_deref()) {
            (Some(user), Some(pass)) => {
                let scheme = Url::parse(&url).map(|u| u.scheme().to_string()).unwrap_or_else(|_| "".to_string());
                if scheme == "http" || scheme == "https" {
                    match rt.block_on(async { HttpSession::connect(&url, user, pass).await }) {
                        Ok(s) => {
                            if let Some(db) = connect_db.as_deref() { let _ = rt.block_on(async { s.use_database(db).await }); }
                            if let Some(sch) = connect_schema.as_deref() { let _ = rt.block_on(async { s.use_schema(sch).await }); }
                            println!("connected (http) to {}", url);
                            let db_msg = connect_db.as_deref().unwrap_or("(server default)");
                            let sch_msg = connect_schema.as_deref().unwrap_or("(server default)");
                            println!("Using database: {}; schema: {}", db_msg, sch_msg);
                            session = Some(RemoteTransport::Http(s));
                        }
                        Err(e) => eprintln!("auto-connect failed: {}", e),
                    }
                } else if scheme == "ws" || scheme == "wss" {
                    // Map to http(s) base for auth
                    let mut u = Url::parse(&url).unwrap();
                    if scheme == "wss" { u.set_scheme("https").ok(); } else { u.set_scheme("http").ok(); }
                    u.set_path(""); u.set_query(None); u.set_fragment(None);
                    match rt.block_on(async { HttpSession::connect(u.as_str(), user, pass).await }) {
                        Ok(h) => {
                            let ws = rt.block_on(async { WsSession::from_http_session(&h).await }).unwrap();
                            if let Some(db) = connect_db.as_deref() { let _ = rt.block_on(async { ws.use_database(db).await }); }
                            if let Some(sch) = connect_schema.as_deref() { let _ = rt.block_on(async { ws.use_schema(sch).await }); }
                            println!("connected (ws) to {}", url);
                            let db_msg = connect_db.as_deref().unwrap_or("(server default)");
                            let sch_msg = connect_schema.as_deref().unwrap_or("(server default)");
                            println!("Using database: {}; schema: {}", db_msg, sch_msg);
                            session = Some(RemoteTransport::Ws(ws));
                        }
                        Err(e) => eprintln!("auto-connect failed: {}", e),
                    }
                } else if scheme == "postgres" || scheme == "postgresql" {
                    match rt.block_on(async { PgSession::connect(&url, connect_schema.clone()).await }) {
                        Ok(p) => {
                            println!("connected (pg) to {}", url);
                            let sch_msg = connect_schema.as_deref().unwrap_or("(server default)");
                            println!("Using schema: {} (database in URL)", sch_msg);
                            session = Some(RemoteTransport::Pg(p));
                        }
                        Err(e) => eprintln!("auto-connect failed: {}", e),
                    }
                } else {
                    eprintln!("--connect: unsupported URL scheme '{}'; starting REPL without remote connection", scheme);
                }
            }
            _ => eprintln!("--connect used without --user/--password; starting REPL without remote connection"),
        }
    }

    // Enter REPL, starting with existing session if any
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut input = String::new();
    println!("clarium-cli interpreter. Type 'help' for commands.");
    loop {
        input.clear();
        print!("> "); let _ = stdout.flush();
        if stdin.read_line(&mut input).is_err() { break; }
        let line = input.trim();
        if line.is_empty() { continue; }
        let up = line.to_uppercase();
        if up == "EXIT" || up == "QUIT" { break; }
        if up == "HELP" { print_usage("clarium_cli"); continue; }
        if up.starts_with("CONNECT ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 { eprintln!("usage: connect <url> <user> <password>"); continue; }
            let url = parts[1]; let user = parts[2]; let pass = parts[3];
            let scheme = Url::parse(url).map(|u| u.scheme().to_string()).unwrap_or_else(|_| "".to_string());
            if scheme == "http" || scheme == "https" {
                match rt.block_on(async { HttpSession::connect(url, user, pass).await }) {
                    Ok(ses) => { println!("connected (http) to {}", url); session = Some(RemoteTransport::Http(ses)); }
                    Err(e) => { eprintln!("connect failed: {}", e); }
                }
            } else if scheme == "ws" || scheme == "wss" {
                match rt.block_on(async { HttpSession::connect(url, user, pass).await }) {
                    Ok(h) => { let ws = rt.block_on(async { WsSession::from_http_session(&h).await }).unwrap(); println!("connected (ws) to {}", url); session = Some(RemoteTransport::Ws(ws)); }
                    Err(e) => { eprintln!("connect failed: {}", e); }
                }
            } else if scheme == "postgres" || scheme == "postgresql" {
                match rt.block_on(async { PgSession::connect(url, None).await }) {
                    Ok(p) => { println!("connected (pg) to {}", url); session = Some(RemoteTransport::Pg(p)); }
                    Err(e) => { eprintln!("connect failed: {}", e); }
                }
            } else { eprintln!("unsupported scheme in url: {}", url); }
            continue;
        }
        if up == "DISCONNECT" {
            if session.is_some() { session = None; println!("disconnected"); }
            else { println!("not connected"); }
            continue;
        }
        if up == "STATUS" {
            if let Some(s) = &session {
                println!("connected: {}", s.ident());
            } else {
                println!("local (not connected)\n  root: {}\n  database: {}\n  schema: {}", local_root_path, local_db, local_schema);
            }
            continue;
        }
        if up.starts_with("USE DATABASE ") {
            let name = line[13..].trim();
            if let Some(s) = &session {
                if let Err(e) = rt.block_on(async { s.use_database(name).await }) { eprintln!("error: {}", e); }
            } else {
                // Update local context
                local_db = name.to_string();
                println!("Using database (local): {}", local_db);
            }
            continue;
        }
        if up.starts_with("USE SCHEMA ") {
            let name = line[11..].trim();
            if let Some(s) = &session {
                if let Err(e) = rt.block_on(async { s.use_schema(name).await }) { eprintln!("error: {}", e); }
            } else {
                // Update local context
                local_schema = name.to_string();
                println!("Using schema (local): {}", local_schema);
            }
            continue;
        }
        if let Some(s) = &session {
            match rt.block_on(async { s.post_query(line).await }) {
                Ok(val) => { let pretty = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string()); println!("{}", pretty); }
                Err(e) => eprintln!("error: {}", e),
            }
        } else {
            match rt.block_on(async { execute_query_safe(&store, line).await }) {
                Ok(val) => { let pretty = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string()); println!("{}", pretty); }
                Err(e) => eprintln!("error: {}", e),
            }
        }
    }
    Ok(())
}
