//!
//! Timeline CLI binary
//! -------------------
//! Command-line tool and interactive interpreter for interacting with a local
//! Timeline store or a remote Timeline HTTP API. In REPL mode, supports a
//! `connect` command to authenticate and run queries against a server.

use std::env;
use std::fs;
use std::io::{self, Read, Write};

use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;

use timeline::server::exec::execute_query2;
use timeline::storage::SharedStore;

fn print_usage(program: &str) {
    eprintln!(
        "Usage:\n  {program} --query \"<SQL>\" [--root <db_root>]\n  {program} -q \"<SQL>\" [--root <db_root>]\n  {program} [--root <db_root>]    # reads query text from stdin\n  {program} --repl [--root <db_root>]  # start interactive interpreter\n\nInteractive commands:\n  connect <http(s)://host:port> <user> <password>   connect to remote Timeline server via HTTP API\n  use database <name>                                set current database on remote session\n  use schema <name>                                  set current schema on remote session\n  help                                               show this help\n  quit | exit                                        exit the interpreter\n  <SQL>                                              run a SQL-like query locally (no connect) or remotely (after connect)\n\nExamples:\n  {program} --query \"SCHEMA SHOW timeline\"\n  {program} --repl\n    > connect http://127.0.0.1:7878 timeline timeline\n    > SELECT AVG(value) FROM demo BY 5m\n\nDefaults:\n  --root defaults to dbs\\rust_timeseries relative to current working directory."
    );
}

#[derive(Clone)]
struct HttpSession {
    base: Url,
    client: reqwest::Client,
    csrf: String,
}

impl HttpSession {
    async fn connect(base: &str, user: &str, pass: &str) -> Result<Self> {
        let base_url = Url::parse(base).context("invalid base URL")?;
        let client = reqwest::Client::builder()
            .cookie_store(true)
            .build()?;
        // POST /login
        let login_url = base_url.join("/login")?;
        let resp = client
            .post(login_url)
            .json(&serde_json::json!({"username": user, "password": pass}))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("login failed: HTTP {}", resp.status()));
        }
        let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({"status":"error"}));
        if v.get("status").and_then(|s| s.as_str()) != Some("ok") {
            return Err(anyhow!("login failed"));
        }
        // GET /csrf
        let csrf_url = base_url.join("/csrf")?;
        let resp2 = client.get(csrf_url).send().await?;
        if !resp2.status().is_success() { return Err(anyhow!("failed to obtain csrf: HTTP {}", resp2.status())); }
        let v2: serde_json::Value = resp2.json().await.unwrap_or(serde_json::json!({}));
        let csrf = v2.get("csrf").and_then(|s| s.as_str()).unwrap_or("").to_string();
        if csrf.is_empty() { return Err(anyhow!("csrf token missing")); }
        Ok(Self { base: base_url, client, csrf })
    }

    async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        let qurl = self.base.join("/query")?;
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = self.client.post(qurl)
            .headers(headers)
            .json(&serde_json::json!({"query": text}))
            .send().await?;
        let status = resp.status();
        let val: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({"status":"error"}));
        if !status.is_success() {
            return Err(anyhow!("remote error: {}", val));
        }
        Ok(val)
    }

    async fn use_database(&self, name: &str) -> Result<()> {
        let url = self.base.join("/use/database")?;
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = self.client.post(url).headers(headers).json(&serde_json::json!({"name": name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set database")); }
        Ok(())
    }
    async fn use_schema(&self, name: &str) -> Result<()> {
        let url = self.base.join("/use/schema")?;
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = self.client.post(url).headers(headers).json(&serde_json::json!({"name": name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set schema")); }
        Ok(())
    }
}

/// Entry point for the Timeline CLI. Parses flags and either runs a one-shot
/// query (from --query or stdin) against a local store, or starts the interactive
/// interpreter which can connect to a remote Timeline server.
fn main() -> Result<()> {
    let mut args: Vec<String> = env::args().collect();
    let program = args.remove(0);

    let mut root: Option<String> = None;
    let mut query: Option<String> = None;
    let mut repl: bool = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                if i + 1 >= args.len() { eprintln!("--root requires a value"); print_usage(&program); std::process::exit(2); }
                root = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--query" | "-q" => {
                if i + 1 >= args.len() { eprintln!("--query requires a value"); print_usage(&program); std::process::exit(2); }
                query = Some(args[i+1].clone());
                i += 2; continue;
            }
            "--repl" => { repl = true; i += 1; continue; }
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
        // default to dbs\\rust_timeseries relative to CWD
        let default = if cfg!(windows) { "dbs\\rust_timeseries" } else { "dbs/rust_timeseries" };
        default.to_string()
    });

    // Ensure root directory exists (create if missing for local mode)
    if let Err(e) = fs::create_dir_all(&root_path) { eprintln!("Failed to ensure root directory '{}': {}", root_path, e); }

    // Build store for local mode
    let store = SharedStore::new(&root_path).with_context(|| format!("Failed to open store at {}", root_path))?;

    // Tokio runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build Tokio runtime")?;

    if repl && query.is_none() {
        return run_repl(rt, store);
    }

    // Non-REPL single-shot: if no query arg, read from stdin
    let qtext = if let Some(q) = query { q } else {
        let mut buf = String::new();
        io::stdin().read_to_string(&mut buf).context("Failed to read query from stdin")?;
        buf.trim().to_string()
    };
    if qtext.is_empty() {
        print_usage(&program);
        std::process::exit(2);
    }

    let result = rt.block_on(async { execute_query2(&store, &qtext).await });

    match result {
        Ok(val) => {
            let s = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string());
            println!("{}", s);
            Ok(())
        }
        Err(err) => {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    }
}

fn run_repl(rt: tokio::runtime::Runtime, store: SharedStore) -> Result<()> {
    let mut session: Option<HttpSession> = None;
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut input = String::new();
    println!("timeline-cli interpreter. Type 'help' for commands.");
    loop {
        input.clear();
        print!("> "); let _ = stdout.flush();
        if stdin.read_line(&mut input).is_err() { break; }
        let line = input.trim();
        if line.is_empty() { continue; }
        let up = line.to_uppercase();
        if up == "EXIT" || up == "QUIT" { break; }
        if up == "HELP" {
            print_usage("timeline_cli");
            continue;
        }
        if up.starts_with("CONNECT ") {
            // connect <url> <user> <pass>
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 { eprintln!("usage: connect <url> <user> <password>"); continue; }
            let url = parts[1]; let user = parts[2]; let pass = parts[3];
            match rt.block_on(async { HttpSession::connect(url, user, pass).await }) {
                Ok(ses) => { println!("connected to {}", url); session = Some(ses); }
                Err(e) => { eprintln!("connect failed: {}", e); }
            }
            continue;
        }
        if up.starts_with("USE DATABASE ") {
            let name = line[13..].trim();
            if let Some(s) = &session {
                if let Err(e) = rt.block_on(async { s.use_database(name).await }) { eprintln!("error: {}", e); }
            } else {
                println!("Not connected. 'use database' only affects remote session.");
            }
            continue;
        }
        if up.starts_with("USE SCHEMA ") {
            let name = line[11..].trim();
            if let Some(s) = &session {
                if let Err(e) = rt.block_on(async { s.use_schema(name).await }) { eprintln!("error: {}", e); }
            } else {
                println!("Not connected. 'use schema' only affects remote session.");
            }
            continue;
        }
        // Otherwise treat as query
        if let Some(s) = &session {
            match rt.block_on(async { s.post_query(line).await }) {
                Ok(val) => {
                    // Expect {status:"ok", results: ...}
                    let pretty = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string());
                    println!("{}", pretty);
                }
                Err(e) => eprintln!("error: {}", e),
            }
        } else {
            match rt.block_on(async { execute_query2(&store, line).await }) {
                Ok(val) => { let pretty = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string()); println!("{}", pretty); }
                Err(e) => eprintln!("error: {}", e),
            }
        }
    }
    Ok(())
}
