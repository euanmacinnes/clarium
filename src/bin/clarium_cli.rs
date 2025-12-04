//!
//! clarium CLI binary
//! -------------------
//! Command-line tool and interactive interpreter for interacting with a local
//! clarium store or a remote clarium HTTP API. In REPL mode, supports a
//! `connect` command to authenticate and run queries against a server.

use std::env;
use std::fs;
use std::io::{self, Read, Write};

use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Url;

use clarium::server::exec::execute_query_safe;
use clarium::storage::SharedStore;

fn print_usage(program: &str) {
    eprintln!(
        "Usage:\n  {program} --query \"<SQL>\" [--root <db_root>] [--connect <url>] [--user <u>] [--password <p>] [--database <db>] [--schema <sch>]\n  {program} -q \"<SQL>\" [--root <db_root>] [--connect <url>] [--user <u>] [--password <p>] [--database <db>] [--schema <sch>]\n  {program} [--root <db_root>]    # reads query text from stdin (local by default)\n  {program} --repl [--root <db_root>] [--connect <url>] [--user <u>] [--password <p>] [--database <db>] [--schema <sch>]  # start interactive interpreter\n\nFlags:\n  --root <path>            Local database root for local queries (default: dbs\\rust_timeseries)\n  --connect <url>          Auto-connect using URL scheme to select transport:\n                           http/https → HTTP API; ws/wss → WebSocket; postgres/postgresql → pgwire\n  --user <u>               Username (can also be in URL)\n  --password <p>           Password (can also be in URL)\n  --database <db>          Optional current database (can also be in URL or implied by pg DSN)\n  --schema <sch>           Optional current schema to select (remote modes)\n  --repl                   Start interactive mode\n  -q, --query <SQL>        Run a one-shot query (from arg); if omitted, reads from stdin\n  -h, --help               Show this help\n\nInteractive commands:\n  connect <url> <user> <password>   connect to remote server; transport inferred by URL scheme\n  disconnect                         end remote session (subsequent queries run locally)\n  use database <name>                set current database on remote session\n  use schema <name>                  set current schema on remote session\n  status                             show current connection info\n  help                               show this help\n  quit | exit                        exit the interpreter\n  <SQL>                              run a SQL-like query locally (no connect) or remotely (after connect)\n\nExamples:\n  {program} --query \"SCHEMA SHOW clarium\"\n  {program} --connect http://127.0.0.1:7878 --user clarium --password clarium --query \"SELECT COUNT(*) FROM public.demo\"\n  {program} --connect ws://127.0.0.1:7878 --user clarium --password clarium --query \"SELECT COUNT(*) FROM public.demo\"\n  {program} --connect postgres://clarium:clarium@127.0.0.1:5433/clarium --schema public -q \"SELECT COUNT(*) FROM demo\"\n  {program} --repl --connect http://127.0.0.1:7878 --user clarium --password clarium --database clarium --schema public\n    > SELECT AVG(value) FROM demo BY 5m\n\nDefaults:\n  --root defaults to dbs\\rust_timeseries relative to current working directory."
    );
}

#[derive(Clone)]
struct HttpSession {
    base: Url,
    client: reqwest::Client,
    csrf: String,
    cookie_header: String,
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
        // Capture Set-Cookie headers into a single Cookie string (for WS upgrades)
        let mut cookies: Vec<String> = Vec::new();
        for val in resp.headers().get_all(reqwest::header::SET_COOKIE).iter() {
            if let Ok(s) = val.to_str() {
                // take name=value before first ';'
                if let Some((nv, _)) = s.split_once(';') { cookies.push(nv.trim().to_string()); }
            }
        }
        let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({"status":"error"}));
        if v.get("status").and_then(|s| s.as_str()) != Some("ok") {
            return Err(anyhow!("login failed"));
        }
        // GET /csrf
        let csrf_url = base_url.join("/csrf")?;
        let resp2 = client.get(csrf_url).send().await?;
        if !resp2.status().is_success() { return Err(anyhow!("failed to obtain csrf: HTTP {}", resp2.status())); }
        // Add any cookies from CSRF response too
        for val in resp2.headers().get_all(reqwest::header::SET_COOKIE).iter() {
            if let Ok(s) = val.to_str() {
                if let Some((nv, _)) = s.split_once(';') { cookies.push(nv.trim().to_string()); }
            }
        }
        let v2: serde_json::Value = resp2.json().await.unwrap_or(serde_json::json!({}));
        let csrf = v2.get("csrf").and_then(|s| s.as_str()).unwrap_or("").to_string();
        if csrf.is_empty() { return Err(anyhow!("csrf token missing")); }
        let cookie_header = if cookies.is_empty() { String::new() } else { cookies.join("; ") };
        Ok(Self { base: base_url, client, csrf, cookie_header })
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

#[derive(Clone)]
enum RemoteTransport {
    Http(HttpSession),
    Ws(WsSession),
    Pg(PgSession),
}

impl RemoteTransport {
    async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        match self {
            RemoteTransport::Http(h) => h.post_query(text).await,
            RemoteTransport::Ws(w) => w.post_query(text).await,
            RemoteTransport::Pg(p) => p.post_query(text).await,
        }
    }
    async fn use_database(&self, name: &str) -> Result<()> {
        match self {
            RemoteTransport::Http(h) => h.use_database(name).await,
            RemoteTransport::Ws(w) => w.use_database(name).await,
            RemoteTransport::Pg(p) => p.use_database(name).await,
        }
    }
    async fn use_schema(&self, name: &str) -> Result<()> {
        match self {
            RemoteTransport::Http(h) => h.use_schema(name).await,
            RemoteTransport::Ws(w) => w.use_schema(name).await,
            RemoteTransport::Pg(p) => p.use_schema(name).await,
        }
    }
    fn ident(&self) -> String {
        match self {
            RemoteTransport::Http(h) => format!("http:{}", h.base),
            RemoteTransport::Ws(w) => format!("ws:{}", w.base),
            RemoteTransport::Pg(p) => format!("pg:{}", p.addr_desc()),
        }
    }
}

#[derive(Clone)]
struct WsSession {
    base: Url,
    csrf: String,
    cookie_header: String,
}

impl WsSession {
    async fn from_http_session(http: &HttpSession) -> Result<Self> { Ok(Self { base: http.base.clone(), csrf: http.csrf.clone(), cookie_header: http.cookie_header.clone() }) }

    fn ws_url_from_http_base(&self) -> Result<Url> {
        // Convert http(s)://host[:port][/path] -> ws(s)://host[:port]
        let mut ws = self.base.clone();
        let scheme = ws.scheme().to_string();
        if scheme == "https" { ws.set_scheme("wss").ok(); } else { ws.set_scheme("ws").ok(); }
        // Point to /ws
        let ws2 = ws.join("/ws")?;
        Ok(ws2)
    }

    async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        use tokio_tungstenite::tungstenite::http::HeaderValue as WsHeaderValue;
        let ws_url = self.ws_url_from_http_base()?;
        let mut req = ws_url.as_str().into_client_request()?;
        if !self.cookie_header.is_empty() {
            req.headers_mut().insert("cookie", WsHeaderValue::from_str(&self.cookie_header).unwrap());
        }
        req.headers_mut().insert("x-csrf-token", WsHeaderValue::from_str(&self.csrf).unwrap());
        let (mut stream, _resp) = tokio_tungstenite::connect_async(req).await?;
        use futures_util::{SinkExt, StreamExt};
        stream.send(tokio_tungstenite::tungstenite::Message::Text(text.to_string())).await?;
        // read one message as response
        if let Some(msg) = stream.next().await {
            let m = msg?;
            if let tokio_tungstenite::tungstenite::Message::Text(s) = m {
                let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::json!({"status":"error","error":"invalid json"}));
                if !v.get("status").and_then(|x| x.as_str()).unwrap_or("").eq("ok") {
                    return Err(anyhow!("remote error: {}", v));
                }
                return Ok(v);
            }
        }
        Err(anyhow!("ws: no response"))
    }
    async fn use_database(&self, _name: &str) -> Result<()> {
        // Use HTTP endpoints for DB/Schema since WS auth requires cookies/CSRF; reuse simple reqwest client
        let client = reqwest::Client::new();
        let url = self.base.join("/use/database")?;
        let mut headers = HeaderMap::new();
        if !self.cookie_header.is_empty() { headers.insert("cookie", HeaderValue::from_str(&self.cookie_header).unwrap()); }
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = client.post(url).headers(headers).json(&serde_json::json!({"name": _name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set database")); }
        Ok(())
    }
    async fn use_schema(&self, _name: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let url = self.base.join("/use/schema")?;
        let mut headers = HeaderMap::new();
        if !self.cookie_header.is_empty() { headers.insert("cookie", HeaderValue::from_str(&self.cookie_header).unwrap()); }
        headers.insert("x-csrf-token", HeaderValue::from_str(&self.csrf).unwrap());
        let resp = client.post(url).headers(headers).json(&serde_json::json!({"name": _name})).send().await?;
        if !resp.status().is_success() { return Err(anyhow!("failed to set schema")); }
        Ok(())
    }
}

#[derive(Clone)]
struct PgSession {
    cfg: String,
    schema: Option<String>,
}

impl PgSession {
    async fn connect(url: &str, schema: Option<String>) -> Result<Self> {
        // We delay actual connection until first query to keep it simple.
        Ok(Self { cfg: url.to_string(), schema })
    }
    async fn connect_client(&self) -> Result<tokio_postgres::Client> {
        use tokio_postgres::{NoTls, Config};
        let cfg: Config = self.cfg.parse().context("invalid postgres url")?;
        let (client, conn) = cfg.connect(NoTls).await?;
        // drive the connection in background
        tokio::spawn(async move { let _ = conn.await; });
        if let Some(s) = &self.schema {
            let _ = client.simple_query(&format!("SET search_path TO {}", s)).await; // best-effort
        }
        Ok(client)
    }
    async fn post_query(&self, text: &str) -> Result<serde_json::Value> {
        let client = self.connect_client().await?;
        let msgs = client.simple_query(text).await?;
        // Convert to a generic JSON shape. We return the last result set if multiple.
        use tokio_postgres::SimpleQueryMessage;
        let mut cols: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        for m in msgs {
            match m {
                SimpleQueryMessage::Row(r) => {
                    if cols.is_empty() {
                        cols = (0..r.len()).map(|i| r.columns()[i].name().to_string()).collect();
                    }
                    let mut out_row = Vec::with_capacity(r.len());
                    for i in 0..r.len() {
                        out_row.push(match r.get(i) { Some(s) => serde_json::Value::String(s.to_string()), None => serde_json::Value::Null });
                    }
                    rows.push(out_row);
                }
                SimpleQueryMessage::CommandComplete(_c) => { /* ignore */ }
                _ => {}
            }
        }
        let result = serde_json::json!({
            "status": "ok",
            "results": {
                "columns": cols,
                "rows": rows
            }
        });
        Ok(result)
    }
    async fn use_database(&self, _name: &str) -> Result<()> { Ok(()) /* database encoded in URL for postgres */ }
    async fn use_schema(&self, name: &str) -> Result<()> {
        // best-effort one-off SET for schema
        let client = self.connect_client().await?;
        let _ = client.simple_query(&format!("SET search_path TO {}", name)).await?;
        Ok(())
    }
    fn addr_desc(&self) -> String { self.cfg.clone() }
}

/// Entry point for the clarium CLI. Parses flags and either runs a one-shot
/// query (from --query or stdin) against a local store, or starts the interactive
/// interpreter which can connect to a remote clarium server.
fn main() -> Result<()> {
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
        return run_repl_with_autoconnect(rt, store, connect_url, connect_user, connect_password, connect_db, connect_schema);
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

    // If connect flags were provided, execute remotely; otherwise run locally
    let result = if let Some(url) = connect_url {
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
        rt.block_on(async { execute_query_safe(&store, &qtext).await })
    };

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
    let mut session: Option<RemoteTransport> = None;
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
        if up == "HELP" {
            print_usage("clarium_cli");
            continue;
        }
        if up.starts_with("CONNECT ") {
            // connect <url> <user> <pass>
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
                    Ok(h) => {
                        let ws = rt.block_on(async { WsSession::from_http_session(&h).await }).unwrap();
                        println!("connected (ws) to {}", url);
                        session = Some(RemoteTransport::Ws(ws));
                    }
                    Err(e) => { eprintln!("connect failed: {}", e); }
                }
            } else if scheme == "postgres" || scheme == "postgresql" {
                match rt.block_on(async { PgSession::connect(url, None).await }) {
                    Ok(p) => { println!("connected (pg) to {}", url); session = Some(RemoteTransport::Pg(p)); }
                    Err(e) => { eprintln!("connect failed: {}", e); }
                }
            } else {
                eprintln!("unsupported scheme in url: {}", url);
            }
            continue;
        }
        if up == "DISCONNECT" {
            if session.is_some() { session = None; println!("disconnected"); } else { println!("not connected"); }
            continue;
        }
        if up == "STATUS" {
            if let Some(s) = &session { println!("connected: {}", s.ident()); } else { println!("local (not connected)"); }
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
            match rt.block_on(async { execute_query_safe(&store, line).await }) {
                Ok(val) => { let pretty = serde_json::to_string_pretty(&val).unwrap_or_else(|_| val.to_string()); println!("{}", pretty); }
                Err(e) => eprintln!("error: {}", e),
            }
        }
    }
    Ok(())
}

fn run_repl_with_autoconnect(
    rt: tokio::runtime::Runtime,
    store: SharedStore,
    connect_url: Option<String>,
    connect_user: Option<String>,
    connect_password: Option<String>,
    connect_db: Option<String>,
    connect_schema: Option<String>,
) -> Result<()> {
    let mut session: Option<RemoteTransport> = None;
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
                            session = Some(RemoteTransport::Ws(ws));
                        }
                        Err(e) => eprintln!("auto-connect failed: {}", e),
                    }
                } else if scheme == "postgres" || scheme == "postgresql" {
                    match rt.block_on(async { PgSession::connect(&url, connect_schema.clone()).await }) {
                        Ok(p) => { println!("connected (pg) to {}", url); session = Some(RemoteTransport::Pg(p)); }
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
        if up == "DISCONNECT" { if session.is_some() { session = None; println!("disconnected"); } else { println!("not connected"); } continue; }
        if up == "STATUS" { if let Some(s) = &session { println!("connected: {}", s.ident()); } else { println!("local (not connected)"); } continue; }
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
