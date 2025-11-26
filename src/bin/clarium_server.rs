//!
//! clarium server binary
//! ----------------------
//! Command-line entry point for starting the clarium HTTP server and optional
//! pgwire endpoint. Supports configuration via CLI flags and environment variables.

use anyhow::Result;
use std::env;

fn parse_port_env(name: &str) -> Option<u16> {
    match env::var(name) {
        Ok(val) => val.parse::<u16>().ok(),
        Err(_) => None,
    }
}

fn parse_port_arg(args: &[String], flag: &str) -> Option<u16> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == flag
            && i + 1 < args.len() {
                return args[i + 1].parse::<u16>().ok();
            }
        i += 1;
    }
    None
}

fn parse_bool_env(name: &str) -> Option<bool> {
    match env::var(name) {
        Ok(v) => {
            let s = v.to_lowercase();
            match s.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            }
        }
        Err(_) => None,
    }
}

fn parse_pgwire_arg(args: &[String]) -> Option<bool> {
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == "--no-pgwire" {
            return Some(false);
        }
        if a == "--pgwire" {
            // If next token is present and not another flag, try parse bool; otherwise true
            if i + 1 < args.len() {
                let next = &args[i + 1];
                if !next.starts_with('-') {
                    let s = next.to_lowercase();
                    return match s.as_str() {
                        "1" | "true" | "yes" | "on" => Some(true),
                        "0" | "false" | "no" | "off" => Some(false),
                        _ => Some(true), // non-boolean, treat presence as enable
                    };
                }
            }
            return Some(true);
        }
        i += 1;
    }
    None
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!(r"   ________           _               
  / ____/ /___ ______(_)_  ______ ___ 
 / /   / / __ `/ ___/ / / / / __ `__ \
/ /___/ / /_/ / /  / / /_/ / / / / / /
\____/_/\__,_/_/  /_/\__,_/_/ /_/ /_/  ");

    // Initialize tracing subscriber with env filter if provided
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let args: Vec<String> = env::args().collect();

    if has_flag(&args, "--help") || has_flag(&args, "-h") {
        println!("clarium Server\n\nUSAGE:\n  clarium_server [--http-port N] [--pg-port N] [--db-folder PATH] [--pgwire|--no-pgwire]\n\nOPTIONS:\n  --http-port N       HTTP API port (env: clarium_HTTP_PORT, default 7878)\n  --pg-port N         pgwire port (env: clarium_PG_PORT, default 5433)\n  --db-folder PATH    Database root folder (env: clarium_DB_FOLDER, default dbs/clarium)\n  --pgwire [bool]     Enable pgwire (env: clarium_PGWIRE). Presence enables; or pass true/false.\n  --no-pgwire        Disable pgwire explicitly.\n");
        return Ok(());
    }

    // Defaults
    let default_http: u16 = 7878;
    let default_pg: u16 = 5433;
    let default_root: &str = "dbs";

    // Environment variables
    let env_http = parse_port_env("clarium_HTTP_PORT");
    let env_pg = parse_port_env("clarium_PG_PORT");
    let env_root = std::env::var("clarium_DB_FOLDER").ok();
    let env_pgwire = parse_bool_env("clarium_PGWIRE");

    // CLI arguments override environment
    let arg_http = parse_port_arg(&args, "--http-port");
    let arg_pg = parse_port_arg(&args, "--pg-port");
    let arg_root = {
        let mut i = 0;
        let mut val: Option<String> = None;
        while i < args.len() {
            if args[i] == "--db-folder" {
                if i + 1 < args.len() { val = Some(args[i+1].clone()); }
                break;
            }
            i += 1;
        }
        val
    };
    let arg_pgwire = parse_pgwire_arg(&args);

    let http_port = arg_http.or(env_http).unwrap_or(default_http);
    let pg_port = arg_pg.or(env_pg).unwrap_or(default_pg);
    let db_root = arg_root.or(env_root).unwrap_or_else(|| default_root.to_string());

    // Default enable depends on compile-time feature
    let default_enable = cfg!(feature = "pgwire");
    let enable_pgwire = arg_pgwire.or(env_pgwire).unwrap_or(default_enable);

    #[cfg(feature = "pgwire")]
    {
        let pg_opt = if enable_pgwire { Some(pg_port) } else { None };
        if enable_pgwire {
            println!(
                "clarium starting using ports: http={}, pgwire={}, db_root={}",
                http_port, pg_port, db_root
            );
            tracing::info!("Using ports: http={}, pgwire={}, db_root={}", http_port, pg_port, db_root);
        } else {
            println!(
                "clarium starting with pgwire DISABLED: http={}, db_root={}",
                http_port, db_root
            );
            tracing::info!("pgwire disabled; Using port: http={}, db_root={}", http_port, db_root);
        }
        return clarium::server::run_with_ports(http_port, pg_opt, &db_root).await;
    }

    #[cfg(not(feature = "pgwire"))]
    {
        if enable_pgwire {
            println!("WARNING: --pgwire requested but the binary was built without 'pgwire' feature; pgwire will be disabled.");
            tracing::warn!("pgwire requested but not compiled in; continuing without pgwire");
        }
        println!("clarium starting using ports: http={}, db_root={}", http_port, db_root);
        tracing::info!("Using port: http={}, db_root={}", http_port, db_root);
        // Pass None for pgwire (disabled)
        return clarium::server::run_with_ports(http_port, None, &db_root).await;
    }
}
