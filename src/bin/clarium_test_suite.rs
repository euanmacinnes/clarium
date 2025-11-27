//!
//! clarium_test_suite
//! -------------------
//! A utility that combines server startup and basic connectivity testing over
//! postgres:// using the built-in pgwire endpoint. It is useful for spinning up
//! a local database and verifying client connectivity quickly.
//!
//! Default credentials: username 'clarium', password 'clarium'.
//! Default database: 'clarium', schema 'public'.
//!
//! Example:
//!   cargo run --bin clarium_test_suite -- --db-folder dbs --pg-port 5433 --http-port 7878 --check
//!

use anyhow::{anyhow, Context, Result};
use std::env;
use std::time::{Duration, Instant};

fn arg_val(args: &[String], flag: &str) -> Option<String> {
    let mut i = 0;
    while i + 1 < args.len() {
        if args[i] == flag { return Some(args[i + 1].clone()); }
        i += 1;
    }
    None
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag)
}

fn print_usage() {
    println!(
        "clarium_test_suite\n\nUSAGE:\n  clarium_test_suite [--db-folder PATH] [--pg-port N] [--http-port N] [--user U] [--password P] [--database D] [--timeout SECS] [--check] [--sql SQL] [--exit-after-check] [--sqlx-check] [--seaorm-demo] [--exit-after-seaorm]\n\nOPTIONS:\n  --db-folder PATH          Database root folder (default: dbs)\n  --pg-port N               Port for pgwire (default: 5433)\n  --http-port N             Port for HTTP API (default: 7878)\n  --user U                  Username for connectivity check (default: clarium)\n  --password P              Password for connectivity check (default: clarium)\n  --database D              Database name for DSN (default: clarium)\n  --timeout SECS            Startup timeout for readiness (default: 30)\n  --check                   After start, perform a connectivity check using tokio-postgres\n  --sql SQL                 SQL to run for the connectivity check (simple query flow)\n  --exit-after-check        Exit after the check instead of staying running\n  --sqlx-check              Perform an extended-protocol check (prepare/bind/execute) using tokio-postgres\n  --seaorm-demo             (dev-only feature) Run a SeaORM CRUD smoke test against the DSN\n  --exit-after-seaorm       Exit after SeaORM demo completes (CI-friendly)\n\nNOTES:\n  - The SeaORM demo is feature-gated. Build with: --features seaorm_dev\n  - For extra server-side protocol logs, set: RUST_LOG=debug CLARIUM_PGWIRE_TRACE=1\n"
    );
}

async fn wait_for_tcp(host: &str, port: u16, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let addr = format!("{}:{}", host, port);
    loop {
        match tokio::net::TcpStream::connect(&addr).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if Instant::now() >= deadline { return Err(anyhow!("timeout waiting for {}: {}", addr, e)); }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber (honor RUST_LOG if set)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let args: Vec<String> = env::args().collect();
    if has_flag(&args, "--help") || has_flag(&args, "-h") {
        print_usage();
        return Ok(());
    }

    // Defaults
    let db_folder = arg_val(&args, "--db-folder").unwrap_or_else(|| "dbs".to_string());
    let http_port: u16 = arg_val(&args, "--http-port").and_then(|s| s.parse().ok()).unwrap_or(7878);
    let pg_port: u16 = arg_val(&args, "--pg-port").and_then(|s| s.parse().ok()).unwrap_or(5433);
    let user = arg_val(&args, "--user").unwrap_or_else(|| "clarium".to_string());
    let password = arg_val(&args, "--password").unwrap_or_else(|| "clarium".to_string());
    let database = arg_val(&args, "--database").unwrap_or_else(|| "clarium".to_string());
    let timeout_secs: u64 = arg_val(&args, "--timeout").and_then(|s| s.parse().ok()).unwrap_or(30);
    let do_check = has_flag(&args, "--check");
    let exit_after_check = has_flag(&args, "--exit-after-check");
    let do_sqlx_check = has_flag(&args, "--sqlx-check");
    let do_seaorm = has_flag(&args, "--seaorm-demo");
    let exit_after_seaorm = has_flag(&args, "--exit-after-seaorm");
    let sql = arg_val(&args, "--sql").unwrap_or_else(|| "SELECT COUNT(_time) FROM clarium/public/demo.time".to_string());

    println!("Starting clarium server (http={}, pgwire={}, db_root='{}')...", http_port, pg_port, db_folder);

    // Spawn the server; run_with_ports will block serving HTTP, so we spawn it.
    let db_folder_clone = db_folder.clone();
    tokio::spawn(async move {
        if let Err(e) = clarium::server::run_with_ports(http_port, Some(pg_port), &db_folder_clone).await {
            eprintln!("clarium server terminated with error: {}", e);
        }
    });

    // Wait for pgwire readiness by probing the TCP port
    let timeout = Duration::from_secs(timeout_secs);
    wait_for_tcp("127.0.0.1", pg_port, timeout).await
        .context("pgwire did not become ready within timeout")?;

    // Also ensure HTTP port is up (best-effort)
    let _ = wait_for_tcp("127.0.0.1", http_port, Duration::from_secs(3)).await;

    let dsn = format!(
        "postgres://{}:{}@127.0.0.1:{}/{}?application_name=clarium_test_suite&sslmode=disable",
        urlencoding::encode(&user), urlencoding::encode(&password), pg_port, database
    );
    println!("Server ready. Connect using DSN:\n  {}", dsn);

    if do_check {
        println!("Running connectivity check via tokio-postgres...");
        if let Err(e) = run_check(&dsn, &sql).await {
            eprintln!("Connectivity check FAILED: {}", e);
            eprintln!("  Hints: try setting RUST_LOG=debug and CLARIUM_PGWIRE_TRACE=1 for detailed server logs.");
            // If explicitly checking, return error code
            if exit_after_check { return Err(e); }
        } else {
            println!("Connectivity check OK");
        }
        if exit_after_check {
            return Ok(());
        }
    }

    if do_sqlx_check {
        println!("Running extended-protocol check (prepare/bind/execute via tokio-postgres)...");
        if let Err(e) = run_sqlx_like_check(&dsn).await {
            eprintln!("Extended-protocol check FAILED: {}", e);
            eprintln!("  Hints: enable RUST_LOG=debug and CLARIUM_PGWIRE_TRACE=1 on the server for detailed pgwire traces.");
            if exit_after_check { return Err(e); }
        } else {
            println!("Extended-protocol check OK");
        }
    }

    if do_seaorm {
        println!("Running SeaORM demo (dev-only feature)...");
        let r = run_seaorm_demo(&dsn).await;
        match r {
            Ok(_) => println!("SeaORM demo OK"),
            Err(e) => {
                eprintln!("SeaORM demo FAILED: {}", e);
                if exit_after_seaorm { return Err(e); }
            }
        }
        if exit_after_seaorm { return Ok(()); }
    }

    println!("clarium_test_suite is now running. Press Ctrl+C to stop.");
    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await.ok();
    println!("Shutting down.");
    Ok(())
}

async fn run_check(dsn: &str, sql: &str) -> Result<()> {
    use tokio_postgres::NoTls;
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    // Spawn the connection driver
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {}", e);
        }
    });
    let msgs = client.simple_query(sql).await?;
    // Count DataRow messages and print first few
    let mut row_count = 0usize;
    let mut printed = 0usize;
    for m in msgs {
        match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                row_count += 1;
                if printed < 5 {
                    let mut parts: Vec<String> = Vec::new();
                    for i in 0..r.len() {
                        parts.push(r.get(i).unwrap_or("").to_string());
                    }
                    println!("  row {}: {}", printed, parts.join(", "));
                    printed += 1;
                }
            }
            _ => {}
        }
    }
    println!("Query returned {} row(s)", row_count);
    Ok(())
}

// Minimal extended-protocol round-trip: prepare a statement, bind a param, execute, fetch one row
async fn run_sqlx_like_check(dsn: &str) -> Result<()> {
    use tokio_postgres::NoTls;
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {}", e);
        }
    });
    // Start with a parameterless prepared statement to validate Parse/Describe/Execute/Sync
    let stmt = client.prepare("SELECT 1 AS v").await?;
    let rows = client.query(&stmt, &[]).await?;
    if let Some(row) = rows.first() {
        let v: i64 = row.try_get("v").unwrap_or_default();
        println!("  extended query result: v={}", v);
    }
    Ok(())
}

#[cfg(feature = "seaorm_dev")]
async fn run_seaorm_demo(dsn: &str) -> Result<()> {
    use sea_orm::{Database, Statement, DatabaseBackend, DbConn, QueryTrait, ConnectionTrait, sea_query::{Query, Table, ColumnDef, PostgresQueryBuilder, Expr, Alias, Order}};

    // Connect using SeaORM
    println!("SeaORM: connecting to DSN...\n  {}", dsn);
    let db: DbConn = Database::connect(dsn).await?;
    println!("SeaORM: connected");

    // Ensure a simple table exists: public.metrics
    // Columns: id BIGINT PRIMARY KEY, value DOUBLE PRECISION, label TEXT, created_ms BIGINT
    let create_sql = r#"
        CREATE TABLE IF NOT EXISTS public.metrics (
            id BIGINT PRIMARY KEY,
            value DOUBLE PRECISION,
            label TEXT,
            created_ms BIGINT
        )
    "#;
    println!("SeaORM: executing CREATE TABLE...");
    db.execute(Statement::from_string(DatabaseBackend::Postgres, create_sql.to_string())).await?;
    println!("SeaORM: CREATE TABLE done");

    // Clean any previous rows for deterministic demo
    println!("SeaORM: DELETE FROM public.metrics (cleanup)...");
    db.execute(Statement::from_string(DatabaseBackend::Postgres, "DELETE FROM public.metrics".to_string())).await.ok();
    println!("SeaORM: cleanup done");

    // Insert a few rows using sea_query builder
    println!("SeaORM: inserting sample rows...");
    let insert_stmt = Query::insert()
        .into_table(Alias::new("public.metrics"))
        .columns(vec![Alias::new("id"), Alias::new("value"), Alias::new("label"), Alias::new("created_ms")])
        .values_panic(vec![Expr::val(1i64).into(), Expr::val(1.23_f64).into(), Expr::val("a").into(), Expr::val(1730000000000_i64).into()])
        .values_panic(vec![Expr::val(2i64).into(), Expr::val(4.56_f64).into(), Expr::val("b").into(), Expr::val(1730000005000_i64).into()])
        .values_panic(vec![Expr::val(3i64).into(), Expr::val(7.89_f64).into(), Expr::val("c").into(), Expr::val(1730000010000_i64).into()])
        .to_owned()
        .to_string(PostgresQueryBuilder);
    db.execute(Statement::from_string(DatabaseBackend::Postgres, insert_stmt)).await?;
    println!("SeaORM: insert done");

    // Count rows
    let count_stmt = Query::select()
        .expr_as(Expr::cust("COUNT(*)"), Alias::new("count"))
        .from(Alias::new("public.metrics"))
        .to_string(PostgresQueryBuilder);
    println!("SeaORM: counting rows...");
    let row_opt = db.query_one(Statement::from_string(DatabaseBackend::Postgres, count_stmt)).await?;
    // Parse first row
    let cnt = row_opt.as_ref().and_then(|first| first.try_get::<i64>("", "count").ok()).unwrap_or(0);
    println!("SeaORM: metrics count after insert = {}", cnt);

    // Update one row (raw SQL is simplest to avoid builder type inference pitfalls)
    println!("SeaORM: updating one row (id=2 -> label='z')...");
    db.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        "UPDATE public.metrics SET label = 'z' WHERE id = 2".to_string(),
    )).await?;
    println!("SeaORM: update done");

    // Select a few rows
    println!("SeaORM: selecting rows...");
    let select_stmt = Query::select()
        .columns(vec![Alias::new("id"), Alias::new("value"), Alias::new("label"), Alias::new("created_ms")])
        .from(Alias::new("public.metrics"))
        .order_by(Alias::new("id"), Order::Asc)
        .to_string(PostgresQueryBuilder);
    let rows = db.query_all(Statement::from_string(DatabaseBackend::Postgres, select_stmt)).await?;
    for (i, r) in rows.iter().enumerate().take(5) {
        let id: i64 = r.try_get("", "id").unwrap_or_default();
        let val: Option<f64> = r.try_get("", "value").ok();
        let label: Option<String> = r.try_get("", "label").ok();
        let ts: Option<i64> = r.try_get("", "created_ms").ok();
        println!("  SeaORM row {}: id={}, value={:?}, label={:?}, created_ms={:?}", i, id, val, label, ts);
    }

    // Delete rows
    println!("SeaORM: deleting rows with id >= 2...");
    let delete_stmt = Query::delete()
        .from_table(Alias::new("public.metrics"))
        .and_where(Expr::col(Alias::new("id")).gte(2))
        .to_string(PostgresQueryBuilder);
    db.execute(Statement::from_string(DatabaseBackend::Postgres, delete_stmt)).await?;
    println!("SeaORM: delete done");

    Ok(())
}

#[cfg(not(feature = "seaorm_dev"))]
async fn run_seaorm_demo(_dsn: &str) -> Result<()> {
    Err(anyhow!("SeaORM demo not available: build with --features seaorm_dev"))
}
