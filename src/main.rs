use tracing_subscriber::{EnvFilter, fmt};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logging
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    fmt().with_env_filter(filter).init();

    // Startup banner at info level so something always prints at default verbosity
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "<unset>".to_string());
    // Prefer CLARIUM_* env vars, but support TIMELINE_* for backward compatibility
    let http_port = std::env::var("CLARIUM_HTTP_PORT")
        .or_else(|_| std::env::var("TIMELINE_HTTP_PORT"))
        .unwrap_or_else(|_| "7878".to_string());
    let pg_port = std::env::var("CLARIUM_PG_PORT")
        .or_else(|_| std::env::var("TIMELINE_PG_PORT"))
        .unwrap_or_else(|_| "5433".to_string());
    let db_folder = std::env::var("CLARIUM_DB_FOLDER")
        .or_else(|_| std::env::var("TIMELINE_DB_FOLDER"))
        .unwrap_or_else(|_| "dbs".to_string());
    let pgwire = std::env::var("CLARIUM_PGWIRE")
        .or_else(|_| std::env::var("TIMELINE_PGWIRE"))
        .unwrap_or_else(|_| "false".to_string());
    info!(
        target: "clarium",
        "Clarium starting: RUST_LOG='{}', http_port={}, pg_port={}, pgwire={}, db_root='{}'",
        rust_log, http_port, pg_port, pgwire, db_folder
    );

    crate::server::run().await
}
