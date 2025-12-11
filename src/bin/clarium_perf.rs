//!
//! clarium_perf — local benchmark ingester
//! ---------------------------------------
//! Applies performance schema DDL and ingests Criterion results into the
//! performance schema to track metrics over time.
//!

use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clarium::storage::SharedStore;
use clarium::tools::perf_ingest::{
    apply_performance_schema, ingest, IngestOptions, refresh_hourly, refresh_daily, enforce_retention,
};

fn print_usage(program: &str) {
    eprintln!(
        "Usage:\n  {program} --store <path> apply-ddl\n  {program} --store <path> ingest [--criterion-root <dir>] [--notes <text>] [--ci]\n  {program} --store <path> refresh-hourly [--since-days <N>]\n  {program} --store <path> refresh-daily [--since-days <N>]\n  {program} --store <path> enforce-retention --raw-days <N>\n\nOptions:\n  --store <path>         Root path to the local clarium store (required)\n  --criterion-root <d>   Criterion root directory (default: target/criterion)\n  --notes <text>         Optional notes to attach to the run\n  --ci                   Mark run as CI\n  --since-days <N>       Recompute rollups for the last N days (default: 30)\n  --raw-days <N>         Keep only last N days of raw metric_ts (required for enforce-retention)\n"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let program = &args[0];

    // Parse minimal flags
    let mut store_path: Option<PathBuf> = None;
    let mut criterion_root: Option<PathBuf> = None;
    let mut notes: Option<String> = None;
    let mut ci = false;
    let mut since_days: Option<i64> = None;
    let mut raw_days: Option<i64> = None;

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--store" => { i += 1; store_path = args.get(i).map(|s| PathBuf::from(s)); }
            "--criterion-root" => { i += 1; criterion_root = args.get(i).map(|s| PathBuf::from(s)); }
            "--notes" => { i += 1; notes = args.get(i).cloned(); }
            "--ci" => { ci = true; }
            "--since-days" => { i += 1; since_days = args.get(i).and_then(|s| s.parse::<i64>().ok()); }
            "--raw-days" => { i += 1; raw_days = args.get(i).and_then(|s| s.parse::<i64>().ok()); }
            _ => { break; }
        }
        i += 1;
    }

    if store_path.is_none() || i >= args.len() {
        print_usage(program);
        return Err(anyhow!("missing --store and a subcommand"));
    }
    let cmd = &args[i];
    let store_path = store_path.unwrap();
    let criterion_root = criterion_root.unwrap_or_else(|| PathBuf::from("target").join("criterion"));

    let store = SharedStore::new(&store_path)
        .with_context(|| format!("Failed to open store at {}", store_path.display()))?;

    // Repo root = current working dir (project root when invoked from workspace)
    let repo_root = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    match cmd.as_str() {
        "apply-ddl" => {
            let n = apply_performance_schema(&store, &repo_root).await?;
            println!("applied {} DDL files", n);
        }
        "ingest" => {
            // Ensure schema exists first (idempotent)
            let _ = apply_performance_schema(&store, &repo_root).await;
            let opts = IngestOptions { criterion_root, notes, ci };
            let n = ingest(&store, &repo_root, &opts).await?;
            println!("ingested {} metric rows", n);
        }
        "refresh-hourly" => {
            let days = since_days.unwrap_or(30);
            // Ensure rollup tables exist (part of schema files 11/12)
            let _ = apply_performance_schema(&store, &repo_root).await;
            refresh_hourly(&store, days).await?;
            println!("refreshed hourly rollups for last {} days", days);
        }
        "refresh-daily" => {
            let days = since_days.unwrap_or(60);
            let _ = apply_performance_schema(&store, &repo_root).await;
            refresh_daily(&store, days).await?;
            println!("refreshed daily rollups for last {} days", days);
        }
        "enforce-retention" => {
            let days = raw_days.ok_or_else(|| anyhow!("--raw-days <N> is required"))?;
            enforce_retention(&store, days).await?;
            println!("enforced raw retention: kept last {} days", days);
        }
        _ => {
            print_usage(program);
            return Err(anyhow!("unknown subcommand: {}", cmd));
        }
    }

    Ok(())
}
