use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::storage::SharedStore;
use crate::server::exec::execute_query_safe;

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub criterion_root: PathBuf,
    pub notes: Option<String>,
    pub ci: bool,
}

fn now_ms() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0) }

fn fnv1a64(s: &str) -> i64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as i64).abs()
}

fn esc(s: &str) -> String { s.replace("'", "''") }

fn read_to_string_opt(p: &Path) -> Option<String> { fs::read_to_string(p).ok() }

fn detect_hostname() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "localhost".to_string())
}

fn git(cmd: &[&str]) -> Option<String> {
    std::process::Command::new("git").args(cmd).output().ok().and_then(|o| if o.status.success() { Some(String::from_utf8_lossy(&o.stdout).trim().to_string()) } else { None })
}

fn rustc_version() -> Option<String> {
    std::process::Command::new("rustc").arg("--version").output().ok().and_then(|o| if o.status.success() { Some(String::from_utf8_lossy(&o.stdout).trim().to_string()) } else { None })
}

pub async fn apply_performance_schema(store: &SharedStore, repo_root: &Path) -> Result<usize> {
    let ddl_dir = repo_root.join("scripts").join("ddl").join("performance");
    if !ddl_dir.exists() { return Err(anyhow!(format!("DDL directory not found: {}", ddl_dir.display()))); }
    let mut files: Vec<PathBuf> = fs::read_dir(&ddl_dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|e| e == "sql").unwrap_or(false))
        .collect();
    files.sort();
    let mut applied = 0usize;
    for f in files.iter() {
        if let Some(sql) = read_to_string_opt(f) {
            let _ = execute_query_safe(store, &sql).await; // best-effort; IF NOT EXISTS should make this idempotent
            applied += 1;
        }
    }
    Ok(applied)
}

#[derive(Debug, Clone)]
struct BuildInfo {
    sha: String,
    branch: String,
    rustc: String,
    profile: String,
    features: String,
    build_ts_ms: i64,
}

async fn upsert_env_and_run(store: &SharedStore, notes: Option<String>, ci: bool) -> Result<(i64, i64, i64)> {
    // host
    let hostname = detect_hostname();
    let host_key = format!("{}", hostname);
    let host_id = fnv1a64(&format!("host:{}", host_key));
    let sql_host = format!(
        "INSERT INTO performance.env_host (host_id, hostname, cpu_model, cpu_cores, cpu_threads, cpu_mhz, mem_bytes, os_name, os_version, kernel_version, containerized) VALUES ({}, '{}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE)",
        host_id, esc(&hostname)
    );
    let _ = execute_query_safe(store, &sql_host).await; // ignore duplicates

    // build
    let bi = BuildInfo {
        sha: git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        branch: git(&["rev-parse", "--abbrev-ref", "HEAD"]).unwrap_or_else(|| "unknown".into()),
        rustc: rustc_version().unwrap_or_else(|| "unknown".into()),
        profile: std::env::var("PROFILE").unwrap_or_else(|_| "unknown".into()),
        features: std::env::var("CARGO_FEATURES").unwrap_or_else(|_| "".into()),
        build_ts_ms: now_ms(),
    };
    let build_key = format!("{}|{}|{}", bi.sha, bi.profile, bi.features);
    let build_id = fnv1a64(&format!("build:{}", build_key));
    let sql_build = format!(
        "INSERT INTO performance.env_build (build_id, git_sha, git_branch, rustc_version, profile, features, build_ts) VALUES ({}, '{}', '{}', '{}', '{}', '{}', {})",
        build_id, esc(&bi.sha), esc(&bi.branch), esc(&bi.rustc), esc(&bi.profile), esc(&bi.features), bi.build_ts_ms
    );
    let _ = execute_query_safe(store, &sql_build).await;

    // run
    let run_id = fnv1a64(&format!("run:{}:{}", build_id, now_ms()));
    let sql_run = format!(
        "INSERT INTO performance.run (run_id, run_ts, host_id, build_id, runner, ci, notes) VALUES ({}, {}, {}, {}, '{}', {}, {})",
        run_id, now_ms(), host_id, build_id, esc(&whoami::username()), if ci { "TRUE" } else { "FALSE" },
        match notes { Some(n) => format!("'{}'", esc(&n)), None => "NULL".to_string() }
    );
    let _ = execute_query_safe(store, &sql_run).await;
    Ok((host_id, build_id, run_id))
}

fn parse_estimates_json(path: &Path) -> Option<(f64, f64)> {
    // returns (mean_ns, stddev_ns)
    let text = fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let mean = v.get("mean").and_then(|m| m.get("point_estimate")).and_then(|x| x.as_f64());
    let stddev = v.get("std_dev").and_then(|m| m.get("point_estimate")).and_then(|x| x.as_f64());
    match (mean, stddev) { (Some(m), Some(s)) => Some((m, s)), (Some(m), None) => Some((m, 0.0)), _ => None }
}

fn infer_suite_group_bench(from: &Path) -> (String, String, String) {
    // target/criterion/<group>/<bench>/new/estimates.json
    let comps: Vec<String> = from.ancestors()
        .filter_map(|p| p.file_name().map(|s| s.to_string_lossy().to_string()))
        .take(5)
        .collect();
    // comps[0] is "estimates.json" or "new"; we want comps[2]=bench, comps[3]=group typically
    let group = comps.get(3).cloned().unwrap_or_else(|| "unknown_group".into());
    let bench = comps.get(2).cloned().unwrap_or_else(|| "unknown_bench".into());
    // suite: try to split group by first underscore "sql_*" → "sql"
    let suite = group.split('_').next().unwrap_or("suite").to_string();
    (suite, group, bench)
}

async fn ensure_bench(store: &SharedStore, suite: &str, group: &str, bench: &str) -> Result<i64> {
    let bench_id = fnv1a64(&format!("bench:{}:{}:{}", suite, group, bench));
    let sql = format!(
        "INSERT INTO performance.bench (bench_id, suite, group_name, bench_name, description) VALUES ({}, '{}', '{}', '{}', NULL)",
        bench_id, esc(suite), esc(group), esc(bench)
    );
    let _ = execute_query_safe(store, &sql).await;
    Ok(bench_id)
}

async fn ensure_metric_defs(store: &SharedStore) -> Result<(i64, i64)> {
    let mean_id = fnv1a64("metric:time_mean_ns");
    let std_id = fnv1a64("metric:std_dev_ns");
    let sql1 = format!("INSERT INTO performance.metric_def (metric_id, name, unit, kind, aggregation, description) VALUES ({}, 'time_mean_ns', 'ns', 'latency', 'lower_is_better', 'Criterion mean point_estimate (nanoseconds)')", mean_id);
    let sql2 = format!("INSERT INTO performance.metric_def (metric_id, name, unit, kind, aggregation, description) VALUES ({}, 'std_dev_ns', 'ns', 'latency', 'lower_is_better', 'Criterion std_dev point_estimate (nanoseconds)')", std_id);
    let _ = execute_query_safe(store, &sql1).await;
    let _ = execute_query_safe(store, &sql2).await;
    Ok((mean_id, std_id))
}

fn fingerprint_params(group: &str, bench: &str) -> String {
    let s = format!("g={}#b={}", group, bench);
    format!("{:016x}", fnv1a64(&s))
}

pub async fn ingest(store: &SharedStore, repo_root: &Path, opts: &IngestOptions) -> Result<usize> {
    // Ensure schema exists
    let _ = apply_performance_schema(store, repo_root).await;

    // Prepare env/build/run
    let (_host_id, _build_id, run_id) = upsert_env_and_run(store, opts.notes.clone(), opts.ci).await?;

    // Ensure metric defs
    let (mean_id, std_id) = ensure_metric_defs(store).await?;

    // Walk criterion outputs
    let root = &opts.criterion_root;
    if !root.exists() { return Err(anyhow!(format!("criterion root not found: {}", root.display()))); }
    let mut count = 0usize;
    for entry in walkdir::WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        if entry.file_name().to_string_lossy() == "estimates.json" {
            let path = entry.path().to_path_buf();
            if let Some((mean_ns, std_ns)) = parse_estimates_json(&path) {
                let (suite, group, bench) = infer_suite_group_bench(&path);
                let bench_id = ensure_bench(store, &suite, &group, &bench).await?;
                let pf = fingerprint_params(&group, &bench);
                let ts = now_ms();
                let sql_mean = format!(
                    "INSERT INTO performance.metric_ts (ts, run_id, bench_id, metric_id, param_fingerprint, value, samples, ci_low, ci_high) VALUES ({}, {}, {}, {}, '{}', {}, NULL, NULL, NULL)",
                    ts, run_id, bench_id, mean_id, pf, mean_ns
                );
                let sql_std = format!(
                    "INSERT INTO performance.metric_ts (ts, run_id, bench_id, metric_id, param_fingerprint, value, samples, ci_low, ci_high) VALUES ({}, {}, {}, {}, '{}', {}, NULL, NULL, NULL)",
                    ts, run_id, bench_id, std_id, pf, std_ns
                );
                let _ = execute_query_safe(store, &sql_mean).await;
                let _ = execute_query_safe(store, &sql_std).await;
                count += 2;
            }
        }
    }
    Ok(count)
}

fn days_to_ms(days: i64) -> i64 { days.saturating_mul(86_400_000) }

/// Refresh hourly rollups for data since `since_days` ago.
/// Strategy: delete overlapping buckets, then insert aggregated rows from raw metric_ts.
pub async fn refresh_hourly(store: &SharedStore, since_days: i64) -> Result<()> {
    let since_ms = now_ms().saturating_sub(days_to_ms(since_days.max(0)));
    let del = format!(
        "DELETE FROM performance.metric_hourly WHERE ts_hour >= {}",
        since_ms
    );
    let _ = execute_query_safe(store, &del).await;
    let ins = format!(
        "INSERT INTO performance.metric_hourly (ts_hour, bench_id, metric_id, param_fingerprint, cnt, avg_value, min_value, max_value) \
         SELECT ((ts / 3600000) * 3600000) AS ts_hour, bench_id, metric_id, param_fingerprint, \
                COUNT(*) AS cnt, AVG(value) AS avg_value, MIN(value) AS min_value, MAX(value) AS max_value \
         FROM performance.metric_ts \
         WHERE ts >= {} \
         GROUP BY ts_hour, bench_id, metric_id, param_fingerprint",
        since_ms
    );
    let _ = execute_query_safe(store, &ins).await;
    Ok(())
}

/// Refresh daily rollups for data since `since_days` ago.
pub async fn refresh_daily(store: &SharedStore, since_days: i64) -> Result<()> {
    let since_ms = now_ms().saturating_sub(days_to_ms(since_days.max(0)));
    let del = format!(
        "DELETE FROM performance.metric_daily WHERE ts_day >= {}",
        since_ms
    );
    let _ = execute_query_safe(store, &del).await;
    let ins = format!(
        "INSERT INTO performance.metric_daily (ts_day, bench_id, metric_id, param_fingerprint, cnt, avg_value, min_value, max_value) \
         SELECT ((ts / 86400000) * 86400000) AS ts_day, bench_id, metric_id, param_fingerprint, \
                COUNT(*) AS cnt, AVG(value) AS avg_value, MIN(value) AS min_value, MAX(value) AS max_value \
         FROM performance.metric_ts \
         WHERE ts >= {} \
         GROUP BY ts_day, bench_id, metric_id, param_fingerprint",
        since_ms
    );
    let _ = execute_query_safe(store, &ins).await;
    Ok(())
}

/// Enforce retention on raw metric_ts; keep only last `raw_days` days.
pub async fn enforce_retention(store: &SharedStore, raw_days: i64) -> Result<()> {
    let cutoff = now_ms().saturating_sub(days_to_ms(raw_days.max(0)));
    let del = format!(
        "DELETE FROM performance.metric_ts WHERE ts < {}",
        cutoff
    );
    let _ = execute_query_safe(store, &del).await;
    Ok(())
}
