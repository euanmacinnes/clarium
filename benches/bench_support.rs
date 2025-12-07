use anyhow::{Result, Context};
use std::path::PathBuf;

use crate::server::exec::execute_query;
use crate::storage::SharedStore;

pub struct BenchCtx {
    pub tmp_dir: tempfile::TempDir,
    pub store: SharedStore,
    rt: tokio::runtime::Runtime,
}

impl BenchCtx {
    pub fn new() -> Result<Self> {
        let tmp = tempfile::tempdir().context("tempdir")?;
        let store = SharedStore::new(tmp.path()).context("SharedStore::new")?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .context("tokio runtime")?;
        // Ensure defaults are in a known state
        // Not all commands require this, but keep consistent with tests
        let ctx = BenchCtx { tmp_dir: tmp, store, rt };
        ctx.exec_ok("USE DATABASE clarium").ok();
        ctx.exec_ok("USE SCHEMA public").ok();
        Ok(ctx)
    }

    pub fn root_path(&self) -> PathBuf { self.tmp_dir.path().to_path_buf() }

    pub fn exec(&self, sql: &str) -> Result<serde_json::Value> {
        self.rt.block_on(execute_query(&self.store, sql)).context("execute_query")
    }

    pub fn exec_ok(&self, sql: &str) -> Result<()> {
        let v = self.exec(sql)?;
        // Most DDL/DML return {status:"ok"}
        if v.get("status").and_then(|s| s.as_str()) == Some("ok") {
            return Ok(());
        }
        // Some SELECT-like may not return status; treat as ok
        if !v.is_null() {
            return Ok(());
        }
        anyhow::bail!("exec_ok unexpected response: {}", v)
    }
}
