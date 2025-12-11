use anyhow::Result;
use chrono::Utc;

/// Upsert a scope/id epoch value in security.epochs and return the new value.
async fn upsert_epoch(store: &crate::storage::SharedStore, scope: &str, id: &str, value: i64) -> Result<i64> {
    let now = Utc::now().timestamp_millis();
    // Delete then insert to emulate upsert since engine lacks ON CONFLICT
    let del = format!(
        "DELETE FROM security.epochs WHERE scope='{}' AND id='{}'",
        scope.replace("'", "''"), id.replace("'", "''")
    );
    let _ = crate::server::exec::execute_query_safe(store, &del).await?;
    let ins = format!(
        "INSERT INTO security.epochs (scope, id, value, updated_at) VALUES ('{}','{}',{}, {})",
        scope.replace("'", "''"), id.replace("'", "''"), value, now
    );
    let _ = crate::server::exec::execute_query_safe(store, &ins).await?;
    Ok(value)
}

/// Bump and persist the global epoch; also bump in-memory counter.
pub async fn bump_epoch_global(store: &crate::storage::SharedStore) -> Result<i64> {
    let v = crate::server::exec::filestore::sec::epochs::bump_global() as i64;
    let _ = upsert_epoch(store, "global", "global", v).await?;
    Ok(v)
}

/// Bump and persist a filestore-scoped epoch; also bump in-memory counter.
pub async fn bump_epoch_filestore(store: &crate::storage::SharedStore, fs: &str) -> Result<i64> {
    let v = crate::server::exec::filestore::sec::epochs::bump_filestore(fs) as i64;
    let _ = upsert_epoch(store, "filestore", fs, v).await?;
    Ok(v)
}

/// Bump and persist a publication-scoped epoch; also bump in-memory counter.
pub async fn bump_epoch_publication(store: &crate::storage::SharedStore, pubname: &str) -> Result<i64> {
    let v = crate::server::exec::filestore::sec::epochs::bump_publication(pubname) as i64;
    let _ = upsert_epoch(store, "publication", pubname, v).await?;
    Ok(v)
}
