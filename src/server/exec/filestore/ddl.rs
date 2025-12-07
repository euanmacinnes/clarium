//! DDL executors for FILESTORE registry persistence.
//! Thin helpers used by SQL layer: CREATE/ALTER/DROP FILESTORE.

use anyhow::{bail, Result};
use chrono::Utc;

use crate::storage::SharedStore;

use super::config::FilestoreConfig;
use super::kv::Keys;
use super::registry::{FilestoreConfigUpdate, FilestoreRegistryEntry, save_filestore_entry, alter_filestore_entry, drop_filestore_entry};

/// Create or overwrite a filestore registry entry.
pub fn create_filestore(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    cfg: FilestoreConfig,
    corr_id: Option<&str>,
) -> Result<FilestoreRegistryEntry> {
    let mut entry = FilestoreRegistryEntry::new(filestore, cfg);
    entry.updated_at = Utc::now().timestamp();
    save_filestore_entry(store, database, filestore, &entry)?;
    let corr = corr_id.unwrap_or("-");
    crate::tprintln!("FILESTORE CREATE filestore={} [corr={}]", filestore, corr);
    Ok(entry)
}

/// Alter a filestore registry entry. Returns updated entry if found.
pub fn alter_filestore_ddl(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    update: FilestoreConfigUpdate,
    corr_id: Option<&str>,
) -> Result<Option<FilestoreRegistryEntry>> {
    let out = alter_filestore_entry(store, database, filestore, update)?;
    let corr = corr_id.unwrap_or("-");
    match &out {
        Some(_) => crate::tprintln!("FILESTORE ALTER filestore={} ok [corr={}]", filestore, corr),
        None => crate::tprintln!("FILESTORE ALTER filestore={} not_found [corr={}]", filestore, corr),
    }
    Ok(out)
}

/// Drop a filestore registry entry. If `force == false`, denies when any data keys exist.
pub fn drop_filestore(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    force: bool,
    corr_id: Option<&str>,
) -> Result<bool> {
    if !force {
        let prefix = Keys::path(database, filestore, "");
        let kv = store.kv_store(database, filestore);
        let any = kv.keys().into_iter().any(|k| k.starts_with(&prefix));
        if any { bail!("filestore_not_empty"); }
    }
    let ok = drop_filestore_entry(store, database, filestore)?;
    let corr = corr_id.unwrap_or("-");
    crate::tprintln!("FILESTORE DROP filestore={} ok={} [corr={}]", filestore, ok, corr);
    Ok(ok)
}
