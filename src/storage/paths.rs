use std::path::PathBuf;

use super::Store;

impl Store {
    pub(crate) fn db_dir(&self, table: &str) -> PathBuf {
        // Delegate to central identifier module for consistent resolution
        // Detect if this is a time-series table and ensure `.time` suffix on the last segment
        let d = crate::ident::QueryDefaults::from_options(Some("clarium"), Some("public"));
        // Heuristic: if the identifier explicitly contains ".time" anywhere, treat as time table
        let is_time = table.contains(".time");
        let qualified = if is_time {
            crate::ident::qualify_time_ident(table, &d)
        } else {
            crate::ident::qualify_regular_ident(table, &d)
        };
        crate::ident::to_local_path(&self.root, &qualified)
    }

    pub(crate) fn db_file(&self, table: &str) -> PathBuf {
        self.db_dir(table).join("data.parquet")
    }

    pub(crate) fn schema_path(&self, table: &str) -> PathBuf { self.db_dir(table).join("schema.json") }
}
