use std::path::PathBuf;

use super::Store;

impl Store {
    pub(crate) fn db_dir(&self, table: &str) -> PathBuf {
        // Use current session defaults to qualify partial identifiers.
        // Rules:
        //   table              -> <current_db>/<current_schema>/<table>
        //   schema/table       -> <current_db>/<schema>/<table>
        //   db/schema/table    -> used as-is after normalization
        // Time tables (*.time) are routed via `qualify_time_ident`.
        let d = crate::system::current_query_defaults();
        let is_time = table.ends_with(".time")
            || table.split('.').last().map(|t| t.eq_ignore_ascii_case("time")).unwrap_or(false);
        
        // Check if already fully qualified (3+ parts) to avoid double qualification
        let normalized = table.replace('\\', "/");
        let parts_count = normalized.split('/').filter(|p| !p.is_empty()).count();
        let qualified = if parts_count >= 3 {
            // Already fully qualified like "clarium/demo/table" - use as-is
            normalized
        } else if is_time {
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
