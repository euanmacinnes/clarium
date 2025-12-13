use std::path::PathBuf;

use super::Store;

impl Store {
    /// Resolve a logical identifier into a concrete on-disk directory, preferring
    /// a suffix-less folder and falling back to a legacy `<table>.time` folder.
    /// This enables a migration away from the required `.time` suffix while
    /// retaining backward compatibility for existing databases.
    pub(crate) fn resolve_table_dir(&self, table: &str) -> PathBuf {
        // Normalize incoming separators
        let normalized = table.replace('\\', "/");
        let d = crate::system::current_query_defaults();

        // Determine if user explicitly asked for a time table by name
        let explicit_time = normalized.ends_with(".time")
            || normalized
                .split('.')
                .last()
                .map(|t| t.eq_ignore_ascii_case("time"))
                .unwrap_or(false);

        // Build two qualified variants: regular and time
        let qual_regular = crate::ident::qualify_regular_ident(&normalized, &d);
        let qual_time = crate::ident::qualify_time_ident(&normalized, &d);

        // Convert to local FS paths
        let p_regular = crate::ident::to_local_path(&self.root, &qual_regular);
        let p_time = crate::ident::to_local_path(&self.root, &qual_time);

        // Prefer the directory that exists. If both exist, prefer the non-suffix
        // directory to encourage migration forward. If neither exists, fall back
        // to explicit intent (if any), else prefer regular.
        let reg_exists = p_regular.exists();
        let time_exists = p_time.exists();
        crate::tprintln!(
            "[paths.resolve_table_dir] input='{}' qual_regular='{}' qual_time='{}' reg_exists={} time_exists={}",
            table,
            qual_regular,
            qual_time,
            reg_exists,
            time_exists
        );
        if reg_exists && !time_exists {
            return p_regular;
        }
        if time_exists && !reg_exists {
            return p_time;
        }
        if reg_exists && time_exists {
            // If caller explicitly requested a time table, honor that preference.
            if explicit_time { return p_time; }
            return p_regular;
        }
        // Neither exists: pick based on explicit intent, otherwise regular
        if explicit_time { p_time } else { p_regular }
    }
    pub(crate) fn db_dir(&self, table: &str) -> PathBuf { self.resolve_table_dir(table) }

    pub(crate) fn db_file(&self, table: &str) -> PathBuf {
        self.db_dir(table).join("data.parquet")
    }

    pub(crate) fn schema_path(&self, table: &str) -> PathBuf { self.db_dir(table).join("schema.json") }
}
