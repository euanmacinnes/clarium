//! Resource helpers and constants (scaffold).

use super::model::ResourceId;

pub fn res_filestore(db: &str, fs: &str) -> ResourceId {
    ResourceId(format!("res://{}/{}/filestore", db, fs))
}

pub fn res_path(db: &str, fs: &str, path: &str) -> ResourceId {
    ResourceId(format!("res://{}/{}/path/{}", db, fs, path.trim_start_matches('/')))
}

// Database-related resources (kept here for now; split into sub-modules if this grows)
pub fn res_database(db: &str) -> ResourceId {
    ResourceId(format!("res://{}/database", db))
}

pub fn res_schema(db: &str, schema: &str) -> ResourceId {
    ResourceId(format!("res://{}/schema/{}", db, schema))
}

pub fn res_table(db: &str, schema: &str, table: &str) -> ResourceId {
    ResourceId(format!("res://{}/table/{}/{}", db, schema, table))
}

pub fn res_function(db: &str, schema: &str, func: &str) -> ResourceId {
    ResourceId(format!("res://{}/function/{}/{}", db, schema, func))
}
