-- Catalog of materialized resources for indexing and faster selectors
CREATE TABLE IF NOT EXISTS security.resources (
  resource_id TEXT PRIMARY KEY, -- canonical res://id
  kind TEXT,                    -- Filestore|Folder|File|Table|Store|Graph|...
  name TEXT,                    -- human-readable name
  tags_json TEXT,               -- optional tags
  created_at BIGINT,
  updated_at BIGINT
);
