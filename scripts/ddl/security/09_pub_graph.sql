-- Published overlay graph: virtual nodes and jumps into main resources
CREATE TABLE IF NOT EXISTS security.pub_graph (
  publication TEXT,
  virtual_path TEXT,        -- path within the publication namespace
  target_resource_id TEXT,  -- canonical res://... target in main graph
  subpath TEXT,             -- optional sub-scope under target
  created_at BIGINT,
  updated_at BIGINT,
  PRIMARY KEY (publication, virtual_path)
  -- FK(publication) REFERENCES security.publications(publication)
);
