-- Host environment metadata
-- One row per unique host configuration

CREATE TABLE IF NOT EXISTS performance.env_host (
  host_id          BIGINT PRIMARY KEY,
  hostname         TEXT NOT NULL,
  cpu_model        TEXT,
  cpu_cores        INT,
  cpu_threads      INT,
  cpu_mhz          DOUBLE,
  mem_bytes        BIGINT,
  os_name          TEXT,
  os_version       TEXT,
  kernel_version   TEXT,
  containerized    BOOL DEFAULT FALSE,
  UNIQUE(hostname, cpu_model, cpu_cores, cpu_threads, mem_bytes, os_name, os_version)
);
