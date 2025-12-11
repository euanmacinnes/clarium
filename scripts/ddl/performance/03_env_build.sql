-- Build metadata per compiled binary/revision

CREATE TABLE IF NOT EXISTS performance.env_build (
  build_id         BIGINT PRIMARY KEY,
  git_sha          TEXT NOT NULL,
  git_branch       TEXT,
  rustc_version    TEXT,
  profile          TEXT,
  features         TEXT,
  build_ts         TIMESTAMP NOT NULL,
  UNIQUE(git_sha, profile, features)
);
