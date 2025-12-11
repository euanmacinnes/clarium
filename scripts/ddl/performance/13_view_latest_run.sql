-- Latest run on main branch (single row)
-- Note: uses numeric millisecond timestamps stored in TIMESTAMP columns

CREATE VIEW performance.latest_main_run AS
SELECT r.run_id, r.run_ts
FROM performance.run r
JOIN performance.env_build b ON b.build_id = r.build_id
WHERE b.git_branch = 'main'
ORDER BY r.run_ts DESC
LIMIT 1;
