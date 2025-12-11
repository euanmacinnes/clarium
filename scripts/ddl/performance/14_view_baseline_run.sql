-- Baseline run on main branch: latest run at least 7 days older than latest

CREATE VIEW performance.baseline_main_run AS
WITH latest AS (
  SELECT run_id, run_ts FROM performance.latest_main_run
)
SELECT r.run_id, r.run_ts
FROM performance.run r
JOIN performance.env_build b ON b.build_id = r.build_id
CROSS JOIN latest l
WHERE b.git_branch = 'main'
  AND r.run_ts < l.run_ts - (7 * 24 * 60 * 60 * 1000)
ORDER BY r.run_ts DESC
LIMIT 1;
