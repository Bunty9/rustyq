-- Lead the dispatch index with the claim query's ORDER BY so the planner can
-- walk it and stop at LIMIT. The old (queue, ...) prefix was unusable: the
-- claim filters `queue = ANY($2)`, which forced a seq scan + full sort per
-- claim (O(n^2) drain).
-- ponytail: queue is filtered during the scan, so a worker draining a rare
-- queue behind a huge other queue scans past foreign rows; add a per-queue
-- index if that workload appears.
DROP INDEX IF EXISTS idx_jobs_dispatch;
CREATE INDEX idx_jobs_dispatch ON jobs (priority DESC, run_at)
  WHERE state = 'queued';
