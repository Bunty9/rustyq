CREATE TABLE IF NOT EXISTS rustyq_jobs (
  id            UUID PRIMARY KEY,
  queue         TEXT NOT NULL,
  kind          TEXT NOT NULL,
  payload       JSONB NOT NULL,
  state         TEXT NOT NULL CHECK (state IN ('queued','running','done','failed','dead')),
  priority      SMALLINT NOT NULL DEFAULT 0,
  attempts      INT NOT NULL DEFAULT 0,
  max_attempts  INT NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
  run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at     TIMESTAMPTZ,
  locked_by     TEXT,
  last_error    TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rustyq_jobs_dispatch ON rustyq_jobs (queue, state, priority DESC, run_at)
  WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS idx_rustyq_jobs_locked   ON rustyq_jobs (locked_by, state) WHERE state = 'running';
